"""Phase 5 stage 4 — post approval cards and outage alerts to Slack (as sa-engine), keep them up to date.

    python -m reconcile_ingest.slack_notify sync     # post new PENDING cards, close finished ones (also run by
                                                     # the approval_worker DAG every 5 minutes)
    python -m reconcile_ingest.slack_notify test     # one test message to the channel

policy.yml notifications.slack: enabled, channel_id. The bot token is read from Secret Manager
(`slack-bot-token`, sa-engine has accessor on that one secret). Cards carry counts and the engine's own words
only (plus the Communication agent's summary when there is one) — the same content as the console (T26).
A card's buttons hold `<approval_id>|<fingerprint>`; the public callback (slack/app.py) turns a click into a
decision row, and the approval worker re-checks everything before acting. Slack being down never blocks the
pipeline: every call here is best effort and the console keeps working.
"""
from __future__ import annotations

import argparse
import base64
import datetime as dt
import json

from google.cloud import bigquery

POSTS_SCHEMA = [bigquery.SchemaField(n, t) for n, t in [
    ("approval_id", "STRING"), ("channel", "STRING"), ("ts", "STRING"), ("state", "STRING"), ("at", "TIMESTAMP")]]
TOKEN_SECRET = "slack-bot-token"
_token_cache: dict = {}


def config(policy: dict) -> dict | None:
    c = ((policy.get("notifications") or {}).get("slack") or {})
    if not c.get("enabled") or not str(c.get("channel_id") or "").startswith("C"):
        return None
    return {"channel": c["channel_id"], "secret": c.get("token_secret", TOKEN_SECRET)}


def token(secret: str) -> str:
    if secret in _token_cache:
        return _token_cache[secret]
    from google.auth.transport.requests import AuthorizedSession

    from reconcile_ingest.agent_gateway import _creds
    from reconcile_ingest.publish import project_id
    r = AuthorizedSession(_creds("engine")).get(
        f"https://secretmanager.googleapis.com/v1/projects/{project_id()}/secrets/{secret}/versions/latest:access",
        timeout=10)
    r.raise_for_status()
    _token_cache[secret] = base64.b64decode(r.json()["payload"]["data"]).decode().strip()
    return _token_cache[secret]


def call(cfg: dict, method: str, **body) -> dict:
    import requests
    try:
        r = requests.post(f"https://slack.com/api/{method}", json=body, timeout=10,
                          headers={"Authorization": f"Bearer {token(cfg['secret'])}"})
        return r.json() if r.ok else {"ok": False, "error": f"http_{r.status_code}"}
    except Exception as exc:  # noqa: BLE001 — Slack is a convenience; the console and the journal are the record
        return {"ok": False, "error": type(exc).__name__}


# ── what a card looks like in Slack ────────────────────────────────────────
def card_blocks(a: dict) -> list[dict]:
    c = json.loads(a["card"]) if isinstance(a.get("card"), str) else (a.get("card") or {})
    ev = c.get("evidence") or {}
    facts = [f"*{k.replace('_', ' ')}:* {v}" for k, v in ev.items() if v is not None and k != "batch_id"][:10]
    expires = str(a["expires_at"])[:16].replace("T", " ")
    blocks = [
        {"type": "header", "text": {"type": "plain_text", "text": (c.get("title") or a["action"])[:150]}},
        {"type": "section", "text": {"type": "mrkdwn", "text": (c.get("what_happened") or "")[:2900]}},
    ]
    if c.get("summary"):
        blocks.append({"type": "section", "text": {"type": "mrkdwn",
                                                   "text": f"*In plain words:* {c['summary']}"[:2900]}})
        blocks.append({"type": "context", "elements": [{"type": "mrkdwn", "text": (c.get("summary_by") or "")[:300]}]})
    if facts:
        blocks.append({"type": "section", "fields": [{"type": "mrkdwn", "text": f} for f in facts]})
    blocks += [
        {"type": "section", "text": {"type": "mrkdwn", "text":
            f"*Rule:* {c.get('rule', '')}\n*Impact:* {c.get('impact', '')}\n"
            f"*If approved:* {c.get('if_approved', '')}\n*If rejected:* {c.get('if_rejected', '')}"[:2900]}},
        {"type": "context", "elements": [{"type": "mrkdwn", "text":
            f"{a['approval_id']} · incident {a['incident_id']} · policy {a.get('policy_version')} · "
            f"attempt {a.get('attempt')} · expires {expires} UTC · the engine re-checks every fact before acting"}]},
        {"type": "actions", "block_id": "decide", "elements": [
            {"type": "button", "action_id": "approve", "style": "primary",
             "text": {"type": "plain_text", "text": "Approve"}, "value": f"{a['approval_id']}|{a['fingerprint']}"},
            {"type": "button", "action_id": "reject", "style": "danger",
             "text": {"type": "plain_text", "text": "Reject"}, "value": f"{a['approval_id']}|{a['fingerprint']}"},
        ]},
    ]
    return blocks


FINAL_TEXT = {
    "APPROVED": ":white_check_mark: *{id}* approved{by} — re-check passed; the fix is running.",
    "REJECTED": ":x: *{id}* rejected{by} — escalated to a person; publication stays held.",
    "EXPIRED": ":hourglass: *{id}* expired — nobody answered in time; escalated.",
    "STALE": ":warning: *{id}* not executed — the facts changed after approval; escalated.",
    "CANCELLED": ":no_entry_sign: *{id}* cancelled — the incident changed (resolved or held under an outage).",
}


def final_text(a: dict) -> str:
    by = f" by {a['decided_by']}" if a.get("decided_by") else ""
    return FINAL_TEXT.get(a["status"], "*{id}* is now " + a["status"]).format(id=a["approval_id"], by=by)


# after an approved fix ran: how the incident ended (the engine's own words, counts only — T26)
OUTCOMES = {"RESOLVED", "ESCALATED"}


def outcome_text(a: dict, inc: dict) -> str:
    took = ""
    try:
        took = f" in {(inc['at'] - a['requested_at']).total_seconds() / 60:.1f} min"
    except (KeyError, TypeError, AttributeError):
        pass
    head = (f":white_check_mark: *{a['approval_id']}* — fix worked: incident {inc['incident_id']} RESOLVED "
            f"at attempt {inc.get('attempt') or 1}{took}.") if inc["status"] == "RESOLVED" else \
           (f":rotating_light: *{a['approval_id']}* — the approved fix did not resolve incident {inc['incident_id']}; "
            f"escalated to a person{took}.")
    note = (inc.get("note") or "").strip()
    return head + (f"\n>{note[:300]}" if note else "")


# ── keep Slack in step with recovery.approvals ──────────────────────────────
def _posts(bq) -> dict[str, dict]:
    from reconcile_ingest.engine import INCIDENT_DS, _q
    rows = _q(bq, f"""select * from `{bq.project}.{INCIDENT_DS}.slack_posts`
                      qualify row_number() over (partition by approval_id order by `at` desc) = 1""")
    return {r["approval_id"]: r for r in rows}


def sync(policy: dict | None = None) -> dict:
    from reconcile_ingest import engine
    policy = policy or engine.load_policy()
    cfg = config(policy)
    if not cfg:
        print("Slack is off (policy notifications.slack.enabled / channel_id)")
        return {"posted": 0, "closed": 0}
    bq = engine.client_as(engine.ENGINE)
    table = f"{bq.project}.{engine.INCIDENT_DS}.slack_posts"
    bq.create_table(bigquery.Table(table, schema=POSTS_SCHEMA), exists_ok=True)
    posts, rows, now = _posts(bq), [], dt.datetime.now(dt.timezone.utc).isoformat()
    posted = closed = 0
    for a in engine._latest_approvals(bq)[:50]:
        p = posts.get(a["approval_id"])
        if a["status"] == "PENDING" and p is None:
            r = call(cfg, "chat.postMessage", channel=cfg["channel"], text=f"Approval needed: {a['approval_id']}",
                     blocks=card_blocks(a))
            if r.get("ok"):
                rows.append({"approval_id": a["approval_id"], "channel": r["channel"], "ts": r["ts"],
                             "state": "PENDING", "at": now})
                posted += 1
            print(f"  post   {a['approval_id']}: {'ok' if r.get('ok') else r.get('error')}")
        elif p and p["state"] == "APPROVED" and a["status"] == "APPROVED":
            # the card was approved; tell the channel how it ended once the incident settles
            inc = engine._latest_incident(bq, a["incident_id"])
            if inc and inc["status"] in OUTCOMES:
                text = outcome_text(a, inc)
                r = call(cfg, "chat.update", channel=p["channel"], ts=p["ts"], text=text,
                         blocks=[{"type": "section", "text": {"type": "mrkdwn", "text": text}}])
                call(cfg, "chat.postMessage", channel=p["channel"], thread_ts=p["ts"], text=text)  # notifies
                if r.get("ok"):
                    rows.append({**{k: p[k] for k in ("approval_id", "channel", "ts")}, "state": inc["status"],
                                 "at": now})
                    closed += 1
                print(f"  outcome {a['approval_id']} → {inc['status']}: {'ok' if r.get('ok') else r.get('error')}")
        elif p and p["state"] not in OUTCOMES and a["status"] != "PENDING" and p["state"] != a["status"]:
            text = final_text(a)
            r = call(cfg, "chat.update", channel=p["channel"], ts=p["ts"], text=text,
                     blocks=[{"type": "section", "text": {"type": "mrkdwn", "text": text}}])
            if r.get("ok"):
                rows.append({**{k: p[k] for k in ("approval_id", "channel", "ts")}, "state": a["status"], "at": now})
                closed += 1
            print(f"  close  {a['approval_id']} → {a['status']}: {'ok' if r.get('ok') else r.get('error')}")
    if rows:
        cfg_load = bigquery.LoadJobConfig(schema=POSTS_SCHEMA, write_disposition="WRITE_APPEND")
        bq.load_table_from_json(rows, table, job_config=cfg_load).result()
    print(f"Slack sync: {posted} card(s) posted, {closed} closed")
    return {"posted": posted, "closed": closed}


def post_text(policy: dict, text: str) -> str:
    """Best-effort alert (e.g. an outage breaker opening). Returns a short status for the journal."""
    cfg = config(policy)
    if not cfg:
        return "slack off"
    r = call(cfg, "chat.postMessage", channel=cfg["channel"], text=text)
    return "slack sent" if r.get("ok") else f"slack NOT sent ({r.get('error')})"


def main() -> None:
    from reconcile_ingest.engine import load_policy
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("cmd", choices=["sync", "test"])
    a = ap.parse_args()
    if a.cmd == "sync":
        sync()
    else:
        print(post_text(load_policy(), ":wave: Reconcile pipeline: Slack connection test (synthetic POC data)."))


if __name__ == "__main__":
    main()
