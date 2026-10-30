"""reconcile-slack-callback — Phase 5 stage 4: the ONLY public endpoint of the POC.

Slack calls POST /slack/interactions when someone clicks Approve or Reject on a card in the approvals channel.
Runs on Cloud Run as sa-api, which may only INSERT into recovery.approval_decisions (exactly what the console
does). Every request is checked by slack_core (signature, age, channel, approver, card state, fingerprint).
Secrets come from Secret Manager (SLACK_SIGNING_SECRET, SLACK_BOT_TOKEN); nothing is logged except counts.
"""
from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from urllib.parse import parse_qs

import requests
import slack_core as core
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from google.cloud import bigquery

PROJECT = os.environ["GCP_PROJECT_ID"]
CHANNEL = os.environ["SLACK_CHANNEL_ID"]
ALLOWED = core.approvers(os.environ.get("SLACK_APPROVERS", ""))
SECRET = os.environ.get("SLACK_SIGNING_SECRET", "")
TOKEN = os.environ.get("SLACK_BOT_TOKEN", "")

app = FastAPI(title="reconcile-slack-callback", docs_url=None, redoc_url=None, openapi_url=None)
_bq = None


def bq() -> bigquery.Client:
    global _bq
    if _bq is None:
        _bq = bigquery.Client(project=PROJECT, location="asia-south1")
    return _bq


def _rows(sql: str, **params) -> list[dict]:
    cfg = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter(k, "STRING", v)
                                                    for k, v in params.items()], use_query_cache=False)
    return [dict(r.items()) for r in bq().query(sql, job_config=cfg).result()]


def card(approval_id: str) -> dict | None:
    rows = _rows(f"""select * from `{PROJECT}.recovery.approvals` where approval_id = @a
                     qualify row_number() over (order by `at` desc) = 1""", a=approval_id)
    return rows[0] if rows else None


def decision(approval_id: str) -> dict | None:
    rows = _rows(f"""select * from `{PROJECT}.recovery.approval_decisions` where approval_id = @a
                     qualify row_number() over (order by decided_at) = 1""", a=approval_id)
    return rows[0] if rows else None


def slack(method: str, **body) -> dict:
    r = requests.post(f"https://slack.com/api/{method}", json=body, timeout=8,
                      headers={"Authorization": f"Bearer {TOKEN}"})
    return r.json() if r.ok else {"ok": False, "error": f"http_{r.status_code}"}


def email_of(user_id: str) -> str:
    r = requests.get("https://slack.com/api/users.info", params={"user": user_id}, timeout=8,
                     headers={"Authorization": f"Bearer {TOKEN}"}).json()
    return ((r.get("user") or {}).get("profile") or {}).get("email", "") if r.get("ok") else ""


def record(approval_id: str, verdict: str, fingerprint: str, email: str, reason: str | None = None) -> str | None:
    """Re-read the card and decision right before writing (the console does the same)."""
    now = datetime.now(timezone.utc)
    why = core.may_decide(card(approval_id), decision(approval_id), email, fingerprint, now, ALLOWED)
    if why:
        return why
    if verdict == "REJECT" and len((reason or "").strip()) < 3:
        return "a rejection needs a short reason"
    row = core.decision_row(approval_id, verdict, fingerprint, email, reason, now, uuid.uuid4().hex)
    errors = bq().insert_rows_json(f"{PROJECT}.recovery.approval_decisions", [row], row_ids=[row["decision_id"]])
    return f"could not record the decision ({len(errors)} error)" if errors else None


def finish(channel: str, ts: str, text: str) -> None:
    """Replace the card message with the outcome (no buttons left to click)."""
    slack("chat.update", channel=channel, ts=ts, text=text,
          blocks=[{"type": "section", "text": {"type": "mrkdwn", "text": text}}])


def tell(channel: str, user: str, text: str) -> None:
    slack("chat.postEphemeral", channel=channel, user=user, text=f":warning: {text}")


@app.get("/healthz")
def healthz() -> dict:
    return {"ok": True}


@app.post("/slack/interactions")
async def interactions(request: Request) -> Response:
    body = await request.body()
    why = core.verify_signature(SECRET, request.headers.get("X-Slack-Request-Timestamp", ""), body,
                                request.headers.get("X-Slack-Signature", ""))
    if why:
        return Response(status_code=401)            # never explain to an unsigned caller
    payload = json.loads((parse_qs(body.decode()).get("payload") or ["{}"])[0])
    user = (payload.get("user") or {}).get("id", "")

    if payload.get("type") == "block_actions":
        channel = (payload.get("channel") or {}).get("id", "")
        msg_ts = (payload.get("message") or {}).get("ts", "")
        action = (payload.get("actions") or [{}])[0]
        parsed = core.parse_value(action.get("value", ""))
        if channel != CHANNEL or not parsed:
            tell(channel, user, "cards can only be decided in the approvals channel")
            return Response(status_code=200)
        approval_id, fp = parsed
        email = email_of(user)
        if action.get("action_id") == "reject":      # ask for the reason first (modal)
            pre = core.may_decide(card(approval_id), decision(approval_id), email, fp,
                                  datetime.now(timezone.utc), ALLOWED)
            if pre:
                tell(channel, user, pre)
                return Response(status_code=200)
            slack("views.open", trigger_id=payload.get("trigger_id"), view={
                "type": "modal", "callback_id": "reject_reason",
                "private_metadata": json.dumps({"a": approval_id, "fp": fp, "c": channel, "ts": msg_ts}),
                "title": {"type": "plain_text", "text": "Reject card"},
                "submit": {"type": "plain_text", "text": "Reject"},
                "blocks": [{"type": "input", "block_id": "r", "label": {"type": "plain_text", "text": "Reason"},
                            "element": {"type": "plain_text_input", "action_id": "reason", "min_length": 3}}]})
            return Response(status_code=200)
        why = record(approval_id, "APPROVE", fp, email)
        if why:
            tell(channel, user, why)
        else:
            finish(channel, msg_ts, core.outcome_text("APPROVE", email, approval_id))
        return Response(status_code=200)

    if payload.get("type") == "view_submission" and (payload.get("view") or {}).get("callback_id") == "reject_reason":
        meta = json.loads(payload["view"].get("private_metadata") or "{}")
        reason = (((payload["view"].get("state") or {}).get("values") or {}).get("r") or {}).get("reason", {})
        reason = reason.get("value", "")
        if meta.get("c") != CHANNEL:
            return JSONResponse({"response_action": "errors", "errors": {"r": "wrong channel"}})
        email = email_of(user)
        why = record(meta.get("a", ""), "REJECT", meta.get("fp", ""), email, reason)
        if why:
            return JSONResponse({"response_action": "errors", "errors": {"r": why}})
        finish(meta["c"], meta["ts"], core.outcome_text("REJECT", email, meta["a"], reason))
        return Response(status_code=200)

    return Response(status_code=200)                 # other Slack events are ignored
