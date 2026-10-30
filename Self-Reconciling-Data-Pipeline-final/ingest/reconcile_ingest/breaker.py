"""Phase 6 — outage breaker: one incident per source outage instead of one per dataset.

    python -m reconcile_ingest.breaker check      # evaluate every source now (also run by the engine
                                                  # and by the outage_breaker DAG every 5 minutes)
    python -m reconcile_ingest.breaker status     # current state per source

State lives in ops.breakers (append-only; the newest row per source is its state), written by sa-engine
and readable by sa-publish (the gate) and sa-api (the console) with the permissions they already have.

On OPEN:   a breaker row, ONE parent incident (rule outage_breaker_open, P1), the source's failing datasets
           recorded as HELD children, any open incident on that source HELD (queued fixes cancelled; the
           approval worker cancels their pending cards), and ONE alert on the `notifications` topic.
On CLOSE:  only when the probe passes AND every dataset reloaded (T21); parent RESOLVED. The children are
           re-evaluated by the next engine run, oldest first, under the normal rules.
GLOBAL:    `global_open_at_sources` sources open at once → a GLOBAL breaker that holds all publishing.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import uuid

from google.cloud import bigquery

from reconcile_ingest import source_state as ss

BREAKER_SCHEMA = [bigquery.SchemaField(n, t) for n, t in [
    ("source_system", "STRING"), ("state", "STRING"), ("opened_at", "TIMESTAMP"), ("closed_at", "TIMESTAMP"),
    ("reason", "STRING"), ("failing_datasets", "STRING"), ("inventory", "INT64"),
    ("parent_incident_id", "STRING"), ("alert", "STRING"), ("policy_version", "STRING"),
    ("actor", "STRING"), ("at", "TIMESTAMP")]]
BREAKER_RULE = "outage_breaker_open"


def ensure_table(bq) -> None:
    t = bigquery.Table(f"{bq.project}.{ss.OPS_DS}.{ss.BREAKERS_TABLE}", schema=BREAKER_SCHEMA)
    t.description = "Outage breakers per source system (written by sa-engine; newest row per source = state)"
    bq.create_table(t, exists_ok=True)


def _append(bq, rows: list[dict]) -> None:
    if rows:
        cfg = bigquery.LoadJobConfig(schema=BREAKER_SCHEMA, write_disposition="WRITE_APPEND")
        bq.load_table_from_json(rows, f"{bq.project}.{ss.OPS_DS}.{ss.BREAKERS_TABLE}", job_config=cfg).result()


def send_alert(message: dict) -> str:
    """ONE message on the notifications topic per breaker opening (as sa-engine). Never raises."""
    try:
        import google.auth
        from google.auth import impersonated_credentials
        from google.cloud import pubsub_v1

        from reconcile_ingest.publish import SCOPES, project_id, service_account
        source, _ = google.auth.default(scopes=SCOPES)
        creds = impersonated_credentials.Credentials(source_credentials=source,
                                                     target_principal=service_account("engine"),
                                                     target_scopes=SCOPES, lifetime=600)
        pub = pubsub_v1.PublisherClient(credentials=creds)
        msg_id = pub.publish(pub.topic_path(project_id(), "notifications"),
                             json.dumps(message, default=str).encode(), type=message["type"]).result(timeout=30)
        return f"alert sent (notifications message {msg_id})"
    except Exception as exc:  # noqa: BLE001 — the incident is the record; the alert is best effort
        return f"alert NOT sent: {str(exc).splitlines()[0][:160]}"


def slack_alert(policy: dict, text: str) -> str:
    try:
        from reconcile_ingest import slack_notify
        return slack_notify.post_text(policy, text)
    except Exception as exc:  # noqa: BLE001 — an alert channel never blocks the breaker
        return f"slack NOT sent ({type(exc).__name__})"


def _incident(engine, *, inc_id, key, bd, dataset, ftype, status, note, evidence, policy, actor, mode="hold"):
    return {"incident_id": inc_id, "incident_key": key, "business_date": str(bd), "dataset": dataset,
            "failure_type": ftype, "status": status, "attempt": 0, "action": "hold_publish", "mode": mode,
            "evidence": json.dumps(evidence, default=str), "gates": None, "note": note,
            "gate_run_id": None, "actor": actor, "at": engine._now(), "policy_version": policy["version"],
            "rule_id": BREAKER_RULE, "severity": "P1", "route": "L2"}


def _open_incidents_on(bq, engine, datasets: list[str]) -> list[dict]:
    if not datasets:
        return []
    rows = engine._q(bq, f"""select * from `{bq.project}.{engine.INCIDENT_DS}.incidents`
                             where dataset in unnest(@ds)
                               and business_date >= date_sub(current_date(), interval 14 day)
                             qualify row_number() over (partition by incident_id order by `at` desc) = 1""",
                     ds=datasets)
    return [r for r in rows if r["status"] in engine.OPEN_STATES - {"HELD"}]


def check(policy: dict | None = None, now: dt.datetime | None = None, quiet: bool = False) -> dict:
    """Evaluate every source; open/close breakers. Returns {"open": {source: row}, "events": [...]}."""
    from reconcile_ingest import engine
    policy = policy or engine.load_policy()
    now = now or dt.datetime.now(dt.timezone.utc)
    bq = engine.client_as(engine.ENGINE)
    ensure_table(bq)
    engine.ensure_table(bq)
    actor = engine.service_account(engine.ENGINE)
    states = ss.breaker_states(bq)
    widest = max(int(ss.open_rule(policy, s)["within_minutes"]) for s in ss.SOURCES)
    batches = ss.latest_batches(bq, now - dt.timedelta(minutes=widest))
    probes = ss.latest_probes(bq, now - dt.timedelta(minutes=widest))
    rows, incs, events = [], [], []

    def say(msg):
        events.append(msg)
        if not quiet:
            print(msg)

    for src in ss.SOURCES:
        rule = ss.open_rule(policy, src)
        inv = ss.inventory(src)
        since = now - dt.timedelta(minutes=int(rule["within_minutes"]))
        failing = sorted(d for d in inv if d in batches and batches[d]["status"] == "FAILED"
                         and batches[d]["started_at"] >= since)
        probe = probes.get(src)
        probe_down = bool(probe and not probe["reachable"] and probe["probed_at"] >= since)
        cur = states.get(src) or {}
        if cur.get("state") != "OPEN":
            opened, why = ss.should_open(failing, len(inv), rule, probe_down)
            if not opened:
                if failing and not quiet:
                    print(f"  {src:8} CLOSED  {why}")
                continue
            bd = max((batches[d]["business_date"] for d in failing), default=now.date())
            parent = f"INC-{bd:%Y%m%d}-{uuid.uuid4().hex[:6]}"
            alert = send_alert({"type": "source_outage", "source_system": src, "incident_id": parent,
                                "failing_datasets": failing, "reason": why, "opened_at": now.isoformat()})
            alert += " · " + slack_alert(policy, f":rotating_light: *{src} outage* — {why}. Incident {parent}. "
                                                 f"Its datasets and the reports built from them are held; no fixes "
                                                 "run until the source answers and fresh data has loaded.")
            rows.append({"source_system": src, "state": "OPEN", "opened_at": now.isoformat(), "closed_at": None,
                         "reason": why, "failing_datasets": json.dumps(failing), "inventory": len(inv),
                         "parent_incident_id": parent, "alert": alert, "policy_version": policy["version"],
                         "actor": actor, "at": now.isoformat()})
            ev = {"source_system": src, "failing_datasets": failing, "inventory": inv, "reason": why,
                  "errors": {d: (batches[d].get("error") or "")[:160] for d in failing}}
            incs.append(_incident(engine, inc_id=parent, key=f"source_outage:{src}:{bd}", bd=bd, dataset=None,
                                  ftype="source_outage", status="ESCALATED", evidence=ev, policy=policy,
                                  actor=actor, note=f"{src} outage: {why}; all {src} datasets and the reports "
                                                    f"built from them are held; no per-dataset fixes. {alert}"))
            for d in failing:   # children: one HELD row per failing dataset, under the parent
                incs.append(_incident(engine, inc_id=f"INC-{bd:%Y%m%d}-{uuid.uuid4().hex[:6]}",
                                      key=f"extract_failed:{d}:{bd}", bd=bd, dataset=d, ftype="extract_failed",
                                      status="HELD", evidence={"parent": parent, **batches[d]}, policy=policy,
                                      actor=actor, note=f"held under outage {parent}; no per-dataset action"))
            for inc in _open_incidents_on(bq, engine, inv):   # queued fixes / approval cards stop here
                incs.append({**{k: inc[k] for k in inc if k in {f.name for f in engine.INCIDENT_SCHEMA}},
                             "business_date": str(inc["business_date"]), "status": "HELD", "mode": "hold",
                             "at": engine._now(), "actor": actor,
                             "note": f"held under outage {parent}; queued action cancelled (was {inc['status']})"})
            say(f"  {src:8} OPEN    {why} → parent {parent}, {len(failing)} child(ren) held · {alert}")
        else:
            opened_at = cur["opened_at"]
            p_after = ss.latest_probes(bq, opened_at).get(src)
            probe_ok = bool(p_after and p_after["reachable"])
            missing = sorted(set(inv) - ss.reloaded_since(bq, src, opened_at))
            closed, why = ss.should_close(probe_ok, missing)
            if not closed:
                say(f"  {src:8} OPEN    {why}")
                continue
            rows.append({**{k: cur.get(k) for k in ("source_system", "failing_datasets", "inventory",
                                                     "parent_incident_id")},
                         "state": "CLOSED", "opened_at": opened_at.isoformat(), "closed_at": now.isoformat(),
                         "reason": why, "alert": None, "policy_version": policy["version"], "actor": actor,
                         "at": now.isoformat()})
            parent = engine._latest_incident(bq, cur["parent_incident_id"]) if cur.get("parent_incident_id") else None
            if parent:
                incs.append({**{k: parent[k] for k in parent if k in {f.name for f in engine.INCIDENT_SCHEMA}},
                             "business_date": str(parent["business_date"]), "status": "RESOLVED",
                             "at": engine._now(), "actor": actor,
                             "note": f"{src} recovered: {why}; held datasets are re-evaluated by the next run"})
            say(f"  {src:8} CLOSED  {why} → parent {cur.get('parent_incident_id')} RESOLVED")

    # global breaker: N sources open at once (after this pass)
    open_now = {s for s in ss.SOURCES if (states.get(s) or {}).get("state") == "OPEN"}
    for r in rows:
        (open_now.add if r["state"] == "OPEN" else open_now.discard)(r["source_system"])
    g, need = states.get(ss.GLOBAL) or {}, ss.global_threshold(policy)
    if len(open_now) >= need and g.get("state") != "OPEN":
        parent = f"INC-{now:%Y%m%d}-{uuid.uuid4().hex[:6]}"
        why = f"{len(open_now)} sources down at once ({', '.join(sorted(open_now))}) — hold all publishing"
        alert = send_alert({"type": "global_outage", "sources": sorted(open_now), "incident_id": parent})
        alert += " · " + slack_alert(policy, f":rotating_light: *GLOBAL hold* — {why}. Incident {parent}.")
        rows.append({"source_system": ss.GLOBAL, "state": "OPEN", "opened_at": now.isoformat(), "closed_at": None,
                     "reason": why, "failing_datasets": json.dumps(sorted(open_now)), "inventory": len(ss.SOURCES),
                     "parent_incident_id": parent, "alert": alert, "policy_version": policy["version"],
                     "actor": actor, "at": now.isoformat()})
        incs.append(_incident(engine, inc_id=parent, key=f"global_outage:{ss.GLOBAL}:{now.date()}", bd=now.date(),
                              dataset=None, ftype="global_outage", status="ESCALATED", policy=policy, actor=actor,
                              evidence={"sources": sorted(open_now)}, note=f"{why}. {alert}"))
        say(f"  GLOBAL   OPEN    {why} → parent {parent} · {alert}")
    elif len(open_now) < need and g.get("state") == "OPEN":
        rows.append({"source_system": ss.GLOBAL, "state": "CLOSED", "opened_at": g["opened_at"].isoformat(),
                     "closed_at": now.isoformat(), "reason": f"{len(open_now)} source(s) open now",
                     "failing_datasets": json.dumps(sorted(open_now)), "inventory": len(ss.SOURCES),
                     "parent_incident_id": g.get("parent_incident_id"), "alert": None,
                     "policy_version": policy["version"], "actor": actor, "at": now.isoformat()})
        parent = engine._latest_incident(bq, g["parent_incident_id"]) if g.get("parent_incident_id") else None
        if parent:
            incs.append({**{k: parent[k] for k in parent if k in {f.name for f in engine.INCIDENT_SCHEMA}},
                         "business_date": str(parent["business_date"]), "status": "RESOLVED",
                         "at": engine._now(), "actor": actor, "note": "fewer sources down; global hold lifted"})
        say(f"  GLOBAL   CLOSED  {len(open_now)} source(s) open now")
        open_now.discard(ss.GLOBAL)
    if len(open_now) >= need:
        open_now.add(ss.GLOBAL)

    _append(bq, rows)
    engine.record(bq, incs)
    latest = ss.breaker_states(bq)
    if not events and not quiet:
        print("  all breakers CLOSED — no source outage")
    return {"open": {s: r for s, r in latest.items() if r["state"] == "OPEN"}, "events": events}


def status() -> None:
    from reconcile_ingest import engine
    bq = engine.client_as(engine.ENGINE)
    states = ss.breaker_states(bq)
    if not states:
        print("no breaker has ever opened")
    for s, r in sorted(states.items()):
        print(f"{s:8} {r['state']:7} opened {str(r['opened_at'])[:19]}  closed {str(r['closed_at'] or '-')[:19]}  "
              f"parent {r['parent_incident_id'] or '-'}  {r['reason']}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("cmd", choices=["check", "status"])
    a = ap.parse_args()
    if a.cmd == "check":
        print("Outage breaker check")
        check()
    else:
        status()


if __name__ == "__main__":
    main()
