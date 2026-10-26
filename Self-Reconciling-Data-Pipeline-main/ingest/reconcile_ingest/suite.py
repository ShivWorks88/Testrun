"""Failure suite — the 6 "built, not tested" rows of the failure-to-decision table, one command each.

Runs inside the Airflow container (test project only, ALLOW_INJECTIONS=true):
    python -m reconcile_ingest.suite build_error        # one case
    python -m reconcile_ingest.suite all                # all six, then a summary table
    python -m reconcile_ingest.suite list

Each case: inject → (dbt_build) → wait for the engine → check the decision → clean up → wait for RESOLVED.
Evidence lives where it always does: recovery.incidents, publication.publication_state, Airflow runs.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import subprocess
import time

from reconcile_ingest.engine import ENGINE, INCIDENT_DS, _q
from reconcile_ingest.publish import STATE_DS, client_as

BD = "2026-10-14"
MARTS = ["dim_encounters", "fct_bed_occupancy", "fct_daily_claims"]

CASES = {
    "source_timeout": {
        "what": "EHR source times out on the first try",
        "expect": "Airflow retries; the load succeeds on attempt 2; no incident",
        "kind": "retry",
    },
    "build_error": {
        "what": "A release ships SQL that fails at run time (dim_encounters)",
        "expect": "build_error → ESCALATED to an engineer; all 3 reports HELD; no data action",
        "kind": "escalate", "dbt_inject": ["build_error"], "types": {"build_error"}, "held": MARTS,
    },
    "staging_duplicates": {
        "what": "A release breaks the dedupe: 3 claims appear twice in stg_claims",
        "expect": "unique / receipt check fails → ESCALATED; fct_daily_claims HELD; no re-extract",
        "kind": "escalate", "dbt_inject": ["staging_duplicates"],
        "types": {"check_failed", "staging_mismatch"}, "held": ["fct_daily_claims"],
    },
    "join_fanout": {
        "what": "Join key not unique: 25 encounters doubled in the claims report",
        "expect": "mart-vs-staging test fails → ESCALATED; fct_daily_claims HELD",
        "kind": "escalate", "dbt_inject": ["join_fanout"], "types": {"check_failed"},
        "held": ["fct_daily_claims"],
    },
    "schema_drift": {
        "what": "Source renamed claim_amount; the report still uses the new name",
        "expect": "build_error → ESCALATED to an engineer; fct_daily_claims HELD",
        "kind": "escalate", "dbt_inject": ["schema_drift"], "types": {"build_error"},
        "held": ["fct_daily_claims"],
    },
    "offsetting_errors": {
        "what": "Claims: 50 lost + 50 loaded twice (row count still matches the receipt)",
        "expect": "offsetting_errors → auto re-extract → RESOLVED at attempt 1",
        "kind": "auto",
    },
}


# ── helpers ─────────────────────────────────────────────────────────────────
def now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def say(msg: str) -> None:
    print(f"[{now():%H:%M:%S}] {msg}", flush=True)


def airflow(*args: str, check: bool = True) -> str:
    r = subprocess.run(["airflow", *args], capture_output=True, text=True)
    if check and r.returncode != 0:  # surface the tail of the CLI output
        raise RuntimeError(f"airflow {' '.join(args)} failed:\n{r.stdout[-1500:]}\n{r.stderr[-1500:]}")
    return r.stdout


def ingest(system: str, dataset: str, inject: list[str]) -> None:
    conf = {"business_date": BD, "datasets": [dataset], "inject": inject}
    say(f"ingest_{system}: {dataset} {BD} inject={inject or '—'} (dags test, runs even while paused)")
    airflow("dags", "test", f"ingest_{system}", "--conf", json.dumps(conf))


def build(dbt_inject: list[str] | None = None) -> None:
    conf = {"business_date": BD, "dbt_inject": dbt_inject or []}
    say(f"dbt_build {BD} dbt_inject={dbt_inject or '—'}")
    airflow("dags", "trigger", "dbt_build", "--conf", json.dumps(conf))


def wait(what: str, check, timeout_s: int = 720, every_s: int = 20):
    say(f"waiting: {what} (up to {timeout_s // 60} min)")
    t_end = time.time() + timeout_s
    while time.time() < t_end:
        got = check()
        if got:
            return got
        time.sleep(every_s)
    return None


def incidents_since(bq, t0: dt.datetime) -> list[dict]:
    """Latest state of every incident that has an event at or after t0 (window bd-2..bd)."""
    return _q(bq, f"""
        with touched as (
          select distinct incident_id from `{bq.project}.{INCIDENT_DS}.incidents`
          where `at` >= timestamp(@t0) and business_date between date_sub(date(@bd), interval 2 day) and date(@bd))
        select * from `{bq.project}.{INCIDENT_DS}.incidents` join touched using (incident_id)
        qualify row_number() over (partition by incident_id order by `at` desc) = 1""",
              t0=t0.isoformat(), bd=BD)


def held_since(bq, t0: dt.datetime) -> dict[str, str]:
    rows = _q(bq, f"""
        select dataset, state from `{bq.project}.{STATE_DS}.publication_state`
        where decided_at >= timestamp(@t0) and business_date = date(@bd)
        qualify row_number() over (partition by dataset order by decided_at desc) = 1""",
              t0=t0.isoformat(), bd=BD)
    return {r["dataset"]: r["state"] for r in rows}


def show(incs: list[dict]) -> None:
    for i in incs:
        print(f"      {i['incident_id']}  {i['failure_type']:18} {i['status']:17} attempt {i['attempt']}  "
              f"{(i.get('note') or '')[:90]}")


# ── one case ────────────────────────────────────────────────────────────────
def run_case(name: str) -> dict:
    c = CASES[name]
    bq = client_as(ENGINE)
    print(f"\n━━ {name}: {c['what']}\n   expected: {c['expect']}")
    t0 = now()
    ok, detail = False, ""

    if c["kind"] == "retry":
        conf = {"business_date": BD, "datasets": ["encounters"], "inject": ["source_timeout"]}
        say(f"ingest_ehr: encounters {BD} inject=['source_timeout'] (dags test; attempt 1 fails, retry after 1 min)")
        r = subprocess.run(["airflow", "dags", "test", "ingest_ehr", "--conf", json.dumps(conf)],
                           capture_output=True, text=True)
        log = r.stdout + r.stderr
        failed_once = "simulated source timeout" in log
        succeeded = r.returncode == 0 and "state=success" in log
        loaded = next((ln.split("loaded ", 1)[1][:90] for ln in log.splitlines() if "loaded encounters" in ln), None)
        new = incidents_since(bq, t0)
        ok = failed_once and succeeded and loaded is not None and not new
        detail = (f"attempt 1 timed out: {'yes' if failed_once else 'no'}; run "
                  f"{'success' if succeeded else 'FAILED (dags test may not retry — see runbook note)'}; "
                  f"{('loaded ' + loaded) if loaded else 'no load'}; {len(new)} new incident(s)")

    elif c["kind"] == "escalate":
        build(c["dbt_inject"])
        def decided():
            incs = incidents_since(bq, t0)
            busy = [i for i in incs if i["status"] in ("OPEN", "ACTING", "AWAITING_APPROVAL")]
            return incs if incs and not busy and any(i["status"] == "ESCALATED" for i in incs) else None
        incs = wait("engine decision", decided) or incidents_since(bq, t0)
        show(incs)
        held = held_since(bq, t0)
        types = {i["failure_type"] for i in incs if i["status"] == "ESCALATED"}
        acted = [i for i in incs if (i["attempt"] or 0) > 0]
        not_held = [m for m in c["held"] if held.get(m) != "HELD"]
        ok = bool(types & c["types"]) and not acted and not not_held
        detail = (f"escalated types {sorted(types) or '—'}; expected one of {sorted(c['types'])}; "
                  f"data actions {len(acted)}; reports {held or '—'}")
        # clean up: a normal build (no switches) — the engine closes the incidents on newer evidence
        t1 = now()
        build([])
        ids = {i["incident_id"] for i in incs}
        def closed():
            latest = {i["incident_id"]: i for i in incidents_since(bq, t1)}
            return latest if ids and ids <= set(latest) and all(
                latest[i]["status"] == "RESOLVED" for i in ids) else None
        cl = wait("clean rebuild → incidents RESOLVED", closed)
        detail += "; cleanup " + ("RESOLVED" if cl else "NOT resolved yet — check `engine status`")

    else:  # auto
        ingest("billing", "claims", ["offsetting_errors"])
        build([])
        key = f"offsetting_errors:claims:{BD}"
        def resolved():
            incs = [i for i in incidents_since(bq, t0) if i["incident_key"] == key]
            return incs if incs and incs[0]["status"] in ("RESOLVED", "ESCALATED") else None
        incs = wait("auto re-extract → RESOLVED", resolved, timeout_s=900) or \
            [i for i in incidents_since(bq, t0) if i["incident_key"] == key]
        show(incs)
        ok = bool(incs) and incs[0]["status"] == "RESOLVED" and int(incs[0]["attempt"] or 0) == 1
        detail = (f"{incs[0]['incident_id']} {incs[0]['status']} attempt {incs[0]['attempt']}"
                  if incs else "no offsetting_errors incident found")

    mins = round((now() - t0).total_seconds() / 60, 1)
    print(f"   {'✅ PASS' if ok else '❌ CHECK'}  {detail}  ({mins} min)")
    return {"case": name, "ok": ok, "detail": detail, "minutes": mins}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("case", choices=[*CASES, "all", "list"])
    a = ap.parse_args()
    if a.case == "list":
        for k, c in CASES.items():
            print(f"{k:20} {c['what']}\n{'':20} → {c['expect']}")
        return
    names = list(CASES) if a.case == "all" else [a.case]
    results = [run_case(n) for n in names]
    print("\n━━ Summary")
    for r in results:
        print(f"  {'✅' if r['ok'] else '❌'} {r['case']:20} {r['minutes']:>5} min  {r['detail']}")
    print(f"  {sum(r['ok'] for r in results)} of {len(results)} passed")


if __name__ == "__main__":
    main()
