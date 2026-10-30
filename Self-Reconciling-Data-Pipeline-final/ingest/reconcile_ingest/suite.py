"""Failure suite — rows of the failure-to-decision table, one command each.

Runs inside the Airflow container (test project only, ALLOW_INJECTIONS=true):
    python -m reconcile_ingest.suite build_error        # one case
    python -m reconcile_ingest.suite all                # every case, then a summary table
    python -m reconcile_ingest.suite pkg1               # Phase 6 package 1 only (outage + freshness)
    python -m reconcile_ingest.suite pkg3               # Phase 6 package 3 (late changes, archive)
    python -m reconcile_ingest.suite pkg4               # Phase 6 package 4 (orphans, merged patient: you approve)
Packages 3 and 4 edit the synthetic SOURCE and undo it in the same case; SUITE_SOURCE_DSN may name a source
user that is allowed to write (the ingest user is read-only on a real replica).
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
from contextlib import contextmanager

from reconcile_ingest.engine import ENGINE, INCIDENT_DS, _q
from reconcile_ingest.publish import STATE_DS, client_as

BD = "2026-10-14"
MARTS = ["dim_encounters", "fct_bed_occupancy", "fct_daily_claims"]   # EHR + Billing reports
LAB_MART = "fct_lab_results_daily"                                       # LIS report (Phase 6 pkg 2)

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
    # ── Phase 6 package 1: outage breaker and freshness ──
    "source_outage": {
        "what": "Billing outage: every claims/payments/adjustments extract times out (T19, T20, T21)",
        "expect": "breaker OPEN → ONE parent incident + 3 children HELD, 0 fixes; fct_daily_claims HELD, "
                  "EHR reports publish; probe alone keeps it OPEN; reload → CLOSED → all RESOLVED",
        "kind": "outage", "pkg": 1,
    },
    "global_outage": {
        "what": "Billing AND LIS down at once",
        "expect": "GLOBAL breaker OPEN → ALL reports HELD; reload both → CLOSED → published",
        "kind": "global", "pkg": 1,
    },
    "stale_feed": {
        "what": "EHR replica 30 h behind (data loads, but it is old)",
        "expect": "stale_feed → WAITING (no data change); all EHR-based reports HELD; fresh probe + load → RESOLVED",
        "kind": "stale", "pkg": 1,
    },
    "late_documentation": {
        "what": "EHR diagnoses: only 35% of the day documented yet (receipt matches)",
        "expect": "late_data → WAITING (no data change); full reload → RESOLVED",
        "kind": "late", "pkg": 1,
    },
    # ── Phase 6 package 2: wrong values and bad releases ──
    "value_mismatch": {
        "what": "A buggy release rounds 14 lab results in staging (raw is right) — T6",
        "expect": "value check FAILS → value_mismatch ESCALATED (value_corruption, L3, never auto); "
                  "fct_lab_results_daily HELD; EHR/Billing reports publish; clean build → RESOLVED",
        "kind": "pkg2", "pkg": 2, "dbt_inject": ["value_truncation"], "type": "value_mismatch",
        "rule": "value_corruption", "held": LAB_MART, "published": "dim_encounters",
    },
    "deployment_regression": {
        "what": "A release changed fct_daily_claims; its join now doubles 25 encounters — T14",
        "expect": "suspected_regression ESCALATED with the release attached; fct_daily_claims HELD; "
                  "no data action; clean build → RESOLVED",
        "kind": "pkg2", "pkg": 2, "dbt_inject": ["join_fanout"], "type": "check_failed",
        "rule": "suspected_regression", "held": "fct_daily_claims", "published": "dim_encounters",
        "release": ["fct_daily_claims"],
    },
    # ── Phase 6 package 3: late changes and expected differences (reversible source edits) ──
    "archived_history": {
        "what": "The EHR archives encounters of 12 Oct (source now has fewer rows than raw)",
        "expect": "archived_history EXPECTED (log_expected, no alarm); every report PUBLISHED; restore → RESOLVED",
        "kind": "pkg3", "pkg": 3,
    },
    "late_change": {
        "what": "3 claims of 13 Oct are reversed at the source after we loaded them",
        "expect": "late_change → rebuild_partition (auto) → claims 13 Oct re-extracted and rebuilt → RESOLVED",
        "kind": "pkg3", "pkg": 3,
    },
    "reversal_beyond_lookback": {
        "what": "3 claims of 11 Oct (older than the 3-day look-back) are reversed at the source",
        "expect": "late_change_old → rebuild_partition with a dbt build FOR 11 Oct (look-back rebuild) → RESOLVED",
        "kind": "pkg3", "pkg": 3,
    },
    # ── Phase 6 package 4: orphan records ──
    "orphans": {
        "what": "Lab results of 14 Oct: 2 point to archived encounters, 3 to encounters that exist nowhere, "
                "2 late-evening ones to encounters of 15 Oct (not loaded yet)",
        "expect": "archived_parent EXPECTED, true_orphan ESCALATED, young_orphan EXPECTED (unknown member); "
                  "restore → RESOLVED",
        "kind": "pkg4", "pkg": 4,
    },
    "merged_patient": {
        "what": "The MPI merges one patient into another; the retired ID still has claims and lab results",
        "expect": "merged_patient → approval card (Slack/console) → YOU approve → remap in staging → RESOLVED",
        "kind": "pkg4", "pkg": 4,
    },
}
PKG1 = [k for k, c in CASES.items() if c.get("pkg") == 1]
PKG3 = [k for k, c in CASES.items() if c.get("pkg") == 3]
PKG4 = [k for k, c in CASES.items() if c.get("pkg") == 4]
PKG2 = [k for k, c in CASES.items() if c.get("pkg") == 2]


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


def ingest(system: str, dataset: str | list[str] | None, inject: list[str], check: bool = True,
           day: str = BD) -> None:
    datasets = [dataset] if isinstance(dataset, str) else dataset
    conf = {"business_date": day, "inject": inject, **({"datasets": datasets} if datasets else {})}
    say(f"ingest_{system}: {datasets or 'all datasets'} {day} inject={inject or '—'} (dags test, runs while paused)"
        + ("" if check else " — expected to FAIL"))
    airflow("dags", "test", f"ingest_{system}", "--conf", json.dumps(conf), check=check)


def build(dbt_inject: list[str] | None = None, day: str = BD) -> None:
    conf = {"business_date": day, "dbt_inject": dbt_inject or []}
    say(f"dbt_build {day} dbt_inject={dbt_inject or '—'}")
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


# Phase 6 pkgs 3+4 run on every engine pass (young orphans, MPI merges, source drift). They are real findings,
# but a case about something else ignores them unless it asks for them by type.
BACKGROUND = {"young_orphan", "merged_patient", "archived_parent", "true_orphan",
              "archived_history", "count_excess", "late_change", "late_change_old"}


def incidents_since(bq, t0: dt.datetime, types: set[str] | None = None, back: int = 2) -> list[dict]:
    """Latest state of every incident that has an event at or after t0 (window bd-back..bd).
    types: only these failure types; default: everything except the BACKGROUND types."""
    rows = _q(bq, f"""
        with touched as (
          select distinct incident_id from `{bq.project}.{INCIDENT_DS}.incidents`
          where `at` >= timestamp(@t0)
            and business_date between date_sub(date(@bd), interval {int(back)} day) and date(@bd))
        select * from `{bq.project}.{INCIDENT_DS}.incidents` join touched using (incident_id)
        qualify row_number() over (partition by incident_id order by `at` desc) = 1""",
              t0=t0.isoformat(), bd=BD)
    return [r for r in rows if (r["failure_type"] in types if types else r["failure_type"] not in BACKGROUND)]


def held_since(bq, t0: dt.datetime) -> dict[str, str]:
    rows = _q(bq, f"""
        select dataset, state from `{bq.project}.{STATE_DS}.publication_state`
        where decided_at >= timestamp(@t0) and business_date = date(@bd)
        qualify row_number() over (partition by dataset order by decided_at desc) = 1""",
              t0=t0.isoformat(), bd=BD)
    return {r["dataset"]: r["state"] for r in rows}


def gate_since(bq, t0: dt.datetime) -> dict[str, tuple[str, str]]:
    rows = _q(bq, f"""
        select dataset, state, reasons from `{bq.project}.{STATE_DS}.publication_state`
        where decided_at >= timestamp(@t0) and business_date = date(@bd) and state != 'PENDING'
        qualify row_number() over (partition by dataset order by decided_at desc) = 1""",
              t0=t0.isoformat(), bd=BD)
    return {r["dataset"]: (r["state"], r["reasons"] or "") for r in rows}


def breakers() -> dict[str, dict]:
    from reconcile_ingest import source_state as ss
    return ss.breaker_states(client_as(ENGINE))


def breaker_check() -> dict:
    from reconcile_ingest import breaker
    say("breaker check")
    return breaker.check()


def probe_only(system: str) -> None:
    from reconcile_ingest import pipeline
    from reconcile_ingest.engine import _ingest_components
    s, wh, _ = _ingest_components()
    r = pipeline.probe_source(s, wh, system)
    say(f"probe {system} only (no load): reachable={r['reachable']}")


def gate_settled(bq, t: dt.datetime, expect: dict[str, str]):
    """Wait until the gate has decided every report in `expect`, then give the engine run time to finish."""
    def ready():
        g = gate_since(bq, t)
        return g if all(m in g for m in expect) else None
    g = wait("gate decision for " + ", ".join(expect), ready) or gate_since(bq, t)
    time.sleep(90)   # recovery_engine starts right after publish; let it decide
    return g


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

    elif c["kind"] in ("outage", "global"):
        ok, detail = run_outage(bq, t0, global_=c["kind"] == "global")

    elif c["kind"] in ("stale", "late"):
        ok, detail = run_freshness(bq, t0, c["kind"])

    elif c["kind"] == "pkg2":
        ok, detail = run_pkg2(bq, t0, c)

    elif c["kind"] in ("pkg3", "pkg4"):
        ok, detail = PKG34_RUNNERS[name](bq, t0)

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


# ── Phase 6 package 1 ───────────────────────────────────────────────────────
def run_outage(bq, t0: dt.datetime, global_: bool) -> tuple[bool, str]:
    systems = ["billing", "lis"] if global_ else ["billing"]
    notes, ok = [], True
    try:
        for sysname in systems:
            ingest(sysname, None, ["source_down"], check=False)
        breaker_check()
        st = breakers()
        opened = [s for s in ("BILLING", "LIS", "GLOBAL") if (st.get(s) or {}).get("state") == "OPEN"]
        want = ["BILLING", "LIS", "GLOBAL"] if global_ else ["BILLING"]
        ok &= all(s in opened for s in want)
        notes.append(f"open {opened}")
        incs = incidents_since(bq, t0)
        parents = [i for i in incs if i["failure_type"] in ("source_outage", "global_outage")]
        children = [i for i in incs if i["failure_type"] == "extract_failed"]
        n_src = len(systems)
        ok &= len([p for p in parents if p["failure_type"] == "source_outage"]) == n_src
        ok &= bool(children) and all(i["status"] == "HELD" and int(i["attempt"] or 0) == 0 for i in children)
        notes.append(f"{len(parents)} parent incident(s), {len(children)} children HELD")
        show(parents + children)

        tb = now()
        build([])
        expect = ({m: "HELD" for m in MARTS} if global_ else
                  {"fct_daily_claims": "HELD", "dim_encounters": "PUBLISHED", "fct_bed_occupancy": "PUBLISHED"})
        g = gate_settled(bq, tb, expect)
        wrong = {m: g.get(m, ("—",))[0] for m, s in expect.items() if g.get(m, ("—",))[0] != s}
        held_why = g.get("fct_daily_claims", ("", ""))[1]
        ok &= not wrong and ("breaker" in held_why or "outage" in held_why)
        acted = [i for i in incidents_since(bq, t0) if int(i["attempt"] or 0) > 0]
        ok &= not acted
        notes.append("gate " + ", ".join(f"{m} {g.get(m, ('—',))[0]}" for m in MARTS)
                     + (f" (WRONG {wrong})" if wrong else "") + f"; data actions {len(acted)}")

        if not global_:   # T21: a passing probe alone must NOT close the breaker
            probe_only("BILLING")
            breaker_check()
            still = (breakers().get("BILLING") or {}).get("state")
            ok &= still == "OPEN"
            notes.append(f"T21 probe only → {still}")
    finally:
        for sysname in systems:   # recovery (and cleanup): the source is back
            ingest(sysname, None, [])
        breaker_check()
    st = breakers()
    closed = all((st.get(s) or {}).get("state") != "OPEN" for s in ("BILLING", "LIS", "GLOBAL"))
    ok &= closed
    tc = now()
    build([])
    g = gate_settled(bq, tc, {m: "PUBLISHED" for m in MARTS})
    def settled():
        incs = incidents_since(bq, t0)
        return incs if incs and all(i["status"] == "RESOLVED" for i in incs) else None
    final = wait("all outage incidents RESOLVED", settled, timeout_s=300) or incidents_since(bq, t0)
    ok &= bool(final) and all(i["status"] == "RESOLVED" for i in final) and \
        all(g.get(m, ("—",))[0] == "PUBLISHED" for m in MARTS)
    notes.append(f"after reload: breakers {'CLOSED' if closed else 'still OPEN'}, "
                 f"incidents {sorted({i['status'] for i in final})}, "
                 + ", ".join(f"{m} {g.get(m, ('—',))[0]}" for m in MARTS))
    return bool(ok), "; ".join(notes)


def run_freshness(bq, t0: dt.datetime, kind: str) -> tuple[bool, str]:
    if kind == "stale":
        system, ds, inject, ftype = "ehr", "encounters", ["stale_feed"], "stale_feed"
        expect = {m: "HELD" for m in MARTS}
    else:
        system, ds, inject, ftype = "ehr", "diagnoses", ["late_documentation"], "late_data"
        expect = {}
    notes, ok = [], True
    ingest(system, ds, inject)
    tb = now()
    build([])
    if expect:
        g = gate_settled(bq, tb, expect)
        wrong = {m: g.get(m, ("—",))[0] for m, s in expect.items() if g.get(m, ("—",))[0] != s}
        ok &= not wrong
        notes.append("gate " + ", ".join(f"{m} {g.get(m, ('—',))[0]}" for m in MARTS))
    def waiting():
        incs = [i for i in incidents_since(bq, t0) if i["failure_type"] == ftype]
        return incs if incs and incs[0]["status"] in ("WAITING", "ESCALATED") else None
    incs = wait(f"engine: {ftype} → WAITING", waiting) or \
        [i for i in incidents_since(bq, t0) if i["failure_type"] == ftype]
    show(incs)
    ok &= bool(incs) and incs[0]["status"] == "WAITING" and int(incs[0]["attempt"] or 0) == 1
    if not incs and kind == "late":
        from reconcile_ingest.engine import load_policy
        from reconcile_ingest.source_state import volume_report
        v = volume_report(bq, dt.date.fromisoformat(BD), load_policy(), ["diagnoses"]).get("diagnoses")
        notes.append(f"volume check saw {v} (needs at least 2 earlier loaded days of diagnoses)")
    notes.append(f"{ftype}: " + (f"{incs[0]['status']} attempt {incs[0]['attempt']}" if incs else "no incident"))
    acted = [i for i in incidents_since(bq, t0) if i["status"] == "ACTING"]
    ok &= not acted
    # recovery: the feed catches up / the rest of the day is documented
    ingest(system, ds, [])
    tc = now()
    build([])
    ids = {i["incident_id"] for i in incs}
    def resolved():
        latest = {i["incident_id"]: i for i in incidents_since(bq, tc)}
        return latest if ids and ids <= set(latest) and all(latest[i]["status"] == "RESOLVED" for i in ids) else None
    cl = wait("fresh data → RESOLVED", resolved)
    ok &= bool(cl)
    notes.append("cleanup " + ("RESOLVED" if cl else "NOT resolved yet — check `engine status`"))
    return bool(ok), "; ".join(notes)


# ── Phase 6 package 2 ───────────────────────────────────────────────────────
def run_pkg2(bq, t0: dt.datetime, c: dict) -> tuple[bool, str]:
    notes, ok, rel_id = [], True, None
    if c.get("release"):   # a test release that stops counting after an hour
        from reconcile_ingest import releases
        rel_id = releases.record(c["release"], "suite-test", "failure suite: deployment_regression",
                                 expires_minutes=60)["release_id"]
        notes.append(f"release {rel_id} touches {c['release']}")
    tb = now()
    build(c["dbt_inject"])
    g = gate_settled(bq, tb, {c["held"]: "HELD", c["published"]: "PUBLISHED"})
    ok &= g.get(c["held"], ("—",))[0] == "HELD" and g.get(c["published"], ("—",))[0] == "PUBLISHED"
    notes.append(f"gate {c['held']} {g.get(c['held'], ('—', ''))[0]} ({g.get(c['held'], ('', ''))[1][:70]}), "
                 f"{c['published']} {g.get(c['published'], ('—',))[0]}")
    def decided():
        incs = [i for i in incidents_since(bq, t0) if i.get("rule_id") == c["rule"]]
        return incs if incs and all(i["status"] == "ESCALATED" for i in incs) else None
    incs = wait(f"engine: {c['rule']} → ESCALATED", decided) or \
        [i for i in incidents_since(bq, t0) if i.get("rule_id") == c["rule"]]
    show(incs)
    ok &= bool(incs) and all(i["failure_type"] == c["type"] and i["status"] == "ESCALATED" for i in incs)
    if rel_id:
        ok &= all(rel_id in (i.get("evidence") or "") for i in incs)
        notes.append("release attached to the incident" if incs and rel_id in (incs[0].get("evidence") or "")
                     else "release NOT attached")
    acted = [i for i in incidents_since(bq, t0) if int(i["attempt"] or 0) > 0]
    ok &= not acted
    notes.append(f"{c['rule']}: {len(incs)} incident(s) ESCALATED; data actions {len(acted)}")
    tc = now()
    build([])
    ids = {i["incident_id"] for i in incs}
    def closed():
        latest = {i["incident_id"]: i for i in incidents_since(bq, tc)}
        return latest if ids and ids <= set(latest) and all(latest[i]["status"] == "RESOLVED" for i in ids) else None
    cl = wait("clean rebuild → incidents RESOLVED", closed)
    ok &= bool(cl)
    notes.append("cleanup " + ("RESOLVED" if cl else "NOT resolved yet — check `engine status`"))
    return bool(ok), "; ".join(notes)


# ── Phase 6 packages 3 and 4: reversible edits to the SOURCE (test project only) ─
A_DAY, C_DAY, O_DAY = "2026-10-12", "2026-10-13", "2026-10-11"     # archived · late change · beyond look-back
SHIFT = dt.timedelta(days=3650)   # "archiving" = the encounters move out of the day (no deletes, no FK issues)


def _source_dsn(write: bool) -> str:
    """The suite edits the synthetic source the way the hospital systems would. SUITE_SOURCE_DSN (a user that may
    write, e.g. the datagen one) is used for that; otherwise the ingest DSN."""
    import os

    from reconcile_ingest.settings import Settings
    st = Settings.from_env()
    if write and not st.allow_injections:
        raise PermissionError("source edits are test-project only (ALLOW_INJECTIONS=true)")
    return (os.environ.get("SUITE_SOURCE_DSN") if write else None) or st.source_dsn


@contextmanager
def source_rw():
    """Write access to the synthetic source, for the suite only. Every edit is undone by the same case."""
    import psycopg
    with psycopg.connect(_source_dsn(write=True)) as conn:      # commits when the block ends without an error
        yield conn


def _reader_role() -> str | None:
    from psycopg.conninfo import conninfo_to_dict
    return conninfo_to_dict(_source_dsn(write=False)).get("user")


def _archive_add(src, encounter_id: str) -> None:
    """Insert a test row into ehr.archive_index, filling whatever columns that table requires."""
    cols = src.execute("""select column_name, data_type from information_schema.columns
                          where table_schema = 'ehr' and table_name = 'archive_index'
                            and is_nullable = 'NO' and column_default is null and column_name <> 'encounter_id'
                          order by ordinal_position""").fetchall()
    filler = {"date": "current_date", "integer": "0", "bigint": "0", "smallint": "0", "numeric": "0",
              "boolean": "false"}
    names = ["encounter_id"] + [c for c, _ in cols]
    values = ["%s"] + [("now()" if t.startswith("timestamp") else filler.get(t, "'suite-test'")) for _, t in cols]
    src.execute(f"insert into ehr.archive_index ({', '.join(names)}) values ({', '.join(values)}) "
                "on conflict do nothing", (encounter_id,))


def source_ready() -> tuple[bool, str]:
    """Preflight: can the suite make (and undo) the source edits packages 3 and 4 need?"""
    import psycopg
    need = {"ehr.encounters": "UPDATE", "billing.claims": "UPDATE", "ehr.archive_policy": "UPDATE"}
    try:
        with source_rw() as src:
            src.execute("create schema if not exists mpi")
            src.execute("create table if not exists mpi.patient_merges (retired_id text primary key, "
                        "surviving_id text not null, merged_at timestamptz not null default now())")
            src.execute("create table if not exists ehr.archive_index (encounter_id text primary key, "
                        "archived_ts timestamptz not null default now())")
            reader = _reader_role()
            me = src.execute("select current_user").fetchone()[0]
            if reader and reader != me:          # the engine reads with the ingest user (read-only)
                src.execute(f'grant usage on schema mpi to "{reader}"')
                src.execute(f'grant select on mpi.patient_merges, ehr.archive_index to "{reader}"')
            missing = [f"{t} {p}" for t, p in need.items()
                       if not src.execute("select to_regclass(%s) is not null and has_table_privilege(%s, %s)",
                                          (t, t, p)).fetchone()[0]]
            src.execute("select 1 from ehr.archive_index limit 1")
            _archive_add(src, "E-PRECHECK")
            src.execute("delete from ehr.archive_index where encounter_id = 'E-PRECHECK'")
    except (psycopg.Error, PermissionError) as exc:
        return False, f"{type(exc).__name__}: {str(exc).splitlines()[0][:200]}"
    if missing:
        return False, "missing privileges: " + ", ".join(missing)
    return True, f"source writable as {me} (engine reads as {reader or me})"


def _day_bounds(day: str) -> tuple[dt.datetime, dt.datetime]:
    from zoneinfo import ZoneInfo
    start = dt.datetime.combine(dt.date.fromisoformat(day), dt.time(), tzinfo=ZoneInfo("Asia/Kolkata"))
    return start, start + dt.timedelta(days=1)


def raw_rows(bq, ds: str, day: str) -> int:
    return int(_q(bq, f"select count(*) n from `{bq.project}.raw.{ds}` where business_date = date(@d)", d=day)[0]["n"])


def wait_types(bq, t0, types: set[str], want: dict[str, str], back: int = 2, timeout_s: int = 720):
    """Wait until every type in `want` has an incident in the wanted status."""
    def ready():
        incs = incidents_since(bq, t0, types=types, back=back)
        got: dict[str, set] = {}
        for i in incs:
            got.setdefault(i["failure_type"], set()).add(i["status"])
        return incs if all(st in got.get(t, set()) for t, st in want.items()) else None
    return wait(" · ".join(f"{t} → {st}" for t, st in want.items()), ready, timeout_s=timeout_s) or \
        incidents_since(bq, t0, types=types, back=back)


def _statuses(incs: list[dict]) -> dict[str, set]:
    out: dict[str, set] = {}
    for i in incs:
        out.setdefault(i["failure_type"], set()).add(i["status"])
    return out


def _preflight() -> tuple[bool, str] | None:
    ok, why = source_ready()
    say(("source preflight: " + why) if ok else f"source preflight FAILED: {why}")
    return None if ok else (False, f"source not writable for the suite ({why}) — set SUITE_SOURCE_DSN, see runbook")


def run_archived_history(bq, t0):
    if (bad := _preflight()):
        return bad
    notes, ok = [], True
    if not raw_rows(bq, "encounters", A_DAY):
        ingest("ehr", "encounters", [], day=A_DAY)
    s, e = _day_bounds(A_DAY)
    with source_rw() as src:
        row = src.execute("select cutoff_date from ehr.archive_policy where id = 1").fetchone()
        had_row, old_cutoff = row is not None, row[0] if row else None
        moved = src.execute("update ehr.encounters set admit_ts = admit_ts - %s where admit_ts >= %s and admit_ts < %s",
                            (SHIFT, s, e)).rowcount
        new_cutoff = dt.date.fromisoformat(A_DAY) + dt.timedelta(days=1)
        if had_row:
            src.execute("update ehr.archive_policy set cutoff_date = %s where id = 1", (new_cutoff,))
        else:
            src.execute("insert into ehr.archive_policy (id, cutoff_date) values (1, %s)", (new_cutoff,))
    say(f"source: {moved} encounters of {A_DAY} archived (moved out of the day), archive cutoff → {new_cutoff}")
    try:
        tb = now()
        build([])
        incs = wait_types(bq, t0, {"archived_history", "count_excess"}, {"archived_history": "EXPECTED"})
        show(incs)
        g = gate_settled(bq, tb, {m: "PUBLISHED" for m in MARTS}) or {}
        st = _statuses(incs)
        no_alarm = "count_excess" not in st and all((g.get(m) or ("—",))[0] == "PUBLISHED" for m in MARTS)
        ok &= no_alarm and "EXPECTED" in st.get("archived_history", set())
        notes.append(f"archived_history {sorted(st.get('archived_history', [])) or '—'}; "
                     + ("reports PUBLISHED, no alarm" if no_alarm else f"alarm or held: {st} {g}"))
    finally:
        with source_rw() as src:
            src.execute("update ehr.encounters set admit_ts = admit_ts + %s where admit_ts >= %s and admit_ts < %s",
                        (SHIFT, s - SHIFT, e - SHIFT))
            if had_row:
                src.execute("update ehr.archive_policy set cutoff_date = %s where id = 1", (old_cutoff,))
            else:
                src.execute("delete from ehr.archive_policy where id = 1")
        say("source restored (encounters back in their day, cutoff back)")
    tc = now()
    build([])
    res = wait_types(bq, tc, {"archived_history"}, {"archived_history": "RESOLVED"}, timeout_s=600)
    back_ok = "RESOLVED" in _statuses(res).get("archived_history", set())
    ok &= back_ok
    notes.append("after restore " + ("RESOLVED" if back_ok else "NOT resolved"))
    return bool(ok), "; ".join(notes)


def _reverse_claims(day: str, n: int = 3) -> list[tuple]:
    s, e = _day_bounds(day)
    with source_rw() as src:
        rows = src.execute("select claim_id, status, adjudicated_ts from billing.claims where submitted_ts >= %s "
                           "and submitted_ts < %s and status != 'reversed' order by claim_id limit %s",
                           (s, e, n)).fetchall()
        for cid, _, _ in rows:
            src.execute("update billing.claims set status = 'reversed', adjudicated_ts = now() where claim_id = %s",
                        (cid,))
    say(f"source: {len(rows)} claims of {day} reversed after they were loaded")
    return rows


def _restore_claims(rows: list[tuple]) -> None:
    with source_rw() as src:
        for cid, status, adj in rows:
            src.execute("update billing.claims set status = %s, adjudicated_ts = %s where claim_id = %s",
                        (status, adj, cid))
    say("source restored (claim statuses back)")


def run_late_change(bq, t0, day: str = C_DAY, kind: str = "late_change", back: int = 2):
    if (bad := _preflight()):
        return bad
    notes, ok = [], True
    if not raw_rows(bq, "claims", day):
        ingest("billing", "claims", [], day=day)
    rows = _reverse_claims(day)
    if not rows:
        return False, f"no claims to reverse on {day}"
    try:
        build([])
        incs = wait_types(bq, t0, {kind}, {kind: "RESOLVED"}, back=back, timeout_s=1200)
        show(incs)
        fixed = [i for i in incs if i["status"] == "RESOLVED" and int(i.get("attempt") or 0) <= 1]
        ok &= bool(fixed)
        notes.append(f"{kind}: " + (f"rebuild_partition re-extracted and rebuilt {fixed[0]['business_date']}, "
                                    "RESOLVED at attempt 1" if fixed else f"{_statuses(incs) or '—'}"))
    finally:
        _restore_claims(rows)
        ingest("billing", "claims", [], day=day)      # raw = source again (no drift left behind)
        build([], day=day)
    return bool(ok), "; ".join(notes)


def run_beyond_lookback(bq, t0):
    s, e = _day_bounds(O_DAY)
    import psycopg
    with psycopg.connect(_source_dsn(write=False)) as src:
        n = src.execute("select count(*) from billing.claims where submitted_ts >= %s and submitted_ts < %s",
                        (s, e)).fetchone()[0]
    if not n:
        return False, f"no source claims for {O_DAY} — generate that day first (datagen)"
    return run_late_change(bq, t0, day=O_DAY, kind="late_change_old", back=4)


def _young_now(bq) -> tuple[str | None, int, dt.datetime | None]:
    """Latest state of today's young-orphan record: (status, parents_missing, at). Real data can already have
    a few young orphans (late-evening results whose encounter comes with tomorrow's load)."""
    rows = _q(bq, f"""select status, evidence, `at` from `{bq.project}.{INCIDENT_DS}.incidents`
                      where incident_key = @k qualify row_number() over (order by `at` desc) = 1""",
              k=f"young_orphan:lab_results:{BD}")
    if not rows:
        return None, 0, None
    try:
        n = int(json.loads(rows[0]["evidence"] or "{}").get("parents_missing") or 0)
    except (ValueError, TypeError, AttributeError):
        n = 0
    return rows[0]["status"], n, rows[0]["at"]


def run_orphans(bq, t0):
    if (bad := _preflight()):
        return bad
    from reconcile_ingest.pipeline import ARCHIVED_TEST_ENCOUNTERS
    notes, ok = [], True
    st0, base, _ = _young_now(bq)
    base = base if st0 == "EXPECTED" else 0
    say(f"young orphans before the case: {base} (real late-evening results, already recorded)" if base
        else "young orphans before the case: none")
    with source_rw() as src:
        for eid in ARCHIVED_TEST_ENCOUNTERS:
            _archive_add(src, eid)
    say(f"source: {len(ARCHIVED_TEST_ENCOUNTERS)} test encounters registered in the EHR archive index")
    want = {"archived_parent": "EXPECTED", "true_orphan": "ESCALATED"}
    try:
        ingest("lis", "lab_results", ["orphan_results"])
        build([])

        def ready():
            incs = incidents_since(bq, t0, types=set(want))
            got = {i["failure_type"]: i["status"] for i in incs}
            st, n, at = _young_now(bq)
            young = st == "EXPECTED" and n >= base + 2 and at is not None and at >= t0
            return (incs, n) if all(got.get(t) == v for t, v in want.items()) and young else None
        res = wait(" · ".join(f"{t} → {v}" for t, v in want.items()) + f" · young_orphan → EXPECTED ({base + 2})",
                   ready)
        incs = res[0] if res else incidents_since(bq, t0, types=set(want))
        show(incs)
        got = {i["failure_type"]: i["status"] for i in incs}
        st, n, _ = _young_now(bq)
        young_ok = st == "EXPECTED" and n >= base + 2
        ok &= all(got.get(t) == v for t, v in want.items()) and young_ok
        notes.append(", ".join(f"{t} {got.get(t, '—')}" for t in want)
                     + f", young_orphan {st or '—'} ({n} parents missing, {base} before)")
    finally:
        with source_rw() as src:
            src.execute("delete from ehr.archive_index where encounter_id = any(%s)", (list(ARCHIVED_TEST_ENCOUNTERS),))
        ingest("lis", "lab_results", [])               # clean batch: every result points at its real encounter
        say("restored (archive index test rows removed, lab results re-loaded clean)")
    tc = now()
    build([])

    def settled():
        res = incidents_since(bq, tc, types=set(want))
        cleared = {i["failure_type"] for i in res if i["status"] == "RESOLVED"}
        st, n, _ = _young_now(bq)
        young_back = st == "RESOLVED" or (st == "EXPECTED" and n == base)
        return cleared if set(want) <= cleared and young_back else None
    cleared = wait("clean load → orphans resolved, young orphans back to the real count", settled, timeout_s=600)
    ok &= bool(cleared)
    st, n, _ = _young_now(bq)
    notes.append("after clean load " + ("all back to normal" if cleared else f"not settled (young {st} {n})"))
    return bool(ok), "; ".join(notes)


def _latest_cards(bq, where: str, **params) -> list[dict]:
    return _q(bq, f"""select approval_id, status, incident_id from `{bq.project}.{INCIDENT_DS}.approvals`
                      where {where}
                      qualify row_number() over (partition by approval_id order by `at` desc) = 1""", **params)


def run_merged_patient(bq, t0):
    if (bad := _preflight()):
        return bad
    notes, ok = [], True
    s, e = _day_bounds(BD)
    with source_rw() as src:
        pats = [r[0] for r in src.execute("select distinct patient_id from billing.claims where submitted_ts >= %s "
                                          "and submitted_ts < %s and patient_id is not null order by 1 limit 2",
                                          (s, e)).fetchall()]
        if len(pats) < 2:
            return False, f"need two patients with claims on {BD}"
        retired, surviving = pats
        src.execute("insert into mpi.patient_merges (retired_id, surviving_id) values (%s, %s) "
                    "on conflict (retired_id) do update set surviving_id = excluded.surviving_id", (retired, surviving))
    say("source: the MPI merged one patient into another (the retired ID still has claims and lab results)")
    try:
        build([])
        incs = wait_types(bq, t0, {"merged_patient"}, {"merged_patient": "AWAITING_APPROVAL"})
        show(incs)
        pend = [c["approval_id"] for c in _latest_cards(bq, "incident_id in unnest(@ids)",
                                                         ids=[i["incident_id"] for i in incs] or ["-"])
                if c["status"] == "PENDING"]
        if not pend:
            return False, f"no PENDING card for merged_patient ({_statuses(incs) or 'no incident'})"
        print(f"\n   >>> NOW: approve card {', '.join(pend)} in Slack or in the console. "
              "The suite waits up to 20 minutes. <<<\n", flush=True)

        def decided():
            dec = _q(bq, f"select approval_id from `{bq.project}.{INCIDENT_DS}.approval_decisions` "
                         "where approval_id in unnest(@a)", a=pend)
            return dec or None
        if not wait("your decision on the card", decided, timeout_s=1200, every_s=15):
            return False, f"no decision on {pend[0]} within 20 minutes"
        airflow("dags", "trigger", "approval_worker")
        res = wait_types(bq, t0, {"merged_patient"}, {"merged_patient": "RESOLVED"}, timeout_s=900)
        show(res)
        remap = int(_q(bq, f"select count(*) n from `{bq.project}.ops.patient_remaps` where retired_id = @r",
                       r=retired)[0]["n"])
        fixed = "RESOLVED" in _statuses(res).get("merged_patient", set())
        ok &= fixed and remap > 0
        notes.append(f"card {pend[0]} decided; remap rows {remap}; merged_patient "
                     + ("RESOLVED (staging now uses the surviving ID)" if fixed else f"{_statuses(res)}"))
    finally:
        with source_rw() as src:
            src.execute("delete from mpi.patient_merges where retired_id = %s", (retired,))
        _q(bq, f"delete from `{bq.project}.ops.patient_remaps` where retired_id = @r", r=retired)
        say("restored (MPI merge row and remap row removed)")
        build([])
    return bool(ok), "; ".join(notes)


PKG34_RUNNERS = {"archived_history": run_archived_history, "late_change": run_late_change,
                 "reversal_beyond_lookback": run_beyond_lookback, "orphans": run_orphans,
                 "merged_patient": run_merged_patient}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("case", choices=[*CASES, "all", "pkg1", "pkg2", "pkg3", "pkg4", "list"])
    a = ap.parse_args()
    if a.case == "list":
        for k, c in CASES.items():
            print(f"{k:20} {c['what']}\n{'':20} → {c['expect']}")
        return
    groups = {"all": [k for k in CASES if k != "merged_patient"],   # merged_patient needs a person: run it alone
              "pkg1": PKG1, "pkg2": PKG2, "pkg3": PKG3, "pkg4": PKG4}
    names = groups.get(a.case, [a.case])
    results = [run_case(n) for n in names]
    print("\n━━ Summary")
    for r in results:
        print(f"  {'✅' if r['ok'] else '❌'} {r['case']:20} {r['minutes']:>5} min  {r['detail']}")
    print(f"  {sum(r['ok'] for r in results)} of {len(results)} passed")


if __name__ == "__main__":
    main()
