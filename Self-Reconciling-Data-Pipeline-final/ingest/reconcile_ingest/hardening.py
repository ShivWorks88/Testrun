"""Phase 6 hardening pass — one read-only health check of guardrails, patient data and test leftovers.

    python -m reconcile_ingest.hardening            # report (PASS / WARN / FAIL), exit 1 on any FAIL
    python -m reconcile_ingest.hardening --fix      # also remove clearly-test rows the suite may leave behind
                                                    # in the synthetic source (test project only)

Runs inside the Airflow container. What it checks:
  policy      kill switch off, agents off for real data, residency not claimed, Model Armor required,
              never_auto still holds full_refresh / rollback / source_delete, attempt cap 1-3
  T26         no patient identifiers or values in agent calls, approval cards or the incident journal
  journal     incidents still open (not counting quiet EXPECTED ones), cards waiting, open outage breakers
  releases    test releases recorded by the suite that have not expired
  source      test rows in the synthetic source (archive index test encounters, the preflight row, test MPI
              merges, encounters the archive case moved out of their day, an old suite backup table)
  airflow     ingest DAGs paused (only one person ingests, by hand)
  settings    ALLOW_INJECTIONS (expected true in this test project; must be false anywhere else)
Every check is a count or a yes/no — no record, patient or claim identifiers are printed.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import dataclass

TEST_ENCOUNTER_PATTERNS = ["E-20261001-7777%", "E-PRECHECK", "E-2026____-9999_", "E-2026____-8888_"]
TEST_RETIRED_PATIENT = "P-9999%"
SHIFTED_BEFORE = "2020-01-01"          # the archived_history case moves a day's encounters back 10 years
NEVER_AUTO_REQUIRED = {"full_refresh", "rollback", "source_delete"}


@dataclass
class Check:
    area: str
    name: str
    status: str          # PASS | WARN | FAIL | SKIP
    detail: str


# ── pure checks (unit-tested) ───────────────────────────────────────────────
def policy_checks(p: dict) -> list[Check]:
    out = []
    ag = p.get("agents") or {}
    armor = ag.get("model_armor") or {}
    cap = int((p.get("limits") or {}).get("attempt_cap", 0) or 0)
    never = set(p.get("never_auto") or [])
    out.append(Check("policy", "kill switch", "PASS" if p.get("kill_switch") is False else "WARN",
                     "off — safe fixes run" if p.get("kill_switch") is False
                     else "ON — recommend only (never commit policy.yml with kill_switch: true)"))
    out.append(Check("policy", "agents for real data", "PASS" if not ag.get("enabled") else "FAIL",
                     "agents.enabled false (POC-SYNTHETIC-ONLY)" if not ag.get("enabled")
                     else "agents.enabled is true — not approved for real data"))
    out.append(Check("policy", "residency claim", "PASS" if not ag.get("residency_confirmed") else "FAIL",
                     "residency_confirmed false" if not ag.get("residency_confirmed")
                     else "residency_confirmed is true without a recorded approval"))
    out.append(Check("policy", "Model Armor", "PASS" if armor.get("template") and armor.get("required", True)
                     and armor.get("location", "asia-south1") == "asia-south1" else "FAIL",
                     f"template {armor.get('template') or '—'}, required {armor.get('required', True)}, "
                     f"{armor.get('location', 'asia-south1')}"))
    missing = sorted(NEVER_AUTO_REQUIRED - never)
    out.append(Check("policy", "never automatic", "PASS" if not missing else "FAIL",
                     ", ".join(sorted(never)) if not missing else f"missing: {', '.join(missing)}"))
    out.append(Check("policy", "attempt cap", "PASS" if 1 <= cap <= 3 else "FAIL", f"{cap} per incident"))
    return out


def count_check(area: str, name: str, n: int | None, ok_detail: str, bad_detail: str, bad: str = "WARN") -> Check:
    if n is None:
        return Check(area, name, "SKIP", "could not be read")
    return Check(area, name, "PASS" if n == 0 else bad, ok_detail if n == 0 else bad_detail.format(n=n))


def paused_check(dags: list[dict]) -> Check:
    ingest = [d for d in dags if str(d.get("dag_id", "")).startswith("ingest_")]
    running = sorted(d["dag_id"] for d in ingest if str(d.get("is_paused")).lower() not in ("true", "1"))
    if not ingest:
        return Check("airflow", "ingest DAGs paused", "SKIP", "no ingest DAGs listed")
    return Check("airflow", "ingest DAGs paused", "PASS" if not running else "WARN",
                 f"{len(ingest)} paused" if not running else f"scheduled: {', '.join(running)} — keep the toggles OFF")


def exit_code(checks: list[Check]) -> int:
    return 1 if any(c.status == "FAIL" for c in checks) else 0


# ── reads ───────────────────────────────────────────────────────────────────
def _bq_count(bq, sql: str, **params) -> int | None:
    from reconcile_ingest.engine import _q
    try:
        return int(_q(bq, sql, **params)[0]["n"])
    except Exception:  # noqa: BLE001 — a missing table is reported as SKIP, never as a crash
        return None


def journal_checks(bq) -> list[Check]:
    from reconcile_ingest.engine import INCIDENT_DS
    from reconcile_ingest.source_state import OPS_DS
    p = bq.project
    open_n = _bq_count(bq, f"""select count(*) n from (
        select status from `{p}.{INCIDENT_DS}.incidents`
        qualify row_number() over (partition by incident_id order by `at` desc) = 1)
        where status not in ('RESOLVED', 'EXPECTED')""")
    cards = _bq_count(bq, f"""select count(*) n from (
        select status, expires_at from `{p}.{INCIDENT_DS}.approvals`
        qualify row_number() over (partition by approval_id order by `at` desc) = 1)
        where status = 'PENDING' and expires_at > current_timestamp()""")
    breakers = _bq_count(bq, f"""select count(*) n from (
        select state from `{p}.{OPS_DS}.breakers`
        qualify row_number() over (partition by source_system order by `at` desc) = 1) where state = 'OPEN'""")
    releases = _bq_count(bq, f"""select count(*) n from `{p}.{OPS_DS}.releases`
        where git_sha = 'suite-test' and (expires_at is null or expires_at > current_timestamp())""")
    return [
        count_check("journal", "open incidents", open_n, "none open (quiet EXPECTED ones not counted)",
                    "{n} still open — resolve or explain before the demo"),
        count_check("journal", "cards waiting", cards, "no approval card waiting",
                    "{n} PENDING — decide or let expire"),
        count_check("journal", "outage breakers", breakers, "all closed", "{n} OPEN — a source is down", "FAIL"),
        count_check("releases", "suite test releases", releases, "none active",
                    "{n} active suite-test release(s) — they would mark real failures as regressions"),
    ]


def source_checks(fix: bool) -> list[Check]:
    from reconcile_ingest.source_checks import source
    out = []
    try:
        with source() as src:
            def n(sql, *a):
                return int(src.execute(sql, a).fetchone()[0])
            reg = lambda name: bool(src.execute("select to_regclass(%s) is not null", (name,)).fetchone()[0])  # noqa: E731
            arch = sum(n("select count(*) from ehr.archive_index where encounter_id like %s", pat)
                       for pat in TEST_ENCOUNTER_PATTERNS) if reg("ehr.archive_index") else 0
            merges = n("select count(*) from mpi.patient_merges where retired_id like %s", TEST_RETIRED_PATIENT) \
                if reg("mpi.patient_merges") else 0
            shifted = n("select count(*) from ehr.encounters where admit_ts < %s", SHIFTED_BEFORE)
            backup = reg("public.suite_backup_encounters")
            labs = n("select count(*) from lis.lab_results where encounter_id like %s or encounter_id like %s",
                     "E-2026____-9999_", "E-2026____-8888_")
    except Exception as exc:  # noqa: BLE001
        return [Check("source", "test leftovers", "SKIP", f"source not reachable ({type(exc).__name__})")]
    out.append(count_check("source", "archive index test rows", arch, "none", "{n} test row(s)"))
    out.append(count_check("source", "test MPI merges", merges, "none", "{n} test merge row(s)"))
    out.append(count_check("source", "encounters moved out of their day", shifted, "none",
                           "{n} encounter(s) before 2020 — an archived_history run did not finish its restore",
                           "FAIL"))
    out.append(Check("source", "old suite backup table", "WARN" if backup else "PASS",
                     "public.suite_backup_encounters exists" if backup else "none"))
    out.append(count_check("source", "lab results on test encounters", labs, "none", "{n} — re-load needed", "FAIL"))
    if fix and (arch or merges or backup):
        from reconcile_ingest.suite import source_rw
        with source_rw() as w:
            for pat in TEST_ENCOUNTER_PATTERNS:
                w.execute("delete from ehr.archive_index where encounter_id like %s", (pat,))
            if merges:
                w.execute("delete from mpi.patient_merges where retired_id like %s", (TEST_RETIRED_PATIENT,))
            if backup:
                w.execute("drop table public.suite_backup_encounters")
        out.append(Check("source", "cleanup", "PASS", "test archive rows, test merges and the backup table removed"))
    return out


def airflow_checks() -> list[Check]:
    try:
        r = subprocess.run(["airflow", "dags", "list", "-o", "json"], capture_output=True, text=True, timeout=120)
        return [paused_check(json.loads(r.stdout or "[]"))]
    except Exception as exc:  # noqa: BLE001
        return [Check("airflow", "ingest DAGs paused", "SKIP", type(exc).__name__)]


def settings_checks() -> list[Check]:
    from reconcile_ingest.settings import Settings
    s = Settings.from_env()
    return [Check("settings", "ALLOW_INJECTIONS", "WARN" if s.allow_injections else "PASS",
                  "true — fine in this test project; must be false in any other" if s.allow_injections
                  else "false")]


def t26_check(bq) -> list[Check]:
    from reconcile_ingest import phi_scan
    try:
        results = phi_scan.scan(bq)
    except Exception as exc:  # noqa: BLE001
        return [Check("T26", "patient data in agent calls / cards / journal", "SKIP", type(exc).__name__)]
    bad = sum(r["found"] for r in results)
    return [Check("T26", "patient data in agent calls / cards / journal", "PASS" if bad == 0 else "FAIL",
                  f"0 findings in {len(results)} checks" if bad == 0 else f"{bad} finding(s) — run phi_scan")]


def main() -> int:
    from reconcile_ingest.engine import ENGINE, client_as, load_policy
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--fix", action="store_true", help="remove clearly-test rows from the synthetic source")
    a = ap.parse_args()
    bq = client_as(ENGINE)
    checks = policy_checks(load_policy()) + t26_check(bq) + journal_checks(bq) + source_checks(a.fix) + \
        airflow_checks() + settings_checks()
    mark = {"PASS": "✅", "WARN": "⚠️ ", "FAIL": "❌", "SKIP": "·"}
    print("Hardening pass — counts only")
    for c in checks:
        print(f"  {mark[c.status]} {c.area:9} {c.name:42} {c.detail}")
    tally = {s: sum(c.status == s for c in checks) for s in ("PASS", "WARN", "FAIL", "SKIP")}
    print(f"  {tally['PASS']} pass · {tally['WARN']} warn · {tally['FAIL']} fail · {tally['SKIP']} skipped")
    return exit_code(checks)


if __name__ == "__main__":
    sys.exit(main())
