"""Phase 4/5 — recovery engine (deterministic rules, no AI), driven by policy/policy.yml.

After every dbt_build the engine reads what the gate recorded, opens an incident per problem,
diagnoses it against the source receipt, picks the rule from policy.yml (stage 1 safety overrides,
then stage 2 ordinary rules, first match wins), applies the five gates and — only when the rule's
route is `auto` and the action is safe — re-extracts the bad day and asks dbt_build to rebuild and
re-verify. Every decision records the policy version, rule, severity and route.

Phase 5 stage 1 (approvals): when the rule's route is L2 and the action is one the executor can run,
the engine writes an approval card (facts from the engine, never typed by a person) and the incident
waits in AWAITING_APPROVAL. The data owner decides in the console. `process-approvals` (DAG
approval_worker) expires unanswered cards, and before acting on an approval re-checks everything:
same evidence fingerprint, incident still waiting, same policy version, kill switch off, attempt cap.

    python -m reconcile_ingest.engine evaluate --date 2026-10-14 [--dry-run]
    python -m reconcile_ingest.engine diagnose --dataset encounters --date 2026-10-14
    python -m reconcile_ingest.engine status
    python -m reconcile_ingest.engine policy        # show what the engine reads from policy.yml
    python -m reconcile_ingest.engine approvals     # list approval cards and their state
    python -m reconcile_ingest.engine process-approvals [--pretend-later-minutes 180]

Phase 6 package 1: before deciding, the engine runs the outage breaker (reconcile_ingest.breaker) and
reads the sources themselves (ops): an extract that failed after Airflow's retries, a stale replica,
and a day with far fewer rows than usual (late documentation). While a source's breaker is OPEN every
finding on that source is HELD under the parent incident — no per-dataset action (rule outage_breaker_open).
`wait_and_recheck` changes no data: the incident WAITS and is re-checked on the next run; after the
attempt cap it escalates.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import re
import uuid
from pathlib import Path

from google.cloud import bigquery

from reconcile_ingest.publish import LOOKBACK_DAYS, NON_BLOCKING, STATE_DS, client_as, service_account

INCIDENT_DS = os.environ.get("RECOVERY_DATASET", "recovery")
POLICY_FILE = os.environ.get("POLICY_FILE", "/opt/reconcile/policy/policy.yml")
ENGINE = "engine"
IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# which ingest DAG / source system re-extracts which dataset
SOURCE_DAG = {
    "encounters": "ingest_ehr", "patients": "ingest_ehr", "diagnoses": "ingest_ehr",
    "archive_index": "ingest_ehr",
    "lab_orders": "ingest_lis", "lab_panels": "ingest_lis", "lab_results": "ingest_lis",
    "claims": "ingest_billing", "payments": "ingest_billing", "adjustments": "ingest_billing",
}
# catalog actions the POC executor can actually run. `reextract_from_raw` re-extracts the day from
# the source with the ingest code (the policy describes it as "from the source or archive").
IMPLEMENTED_ACTIONS = {"reextract_from_raw", "rebuild_partition", "remap_merged_patient"}
# rebuild_partition (pkg 3) = re-extract that ONE day and rebuild it (dbt_build for that business date);
# remap_merged_patient (pkg 4) = write approved retired → surviving patient IDs to ops.patient_remaps.
# diagnoses that mean the raw data itself is wrong (a re-extract can help)
RAW_PROBLEMS = {"partial_extract", "missing_load", "offsetting_errors", "amount_mismatch", "missing_receipt"}
OPEN_STATES = {"OPEN", "ACTING", "RECOMMENDED", "ESCALATED", "AWAITING_APPROVAL", "HELD", "WAITING", "EXPECTED"}
# actions that change no data: the engine records the decision and re-checks on the next run
NO_DATA_ACTIONS = {"wait_and_recheck": "wait", "log_expected": "log", "link_unknown_member": "link"}
# log / link → status EXPECTED: recorded, quiet (no alarm, no attempt used); resolved when the reason is gone
BREAKER_RULE = "outage_breaker_open"
BREAKER_OWNED = ("source_outage:", "global_outage:")   # parent incidents: opened/closed by the breaker only
APPROVAL_ROUTES = {"L2"}  # routes where a named person approves before the engine acts

INCIDENT_SCHEMA = [
    bigquery.SchemaField("incident_id", "STRING"),
    bigquery.SchemaField("incident_key", "STRING"),
    bigquery.SchemaField("business_date", "DATE"),
    bigquery.SchemaField("dataset", "STRING"),
    bigquery.SchemaField("failure_type", "STRING"),
    bigquery.SchemaField("status", "STRING"),  # OPEN ACTING WAITING HELD RECOMMENDED AWAITING_APPR. ESCALATED RESOLVED
    bigquery.SchemaField("attempt", "INT64"),
    bigquery.SchemaField("action", "STRING"),
    bigquery.SchemaField("mode", "STRING"),     # auto | approve | recommend | escalate | none
    bigquery.SchemaField("evidence", "STRING"),  # JSON: counts only, never values
    bigquery.SchemaField("gates", "STRING"),     # JSON: the five gates and their answers
    bigquery.SchemaField("note", "STRING"),
    bigquery.SchemaField("gate_run_id", "STRING"),
    bigquery.SchemaField("actor", "STRING"),
    bigquery.SchemaField("at", "TIMESTAMP"),
    # stage 0 (v5.2): every decision is traceable to the policy that made it
    bigquery.SchemaField("policy_version", "STRING"),
    bigquery.SchemaField("rule_id", "STRING"),
    bigquery.SchemaField("severity", "STRING"),
    bigquery.SchemaField("route", "STRING"),
]

# Approval cards: written by the engine only (append-only; the newest row per approval_id is its state).
APPROVAL_SCHEMA = [
    bigquery.SchemaField("approval_id", "STRING"),
    bigquery.SchemaField("incident_id", "STRING"),
    bigquery.SchemaField("incident_key", "STRING"),
    bigquery.SchemaField("business_date", "DATE"),
    bigquery.SchemaField("dataset", "STRING"),
    bigquery.SchemaField("action", "STRING"),
    bigquery.SchemaField("params", "STRING"),        # JSON: what exactly will run
    bigquery.SchemaField("status", "STRING"),        # PENDING | APPROVED | REJECTED | EXPIRED | STALE | CANCELLED
    bigquery.SchemaField("fingerprint", "STRING"),   # hash of incident, action, params, evidence, policy version
    bigquery.SchemaField("policy_version", "STRING"),
    bigquery.SchemaField("rule_id", "STRING"),
    bigquery.SchemaField("severity", "STRING"),
    bigquery.SchemaField("route", "STRING"),
    bigquery.SchemaField("attempt", "INT64"),        # the attempt this approval would start
    bigquery.SchemaField("card", "STRING"),          # JSON: engine-written facts shown to the approver
    bigquery.SchemaField("gate_run_id", "STRING"),
    bigquery.SchemaField("requested_by", "STRING"),
    bigquery.SchemaField("requested_at", "TIMESTAMP"),
    bigquery.SchemaField("expires_at", "TIMESTAMP"),
    bigquery.SchemaField("decided_by", "STRING"),
    bigquery.SchemaField("decision_reason", "STRING"),
    bigquery.SchemaField("note", "STRING"),
    bigquery.SchemaField("at", "TIMESTAMP"),
]
DECISIONS_TABLE = "approval_decisions"   # written by the console (sa-api) only; created by Terraform

# Phase 5 stage 2: one row per agent call — accepted, rejected and timed out alike (T22). Counts and refs only.
AGENT_CALL_SCHEMA = [bigquery.SchemaField(n, t) for n, t in [
    ("call_id", "STRING"), ("incident_id", "STRING"), ("incident_key", "STRING"), ("business_date", "DATE"),
    ("dataset", "STRING"), ("agent_id", "STRING"), ("provider", "STRING"), ("model", "STRING"),
    ("prompt_version", "STRING"), ("fake_mode", "STRING"), ("policy_version", "STRING"), ("gate_run_id", "STRING"),
    ("request_refs", "STRING"), ("output", "STRING"), ("validation", "STRING"), ("reason", "STRING"),
    ("proposed_action", "STRING"), ("confidence", "FLOAT64"), ("outcome", "STRING"), ("latency_ms", "INT64"),
    ("actor", "STRING"), ("at", "TIMESTAMP")]]
UNKNOWN_RULE = "fallback_unknown"        # the policy rule for "no rule explains this" → may consult an agent


def _now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


# ── policy ──────────────────────────────────────────────────────────────────
def load_policy(path: str = POLICY_FILE) -> dict:
    """The single source of rules, never-auto list, attempt cap and kill switch."""
    import yaml
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"policy file not found at {path}: mount ./policy into the container")
    policy = yaml.safe_load(p.read_text())
    for key in ("version", "kill_switch", "actions", "never_auto", "limits", "rules"):
        if key not in policy:
            raise ValueError(f"policy.yml has no '{key}'")
    return policy


def facts_for(kind: str) -> dict:
    """Translate a diagnosis into the facts that policy rules match on (`when:` keys).
    Facts the POC cannot measure are absent, so rules that need them never match by accident."""
    base = {"breaker_open": False, "expected_tests_missing": False, "control_totals_present": True}
    specific = {
        # no hourly receipt yet, so a count gap is treated as one window (recorded in the gate trace)
        "partial_extract": {"failure": "count_gap", "count_matches": False, "gap_within_single_window": True},
        "missing_load": {"failure": "count_gap", "count_matches": False, "gap_within_single_window": True},
        "offsetting_errors": {"failure": "distinct_keys_or_amount_mismatch", "count_matches": True},
        "amount_mismatch": {"failure": "distinct_keys_or_amount_mismatch", "count_matches": True},
        "raw_duplicates": {"failure": "raw_duplicates"},
        "missing_receipt": {"control_totals_present": False},
        "checks_incomplete": {"expected_tests_missing": True},
        "unit_or_range": {"failure": "value_range_or_anomaly"},
        "staging_mismatch": {"failure": "grain_or_model_test", "recent_release_touched_scope": False},
        "build_error": {"failure": "build_error"},
        "check_failed": {"failure": "check_failed"},
        # Phase 6 package 1 — read from the sources (ops), not from dbt
        "extract_failed": {"failure": "extract_failed"},
        "stale_feed": {"failure": "stale_feed"},
        "late_data": {"failure": "low_volume"},
        # Phase 6 package 2 — a value-level check (raw vs staging, critical lab fields) failed
        "value_mismatch": {"failure": "value_level_mismatch"},
        # Phase 6 package 3 — the source changed or archived a day we already loaded
        "archived_history": {"failure": "count_excess", "all_rows_before_archive_cutoff": True},
        "count_excess": {"failure": "count_excess", "all_rows_before_archive_cutoff": False},
        "late_change": {"failure": "changed_after_load"},
        "late_change_old": {"failure": "changed_after_load_beyond_lookback"},
        # Phase 6 package 4 — orphan records
        "young_orphan": {"failure": "relationship", "max_orphan_age_hours": 2},
        "archived_parent": {"failure": "relationship", "parent_archived": True},
        "true_orphan": {"failure": "relationship_unknown_parent"},
        "merged_patient": {"failure": "relationship", "parent_in_mpi_merges": True},
    }
    return {**base, **specific.get(kind, {"failure": kind})}


_MISSING = object()


def _matches(when: dict, facts: dict) -> bool:
    if when == {"any": True}:
        return True
    return all(facts.get(k, _MISSING) == v for k, v in when.items())


def select_rule(policy: dict, facts: dict) -> tuple[str, dict]:
    """Stage 1 safety overrides first, then stage 2 ordinary rules; first match wins."""
    for stage in ("safety_overrides", "ordinary"):
        for rule in policy["rules"].get(stage) or []:
            if _matches(rule.get("when") or {}, facts):
                return stage, rule
    raise ValueError("no rule matched and policy has no fallback")  # check_policy.py prevents this


def _same_evidence(recorded, current) -> bool:
    """An EXPECTED incident is re-recorded only when its counts change (e.g. more young orphans)."""
    try:
        before = json.loads(recorded) if isinstance(recorded, str) else (recorded or {})
    except ValueError:
        return False
    return json.loads(json.dumps(current or {}, default=str)) == before


def gates(finding: dict, policy: dict, checks_ran: bool, attempts: int) -> dict:
    """The five gates, in order, driven by policy.yml. Returns the decision."""
    facts = facts_for(finding["type"])
    if finding.get("breaker"):   # the finding's source (or every source) is in an outage
        facts["breaker_open"] = True
    if finding.get("release"):   # pkg 2: a recent release changed the model this check is about (T14)
        facts.update({"failure": "grain_or_model_test", "recent_release_touched_scope": True})
    stage, rule = select_rule(policy, facts)
    action, route, severity = rule["action"], rule["route"], rule["severity"]
    catalog, never = policy["actions"], set(policy.get("never_auto") or [])
    cap, kill = int(policy["limits"]["attempt_cap"]), bool(policy["kill_switch"])
    window_note = " (no hourly receipt: a count gap is treated as one window)" \
        if facts.get("gap_within_single_window") else ""
    trace = [
        {"gate": 1, "question": "Did the checks run?",
         "answer": "yes" if checks_ran else "no — a check or the test-count assertion did not complete",
         "pass": checks_ran},
        {"gate": 2, "question": "Is the evidence trustworthy?",
         "answer": (f"no — outage breaker open ({finding.get('breaker')}); the source cannot be trusted"
                    if facts["breaker_open"] else
                    "receipt and raw counts read from BigQuery; no breaker open" if facts["control_totals_present"]
                    else "no receipt for the loaded batch — the data cannot be verified"),
         "pass": facts["control_totals_present"] and not facts["breaker_open"]},
        {"gate": 3, "question": "Is the difference expected?",
         "answer": "yes — log as expected" if action == "log_expected" else "no expected-difference rule matched",
         "pass": True},
        {"gate": 4, "question": "Which rule matches?",
         "answer": f"{stage} · {rule['id']} → {action} (route {route}, {severity}){window_note}",
         "pass": route == "auto" or route in APPROVAL_ROUTES},
        {"gate": 5, "question": "Do guardrails allow a fix?", "answer": "", "pass": False},
    ]

    def done(mode, answer, ok=False):
        trace[4]["answer"], trace[4]["pass"] = answer, ok
        return {"trace": trace, "action": action, "mode": mode, "rule_id": rule["id"], "stage": stage,
                "severity": severity, "route": route}

    if rule["id"] == BREAKER_RULE:
        return done("hold", f"held under outage {finding.get('breaker')}: no per-dataset action while the "
                            "source is down; re-evaluated when the breaker closes")
    if not checks_ran and rule["id"] != "checks_did_not_run":
        return done("escalate", "checks did not run — never act on incomplete evidence")
    mode_in_catalog = (catalog.get(action) or {}).get("mode")
    if route in APPROVAL_ROUTES:
        # a person approves; the engine still refuses anything it could never run safely
        if action == "escalate":
            return done("escalate", f"route {route}: a person decides and fixes")
        if action in never or action not in catalog:
            return done("escalate", f"route {route}: {action} is never run by the engine — a person fixes it")
        if action not in IMPLEMENTED_ACTIONS:
            return done("escalate", f"route {route}: {action} is not implemented in the POC executor yet")
        if attempts >= cap:
            return done("escalate", f"attempt limit reached ({attempts} of {cap})")
        if kill:
            return done("recommend", "kill switch ON in policy.yml — recommend only, no approval card")
        return done("approve", f"route {route}: attempt {attempts + 1} of {cap} waits for the data owner's approval")
    if route != "auto":
        return done("escalate", f"route {route}: a person decides and fixes")
    if action in never or mode_in_catalog != "auto":
        return done("escalate", f"{action} is never automatic or needs approval")
    if action in NO_DATA_ACTIONS:
        if NO_DATA_ACTIONS[action] in ("log", "link"):    # expected difference / unknown member: quiet, no attempt
            if kill:
                return done("recommend", "kill switch ON in policy.yml — recommend only")
            return done(NO_DATA_ACTIONS[action], f"{action}: expected, recorded without an alarm; no data change",
                        ok=True)
        if attempts >= cap:
            return done("escalate", f"re-checked {attempts} of {cap} times and still failing — a person looks")
        if kill:
            return done("recommend", "kill switch ON in policy.yml — recommend only")
        return done(NO_DATA_ACTIONS[action], f"{action}: no data change; re-check {attempts + 1} of {cap} "
                                             "on the next run", ok=True)
    if action not in IMPLEMENTED_ACTIONS:
        return done("escalate", f"{action} is not implemented in the POC executor yet")
    if attempts >= cap:
        return done("escalate", f"attempt limit reached ({attempts} of {cap})")
    if kill:
        return done("recommend", "kill switch ON in policy.yml — recommend only")
    return done("auto", f"attempt {attempts + 1} of {cap}; kill switch off; action in catalog", ok=True)


# ── BigQuery helpers ────────────────────────────────────────────────────────
def _q(bq, sql, **params):
    def param(k, v):
        if isinstance(v, dt.date):
            return bigquery.ScalarQueryParameter(k, "DATE", v)
        if isinstance(v, list):
            kind = "STRING" if v and isinstance(v[0], str) else "DATE"
            return bigquery.ArrayQueryParameter(k, kind, v)
        return bigquery.ScalarQueryParameter(k, "STRING", v)
    cfg = bigquery.QueryJobConfig(query_parameters=[param(k, v) for k, v in params.items()])
    return [dict(r) for r in bq.query(sql, job_config=cfg).result()]


def ensure_table(bq) -> None:
    t = bigquery.Table(f"{bq.project}.{INCIDENT_DS}.incidents", schema=INCIDENT_SCHEMA)
    t.time_partitioning = bigquery.TimePartitioning(field="business_date")
    bq.create_table(t, exists_ok=True)
    bq.create_table(bigquery.Table(f"{bq.project}.{INCIDENT_DS}.approvals", schema=APPROVAL_SCHEMA),
                    exists_ok=True)
    bq.create_table(bigquery.Table(f"{bq.project}.{INCIDENT_DS}.agent_calls", schema=AGENT_CALL_SCHEMA),
                    exists_ok=True)


def record(bq, rows: list[dict]) -> None:
    if rows:
        cfg = bigquery.LoadJobConfig(
            schema=INCIDENT_SCHEMA, write_disposition="WRITE_APPEND",
            schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION])  # adds the v5.2 columns
        bq.load_table_from_json(rows, f"{bq.project}.{INCIDENT_DS}.incidents", job_config=cfg).result()


def record_agent_calls(bq, rows: list[dict]) -> None:
    if rows:
        cfg = bigquery.LoadJobConfig(schema=AGENT_CALL_SCHEMA, write_disposition="WRITE_APPEND")
        bq.load_table_from_json(rows, f"{bq.project}.{INCIDENT_DS}.agent_calls", job_config=cfg).result()


def record_approvals(bq, rows: list[dict]) -> None:
    if rows:
        cfg = bigquery.LoadJobConfig(schema=APPROVAL_SCHEMA, write_disposition="WRITE_APPEND")
        bq.load_table_from_json(rows, f"{bq.project}.{INCIDENT_DS}.approvals", job_config=cfg).result()


# ── 1. what did the gate see? ───────────────────────────────────────────────
def latest_failures(bq, bd: dt.date) -> tuple[str | None, list[dict]]:
    rows = _q(bq, f"""
        select * from `{bq.project}.{STATE_DS}.check_results`
        where business_date = @bd
        qualify gate_run_id = first_value(gate_run_id) over (order by recorded_at desc)""", bd=bd)
    if not rows:
        return None, []
    failures = [r for r in rows if r["status"] in ("fail", "error")
                and (r["policy_outcome"] or "blocking") not in NON_BLOCKING]
    return rows[0]["gate_run_id"], failures


# ── 2. diagnosis: the batch now in raw vs its own receipt, per day (counts only) ─
def classify(receipt: dict | None, raw: dict) -> str:
    if receipt is None:
        return "missing_receipt" if raw["rows"] else "no_data"
    if raw["rows"] == 0:
        return "missing_load"
    if raw["rows"] < receipt["row_count"]:
        return "partial_extract"
    if raw["keys"] < receipt["distinct_keys"]:
        return "offsetting_errors"
    if receipt.get("amount_total") is not None and raw.get("amount") is not None \
            and abs(float(receipt["amount_total"]) - float(raw["amount"])) > 0.005:
        return "amount_mismatch"
    if raw["rows"] > receipt["row_count"]:
        return "raw_duplicates"
    return "raw_matches_receipt"


def diagnose(bq, dataset: str, days: list[dt.date]) -> list[dict]:
    """Anchor on the batch that is actually in raw, and compare it with THAT batch's receipt.
    (Comparing with the latest receipt instead would hide a missing receipt behind an older one.)"""
    p = bq.project
    cols = _q(bq, f"""select any_value(key_column) key_column, any_value(amount_column) amount_column
                      from `{p}.ops.control_totals` where dataset = @ds""", ds=dataset)[0]
    key, amt = cols["key_column"], cols["amount_column"]
    if not key or not IDENT.match(key) or (amt and not IDENT.match(amt)):
        raise ValueError(f"unexpected column names in ops.control_totals for {dataset}")
    out = []
    for d in days:
        got = _q(bq, f"""select max(_batch_id) batch_id, count(*) n_rows, count(distinct {key}) n_keys
                         {', sum(' + amt + ') amount' if amt else ''}
                         from `{p}.raw.{dataset}` where business_date = @d
                         and _batch_id = (select max(_batch_id) from `{p}.raw.{dataset}` where business_date = @d)""",
                 d=d)[0]  # `rows` is a reserved word in BigQuery
        raw = {"rows": got["n_rows"], "keys": got["n_keys"], "amount": got.get("amount")}
        if got["batch_id"]:
            rec = _q(bq, f"""select batch_id, row_count, distinct_keys, amount_total from `{p}.ops.control_totals`
                             where dataset = @ds and business_date = @d and batch_id = @b
                             qualify row_number() over (order by extracted_at desc) = 1""",
                     ds=dataset, d=d, b=got["batch_id"])
        else:  # nothing loaded: is there a receipt for the day at all?
            rec = _q(bq, f"""select batch_id, row_count, distinct_keys, amount_total from `{p}.ops.control_totals`
                             where dataset = @ds and business_date = @d
                             qualify row_number() over (order by extracted_at desc) = 1""", ds=dataset, d=d)
        r = rec[0] if rec else None
        out.append({
            "business_date": str(d), "diagnosis": classify(r, raw),
            "batch_id": got["batch_id"] or (r and r["batch_id"]),
            "receipt_rows": r and r["row_count"], "raw_rows": raw["rows"],
            "receipt_keys": r and r["distinct_keys"], "raw_keys": raw["keys"],
            "missing_rows": (r["row_count"] - raw["rows"]) if r else None,
            "missing_keys": (r["distinct_keys"] - raw["keys"]) if r else None,
        })
    return out


# ── 3. incidents ────────────────────────────────────────────────────────────
def open_incidents(bq, days: list[dt.date]) -> dict[str, dict]:
    rows = _q(bq, f"""
        select * from `{bq.project}.{INCIDENT_DS}.incidents`
        where business_date in unnest(@days)
        qualify row_number() over (partition by incident_id order by `at` desc) = 1""", days=days)
    return {r["incident_key"]: r for r in rows if r["status"] in OPEN_STATES}


def attempts_so_far(bq, incident_id: str) -> int:
    rows = _q(bq, f"""select max(attempt) a from `{bq.project}.{INCIDENT_DS}.incidents`
                      where incident_id = @i and status in ('ACTING', 'WAITING')""", i=incident_id)
    return int(rows[0]["a"] or 0)


def _dataset_of(failure: dict) -> str | None:
    for name in (failure.get("attached_to") or "").split(","):
        if name.startswith("stg_"):
            return name[len("stg_"):]
    return None


def findings(bq, failures: list[dict], days: list[dt.date]) -> list[dict]:
    """Turn failing checks into findings: (key, dataset, date, failure_type, evidence)."""
    out, receipt_datasets = [], set()
    for f in failures:
        parts = f["unique_id"].split(".")
        name = parts[2] if len(parts) > 2 else f["name"]  # generic tests end in a hash
        models = [m for m in (f.get("attached_to") or "").split(",") if m]
        if name.startswith("value_recon_") or any(m.startswith("value_recon_") for m in models):
            vm = next((m for m in models if m.startswith("value_recon_")), "")
            ds = vm[len("value_recon_"):] or None
            if name.startswith("value_recon_") and ds:   # the value-level check itself failed (T6, G4)
                out.append({"key": f"value_mismatch:{ds}:{days[-1]}", "dataset": ds, "date": days[-1],
                            "type": "value_mismatch", "models": models,
                            "evidence": {"check": name, "failures": f["failures"]}})
                continue
        if f["resource_type"] == "operation":
            out.append({"key": f"checks_incomplete:{days[-1]}", "dataset": None, "date": days[-1],
                        "type": "checks_incomplete", "evidence": {"check": name, "status": f["status"]}})
        elif name.startswith(("reconcile_to_control_totals", "control_totals_present")) and _dataset_of(f):
            receipt_datasets.add(_dataset_of(f))
        elif f["resource_type"] == "model":
            out.append({"key": f"build_error:{name}:{days[-1]}", "dataset": None, "date": days[-1],
                        "type": "build_error", "models": [name], "evidence": {"model": name}})
        else:
            kind = "unit_or_range" if "units_and_ranges" in name else "check_failed"
            out.append({"key": f"{kind}:{name}:{days[-1]}", "dataset": _dataset_of(f), "date": days[-1],
                        "type": kind, "models": models, "evidence": {"check": name, "failures": f["failures"]}})
    for ds in sorted(receipt_datasets):
        per_day = diagnose(bq, ds, days)
        bad = [d for d in per_day if d["diagnosis"] in RAW_PROBLEMS]
        if not bad:  # raw matches its receipt but staging does not → logic problem, not data
            out.append({"key": f"staging_mismatch:{ds}:{days[-1]}", "dataset": ds, "date": days[-1],
                        "type": "staging_mismatch", "evidence": {"days": per_day}})
        for d in bad:
            day = dt.date.fromisoformat(d["business_date"])
            out.append({"key": f"{d['diagnosis']}:{ds}:{day}", "dataset": ds, "date": day,
                        "type": d["diagnosis"], "evidence": d})
    return out


def source_findings(bq, bd: dt.date, policy: dict) -> list[dict]:
    """Phase 6: problems seen in the sources themselves (ops), which dbt cannot see."""
    from reconcile_ingest import source_state as ss
    out = []
    since = dt.datetime.combine(bd - dt.timedelta(days=LOOKBACK_DAYS), dt.time(), tzinfo=dt.timezone.utc)
    for ds, b in ss.latest_batches(bq, since).items():
        if b["status"] == "FAILED" and b["business_date"] == bd:
            out.append({"key": f"extract_failed:{ds}:{bd}", "dataset": ds, "date": bd, "type": "extract_failed",
                        "evidence": {"batch_mode": b["mode"], "started_at": str(b["started_at"]),
                                     "error": (b.get("error") or "")[:200]}})
    for src, why in ss.stale_sources(bq, policy).items():
        out.append({"key": f"stale_feed:{src}:{bd}", "dataset": None, "source": src, "date": bd,
                    "type": "stale_feed", "evidence": {"source_system": src, "why": why}})
    for ds, v in ss.volume_report(bq, bd, policy).items():
        if v["low"]:
            out.append({"key": f"late_data:{ds}:{bd}", "dataset": ds, "date": bd, "type": "late_data",
                        "evidence": {k: v[k] for k in ("rows", "median", "share", "baseline_days")}})
    from reconcile_ingest import source_checks  # packages 3 and 4: drift, orphans, merged patients
    return out + source_checks.run(bq, bd, policy, LOOKBACK_DAYS)


def recent_releases(bq, policy: dict) -> list[dict]:
    """Phase 6 pkg 2: releases inside policy releases.window_days (empty when none or not set up yet)."""
    try:
        from reconcile_ingest import releases
        return releases.recent(bq, policy)
    except Exception as exc:  # noqa: BLE001 — no release context means: decide as before (no regression flag)
        print(f"  (release context unavailable: {type(exc).__name__})")
        return []


def check_breakers(policy: dict) -> dict[str, dict]:
    """Run the outage breaker for every source; returns the breakers that are OPEN (incl. GLOBAL)."""
    from reconcile_ingest import breaker
    return breaker.check(policy)["open"]


def mark_breakers(found: list[dict], open_breakers: dict[str, dict]) -> None:
    """Attach the parent incident of an open breaker to every finding on that source (or all, if GLOBAL)."""
    from reconcile_ingest import source_state as ss
    glob = open_breakers.get(ss.GLOBAL)
    for f in found:
        src = f.get("source") or ss.source_of(f.get("dataset"))
        b = glob or open_breakers.get(src or "")
        if b:
            f["breaker"] = b.get("parent_incident_id") or b["source_system"]


# ── 4. evaluate: one deterministic pass ─────────────────────────────────────
def evaluate(business_date, dry_run: bool = False, policy: dict | None = None,
             fake_agent_mode: str | None = None, agent_provider: str | None = None) -> list[dict]:
    """Returns the list of re-extract actions to run (empty when nothing to do).
    fake_agent_mode (test project only) lets unknown incidents be sent to the fake Diagnosis agent."""
    policy = policy or load_policy()
    bd = dt.date.fromisoformat(str(business_date))
    days = [bd - dt.timedelta(days=i) for i in range(LOOKBACK_DAYS - 1, -1, -1)]
    bq = client_as(ENGINE)
    if not dry_run:
        ensure_table(bq)
    open_breakers = {} if dry_run else check_breakers(policy)   # Phase 6: open/close breakers first
    gate_run, failures = latest_failures(bq, bd)
    print(f"Recovery engine · business_date {bd} · gate run {gate_run} · {len(failures)} blocking failure(s) "
          f"· policy {policy['version']} · kill switch {'ON' if policy['kill_switch'] else 'off'}")
    current = findings(bq, failures, days) + source_findings(bq, bd, policy)
    mark_breakers(current, open_breakers)
    from reconcile_ingest import releases
    releases.mark(current, [] if dry_run else recent_releases(bq, policy))
    current.sort(key=lambda f: (str(f["date"]), f["key"]))   # oldest first (children after an outage)
    checks_ran = not any(f["type"] == "checks_incomplete" for f in current)
    from reconcile_ingest.source_checks import DRIFT_TYPES, detectors
    drift_days = int(detectors(policy)["drift"]["window_days"])
    all_days = [bd - dt.timedelta(days=i) for i in range(max(LOOKBACK_DAYS, drift_days + 1) - 1, -1, -1)]
    existing = open_incidents(bq, all_days) if not dry_run else {}
    actor, rows, actions, cards, calls = service_account(ENGINE), [], [], [], []

    def row(inc_id, f, status, attempt, decision, note):
        return {"incident_id": inc_id, "incident_key": f["key"], "business_date": str(f["date"]),
                "dataset": f["dataset"], "failure_type": f["type"], "status": status, "attempt": attempt,
                "action": decision.get("action", "none"), "mode": decision.get("mode", "none"),
                "evidence": json.dumps(f["evidence"], default=str),
                "gates": json.dumps(decision["trace"]) if decision.get("trace") else None, "note": note,
                "gate_run_id": gate_run, "actor": actor, "at": _now(), "policy_version": policy["version"],
                "rule_id": decision.get("rule_id"), "severity": decision.get("severity"),
                "route": decision.get("route")}

    # A fix is already in progress on exactly this evidence → do nothing until the rebuild has been
    # checked by the gate. This is what makes duplicate events harmless.
    in_flight = [i for i in existing.values() if i["status"] == "ACTING" and i["gate_run_id"] == gate_run]
    if in_flight:
        for inc in in_flight:
            print(f"  WAITING   {inc['incident_id']}  {inc['incident_key']} "
                  f"(already acted on this evidence; waiting for the rebuild to be verified)")
        return []

    # close incidents whose problem is gone — only on NEWER gate evidence than the one acted on
    current_keys = {f["key"] for f in current}
    for key, inc in existing.items():
        if key.startswith(BREAKER_OWNED):   # the breaker opens and closes its own parent incidents
            continue
        in_scope = str(inc["business_date"]) in {str(d) for d in days} or inc["failure_type"] in DRIFT_TYPES
        if in_scope and key not in current_keys and inc["gate_run_id"] != gate_run:
            f = {"key": key, "date": inc["business_date"], "dataset": inc["dataset"],
                 "type": inc["failure_type"], "evidence": {"verified_by_gate_run": gate_run}}
            rows.append(row(inc["incident_id"], f, "RESOLVED", inc["attempt"],
                            {"rule_id": inc.get("rule_id"), "severity": inc.get("severity"), "route": inc.get("route")},
                            "re-verified: the failing check now passes"))
            print(f"  RESOLVED  {inc['incident_id']}  {key}")

    for f in current:
        inc = existing.get(f["key"])
        inc_id = inc["incident_id"] if inc else f"INC-{f['date']:%Y%m%d}-{uuid.uuid4().hex[:6]}"
        attempts = attempts_so_far(bq, inc_id) if inc and not dry_run else 0
        d = gates(f, policy, checks_ran, attempts)
        agent = None
        if d["rule_id"] == UNKNOWN_RULE and checks_ran and not (
                inc and inc["status"] in ("ESCALATED", "AWAITING_APPROVAL", "RECOMMENDED")):
            from reconcile_ingest import agent_gateway
            provider, why = agent_gateway.agents_allowed(policy, fake_agent_mode, agent_provider)  # A1
            if provider:
                ev = f["evidence"] if isinstance(f["evidence"], dict) else {}
                names = [f["type"]] + [str(ev[k]) for k in ("check", "model") if ev.get(k)]
                agent = agent_gateway.consult(
                    incident={"id": inc_id, "key": f["key"], "type": f["type"], "dataset": f["dataset"],
                              "business_date": str(f["date"])},
                    evidence=ev, check_names=names, policy=policy, provider=provider, mode=fake_agent_mode,
                    attempts=attempts, implemented=IMPLEMENTED_ACTIONS, actor=actor, gate_run=gate_run)
                d = agent["decision"]
                calls.append(agent["call"])
            else:
                d["trace"][4]["answer"] += f" · {why}"
        if d["mode"] == "auto":
            status, attempt = "ACTING", attempts + 1
            note = f"{'rebuild' if d['action'] == 'rebuild_partition' else 're-extract'} {f['dataset']} {f['date']}"
            actions.append({"incident_id": inc_id, "incident_key": f["key"], "dataset": f["dataset"],
                            "business_date": str(f["date"]), "dag_id": SOURCE_DAG[f["dataset"]],
                            "attempt": attempt, "gate_run_id": gate_run, "action": d["action"],
                            "rule_id": d["rule_id"], "severity": d["severity"], "route": d["route"],
                            "policy_version": policy["version"]})
        elif d["mode"] == "approve":
            if inc and inc["status"] in ("AWAITING_APPROVAL", "ESCALATED"):
                print(f"  {inc['status']} {inc_id}  {f['key']} (waiting for a person; no new card)")
                continue
            card = approval_request(bq, inc_id, f, d, attempts + 1, gate_run, policy, actor, agent=agent,
                                    comms=agent_provider, calls=calls)
            cards.append(card)
            status, attempt = "AWAITING_APPROVAL", attempts
            note = (f"approval {card['approval_id']} sent to the data owner "
                    f"(expires {card['expires_at'][:16].replace('T', ' ')} UTC)")
        elif d["mode"] == "hold":
            status, attempt, note = "HELD", attempts, f"held under outage {f.get('breaker')}; no action"
            if inc and inc["status"] == "HELD":
                print(f"  HELD      {inc_id}  {f['key']} (already held under the outage)")
                continue
        elif d["mode"] in ("log", "link"):
            if inc and inc["status"] == "EXPECTED":
                if _same_evidence(inc.get("evidence"), f.get("evidence")):
                    print(f"  EXPECTED  {inc_id}  {f['key']} (already recorded)")
                    continue
                print(f"  EXPECTED  {inc_id}  {f['key']} (counts changed: recording the new picture)")
            status, attempt = "EXPECTED", attempts
            note = (f"{d['action']}: expected difference recorded; no alarm, no data change" if d["mode"] == "log"
                    else "linked to the unknown member; re-linked automatically when the parent arrives")
        elif d["mode"] == "wait":
            if inc and inc["status"] == "WAITING" and inc["gate_run_id"] == gate_run:
                print(f"  WAITING   {inc_id}  {f['key']} (already decided on this evidence)")
                continue
            status, attempt = "WAITING", attempts + 1
            note = f"{d['action']}: no data change; hold and re-check on the next run ({attempt} of " \
                   f"{policy['limits']['attempt_cap']})"
        elif d["mode"] == "recommend":
            status, attempt, note = "RECOMMENDED", attempts, f"would run {d['action']}; kill switch is on"
        else:
            status, attempt, note = "ESCALATED", attempts, f"needs a person ({d['route']}): {d['trace'][4]['answer']}"
            if inc and inc["status"] == "ESCALATED":
                print(f"  ESCALATED {inc_id}  {f['key']} (already escalated)")
                continue
        rows.append(row(inc_id, f, status, attempt, d, note))
        print(f"  {status:9} {inc_id}  {f['key']}  → {d['rule_id']} · {d['action']} ({d['mode']}, {d['severity']}, "
              f"route {d['route']})  evidence={json.dumps(f['evidence'], default=str)[:140]}")

    if not current and not existing:
        print("  nothing to do — every check passed")
    if not dry_run:
        record_agent_calls(bq, calls)  # provenance first: every call is on record, whatever happened next
        record_approvals(bq, cards)    # the card exists before the incident points at it
        record(bq, rows)
        if cards:                      # Phase 5 stage 4: show new cards in Slack now (worker re-syncs anyway)
            try:
                from reconcile_ingest import slack_notify
                slack_notify.sync(policy)
            except Exception as exc:  # noqa: BLE001 — Slack never blocks the engine
                print(f"  (Slack sync skipped: {type(exc).__name__})")
    return actions


# ── 5. act: re-extract from the source, then confirm raw matches the receipt ─
def _ingest_components():
    """The same building blocks the ingest DAGs use; loads run as sa-ingest, not sa-engine."""
    from reconcile_ingest import gcp
    from reconcile_ingest.archive import GcsArchive
    from reconcile_ingest.settings import Settings
    from reconcile_ingest.warehouse import BigQueryWarehouse
    s = Settings.from_env()
    s.require_gcp()
    wh = BigQueryWarehouse(gcp.bigquery_client(s.project, s.impersonate, s.location), s.project,
                           s.max_bytes_billed)
    return s, wh, GcsArchive(gcp.storage_client(s.project, s.impersonate), s.archive_bucket)


def reextract(actions: list[dict]) -> list[dict]:
    from reconcile_ingest import pipeline

    bq = client_as(ENGINE)
    s, wh, ar = _ingest_components()
    results = []
    for a in actions:
        day = dt.date.fromisoformat(a["business_date"])
        print(f"→ {a['incident_id']}: {a['action']} — re-extract {a['dataset']} {day} from the source "
              f"(same code as {a['dag_id']}, no injectors)")
        error, loaded, remap_note = None, {}, None
        try:
            if a["action"] == "remap_merged_patient":   # pkg 4: approved mapping only — raw is never changed
                from reconcile_ingest import source_checks
                _, remap_note = source_checks.apply_remaps(bq, a["dataset"], day, a.get("approval_id"),
                                                           LOOKBACK_DAYS)
            else:
                res = pipeline.ingest_dataset(s, wh, ar, a["dataset"], day, [],
                                              run_id=f"recovery__{a['incident_id']}__a{a['attempt']}")
                loaded = getattr(res, "as_dict", lambda: {})()
        except Exception as exc:  # noqa: BLE001 — recorded in the journal, then re-raised below
            error = str(exc).splitlines()[0][:300]
        after = diagnose(bq, a["dataset"], [day])[0]
        ok = error is None and (remap_note is not None or after["diagnosis"] not in RAW_PROBLEMS)
        note = (f"{a['action']} failed: {error}" if error else remap_note or
                f"re-extract done (batch {loaded.get('batch_id', '?')}); raw now {after['raw_rows']} rows "
                f"vs receipt {after['receipt_rows']} → {after['diagnosis']}")
        record(bq, [{
            "incident_id": a["incident_id"], "incident_key": a["incident_key"],
            "business_date": a["business_date"], "dataset": a["dataset"],
            "failure_type": a["incident_key"].split(":")[0], "status": "ACTING",
            "attempt": a["attempt"], "action": a["action"], "mode": a.get("mode", "auto"),
            "evidence": json.dumps(after, default=str), "gates": None, "note": note,
            "gate_run_id": a.get("gate_run_id"), "actor": service_account(ENGINE), "at": _now(),
            "policy_version": a.get("policy_version"), "rule_id": a.get("rule_id"),
            "severity": a.get("severity"), "route": a.get("route")}])
        print(("✔ " if ok else "✘ ") + note)
        results.append({**a, "ok": ok, "after": after})
    if not all(r["ok"] for r in results):
        raise RuntimeError("re-extract did not restore the data; the incident stays open")
    return results


# ── 6. approvals (Phase 5 stage 1): engine-written cards, a person decides ──
WHAT_HAPPENED = {
    "missing_receipt": "The batch now in raw has no source receipt (control totals), so nobody can tell whether "
                       "it is complete. Re-extracting the day from the source writes a new batch together with "
                       "its receipt, and dbt then reconciles it.",
    "partial_extract": "Raw holds fewer rows than the source receipt says were extracted.",
    "missing_load": "The source receipt exists but nothing landed in raw for the day.",
    "offsetting_errors": "Row count matches the receipt but distinct keys do not.",
    "amount_mismatch": "Row count matches the receipt but the amount total does not.",
    "merged_patient": "Some rows use a patient ID that the master patient index (MPI) has retired — merged into "
                      "another ID. Approving maps the retired IDs to their surviving IDs in staging; the raw data "
                      "is not changed and the mapping can be removed again.",
}
CARD_TEXT = {   # title and "if approved" per action (the rest of the card is the same for every action)
    "reextract_from_raw": ("Re-extract {ds} for {day} from the source",
                           "The engine re-checks the facts, re-extracts {ds} {day} with the normal ingest code (as "
                           "sa-ingest), then dbt_build rebuilds, re-tests and the gate decides."),
    "remap_merged_patient": ("Remap merged patient IDs in {ds} for {day}",
                             "The engine re-checks the facts, writes the retired → surviving ID pairs to "
                             "ops.patient_remaps, then dbt_build rebuilds staging with the surviving IDs, re-tests "
                             "and the gate decides."),
}
EVIDENCE_KEYS = ("batch_id", "diagnosis", "raw_rows", "receipt_rows", "raw_keys", "receipt_keys",
                 "missing_rows", "missing_keys")


def fingerprint(incident_id: str, action: str, params: dict, evidence: dict, policy_version: str) -> str:
    """What the approver agrees to. Any change to these facts makes an old approval useless."""
    blob = json.dumps({"incident_id": incident_id, "action": action, "params": params,
                       "evidence": {k: evidence.get(k) for k in EVIDENCE_KEYS},
                       "policy_version": policy_version}, sort_keys=True, default=str)
    return hashlib.sha256(blob.encode()).hexdigest()[:16]


def _held_marts(bq, day) -> list[str]:
    try:
        rows = _q(bq, f"""select dataset, state from `{bq.project}.{STATE_DS}.publication_state`
                          where business_date = @d
                          qualify row_number() over (partition by dataset order by decided_at desc) = 1""",
                  d=dt.date.fromisoformat(str(day)))
    except Exception:  # noqa: BLE001 — the card still shows the evidence without the impact line
        return []
    return sorted(r["dataset"] for r in rows if r["state"] in ("HELD", "ROLLED_BACK"))


def approval_request(bq, incident_id: str, f: dict, d: dict, attempt: int, gate_run: str | None,
                     policy: dict, actor: str, agent: dict | None = None, comms: str | None = None,
                     calls: list | None = None) -> dict:
    """Build the approval card. Every fact on it comes from the engine, never from the requester.
    comms (a real agent provider, when allowed): the Communication agent adds a plain-English summary written
    only from the card's facts; it never changes a fact and never affects the fingerprint."""
    cap = int(policy["limits"]["attempt_cap"])
    minutes = int((policy.get("approvals") or {}).get("expiry_minutes", 120))
    now = dt.datetime.now(dt.timezone.utc)
    params = {"dataset": f["dataset"], "business_date": str(f["date"]), "dag_id": SOURCE_DAG[f["dataset"]]}
    ev = f["evidence"] if isinstance(f["evidence"], dict) else {}
    if not all(k in ev for k in ("batch_id", "raw_rows")):
        # not a receipt diagnosis (e.g. an agent proposal for a check failure): fingerprint the raw batch
        # vs its receipt, exactly what recheck() re-reads before acting
        ev = diagnose(bq, f["dataset"], [dt.date.fromisoformat(str(f["date"]))])[0]
    held = _held_marts(bq, f["date"])
    extra = {k: v for k, v in (f["evidence"] or {}).items() if k in ("rows_on_retired_ids", "retired_ids")} \
        if isinstance(f.get("evidence"), dict) else {}
    title, approved = CARD_TEXT.get(d["action"], CARD_TEXT["reextract_from_raw"])
    card = {
        "title": title.format(ds=f["dataset"], day=f["date"]),
        "what_happened": WHAT_HAPPENED.get(f["type"], f"Diagnosis: {f['type']}"),
        "evidence": {**{k: ev.get(k) for k in EVIDENCE_KEYS}, **extra},
        "rule": f"{d['stage']} · {d['rule_id']} → {d['action']} · route {d['route']} · {d['severity']}",
        "impact": (f"Publication for {f['date']} is held for: {', '.join(held)}." if held else
                   f"Publication for {f['date']} stays held until the data is verified."),
        "if_approved": approved.format(ds=f["dataset"], day=f["date"]),
        "if_rejected": "Nothing runs. The incident is escalated; publication stays held until a person fixes it.",
        "guardrails": [f"attempt {attempt} of {cap}",
                       f"expires after {minutes} minutes if nobody answers",
                       f"policy {policy['version']}; a policy change voids this card",
                       "evidence is re-read before acting; any change voids this card",
                       "kill switch is re-checked before acting"],
    }
    if agent:
        p = agent["proposal"]
        card["what_happened"] = (f"No rule explains this {f['type']} incident. The Diagnosis agent "
                                 f"({agent['call']['provider']}, {agent['call']['prompt_version']}) proposes this "
                                 f"action with confidence {p['confidence']:.2f}. Facts it cited: "
                                 + " ".join(p["facts"]))[:900]
        card["rule"] = (f"agent proposal · call {agent['call']['call_id']} → {d['action']} · route L2 · "
                        f"{d['severity']} (agent proposals always need a data owner)")
        card["guardrails"].insert(0, "proposed by an AI agent: validated by the engine (catalog, scope, "
                                     "evidence refs, confidence); it cannot act")
    if comms:
        add_summary(card, policy, comms, {"id": incident_id, "key": f["key"], "business_date": str(f["date"]),
                                          "dataset": f["dataset"]}, actor, gate_run, calls)
    fp = fingerprint(incident_id, d["action"], params, ev, policy["version"])
    return {
        "approval_id": f"APR-{f['date']:%Y%m%d}-{uuid.uuid4().hex[:6]}", "incident_id": incident_id,
        "incident_key": f["key"], "business_date": str(f["date"]), "dataset": f["dataset"],
        "action": d["action"], "params": json.dumps(params), "status": "PENDING", "fingerprint": fp,
        "policy_version": policy["version"], "rule_id": d["rule_id"], "severity": d["severity"],
        "route": d["route"], "attempt": attempt, "card": json.dumps(card, default=str), "gate_run_id": gate_run,
        "requested_by": actor, "requested_at": now.isoformat(),
        "expires_at": (now + dt.timedelta(minutes=minutes)).isoformat(),
        "decided_by": None, "decision_reason": None, "note": "waiting for the data owner", "at": now.isoformat(),
    }


def add_summary(card: dict, policy: dict, requested: str, incident: dict, actor: str, gate_run: str | None,
                calls: list | None) -> None:
    """Communication agent (Phase 5): wording only. Any failure leaves the card exactly as the engine wrote it."""
    from reconcile_ingest import agent_gateway as ag
    provider, _ = ag.agents_allowed(policy, None, requested)
    if provider not in ag.REAL_PROVIDERS:
        return
    try:
        summary, call = ag.communicate(card, policy, provider, incident, actor, gate_run)
    except Exception as exc:  # noqa: BLE001 — a card never depends on the summary
        print(f"  (no card summary: {type(exc).__name__})")
        return
    if calls is not None:
        calls.append(call)
    if summary:
        card["summary"] = summary
        card["summary_by"] = (f"Communication agent ({call['model']}, {call['prompt_version']}) — wording only; "
                              "every fact above comes from the engine")


def _latest_approvals(bq) -> list[dict]:
    from google.api_core.exceptions import NotFound
    try:
        return _q(bq, f"""select * from `{bq.project}.{INCIDENT_DS}.approvals`
                          qualify row_number() over (partition by approval_id order by `at` desc) = 1
                          order by requested_at desc""")
    except NotFound:
        return []


def _decisions(bq, ids: list[str]) -> dict[str, dict]:
    """First decision per approval wins (the console refuses a second one anyway)."""
    from google.api_core.exceptions import NotFound
    if not ids:
        return {}
    try:
        rows = _q(bq, f"""select * from `{bq.project}.{INCIDENT_DS}.{DECISIONS_TABLE}`
                          where approval_id in unnest(@ids)
                          qualify row_number() over (partition by approval_id order by decided_at) = 1""",
                  ids=ids)
    except NotFound:  # Terraform has not created the table yet → nobody can have decided
        return {}
    return {r["approval_id"]: r for r in rows}


def _latest_incident(bq, incident_id: str) -> dict | None:
    rows = _q(bq, f"""select * from `{bq.project}.{INCIDENT_DS}.incidents` where incident_id = @i
                      qualify row_number() over (order by `at` desc) = 1""", i=incident_id)
    return rows[0] if rows else None


def recheck(bq, apr: dict, dec: dict, policy: dict) -> list[str]:
    """A3: everything that must still be true at the moment of acting. Empty list = safe to act."""
    problems = []
    who = (dec.get("decided_by") or "").strip().lower()
    if not who or who.endswith(".gserviceaccount.com") or who == (apr.get("requested_by") or "").lower():
        problems.append("approver must be a named person, not the requester, a service account or an agent")
    if dec["decided_at"] > apr["expires_at"]:
        problems.append("decision arrived after the card expired")
    if dec.get("fingerprint") != apr["fingerprint"]:
        problems.append("the approver saw a different card (fingerprint mismatch)")
    if policy["version"] != apr["policy_version"]:
        problems.append(f"policy changed since the card was written ({apr['policy_version']} → {policy['version']})")
    if policy["kill_switch"]:
        problems.append("kill switch is ON in policy.yml")
    attempts = attempts_so_far(bq, apr["incident_id"])
    cap = int(policy["limits"]["attempt_cap"])
    if attempts >= cap:
        problems.append(f"attempt limit reached ({attempts} of {cap})")
    kind = apr["incident_key"].split(":")[0]
    if not str(apr.get("rule_id") or "").startswith("agent:"):   # agent proposals are not routed by a rule
        d = gates({"type": kind}, policy, checks_ran=True, attempts=attempts)
        if d["mode"] not in ("approve", "recommend") or d["action"] != apr["action"]:
            problems.append(f"the policy no longer routes {kind} to an approved {apr['action']}")
    elif apr["action"] not in IMPLEMENTED_ACTIONS:
        problems.append(f"{apr['action']} is not implemented in the executor")
    fresh = diagnose(bq, apr["dataset"], [dt.date.fromisoformat(str(apr["business_date"]))])[0]
    params = json.loads(apr["params"])
    if fingerprint(apr["incident_id"], apr["action"], params, fresh, apr["policy_version"]) != apr["fingerprint"]:
        problems.append(f"the evidence changed since the card was written (now: {fresh['diagnosis']}, "
                        f"batch {fresh['batch_id']})")
    return problems


def process_approvals(pretend_later_minutes: int = 0, policy: dict | None = None) -> list[dict]:
    """Expire, reject, void or execute approval cards. Returns re-extract actions for approved cards."""
    policy = policy or load_policy()
    bq = client_as(ENGINE)
    ensure_table(bq)
    now = dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=pretend_later_minutes)
    pending = [a for a in _latest_approvals(bq) if a["status"] == "PENDING"]
    print(f"Approval worker · {len(pending)} pending card(s) · policy {policy['version']}"
          + (f" · pretending it is {pretend_later_minutes} min later (expiry only)" if pretend_later_minutes else ""))
    decisions = _decisions(bq, [a["approval_id"] for a in pending])
    actor, cards, incs, actions = service_account(ENGINE), [], [], []

    def card_row(a, status, note, dec=None):
        return {**{k: a[k] for k in (f.name for f in APPROVAL_SCHEMA) if k in a},
                "business_date": str(a["business_date"]), "requested_at": a["requested_at"].isoformat(),
                "expires_at": a["expires_at"].isoformat(), "status": status, "note": note, "at": _now(),
                "decided_by": dec and dec.get("decided_by"), "decision_reason": dec and dec.get("reason")}

    def inc_row(a, inc, status, attempt, mode, note):
        return {"incident_id": a["incident_id"], "incident_key": a["incident_key"],
                "business_date": str(a["business_date"]), "dataset": a["dataset"],
                "failure_type": a["incident_key"].split(":")[0], "status": status, "attempt": attempt,
                "action": a["action"], "mode": mode, "evidence": inc.get("evidence"), "gates": None,
                "note": note, "gate_run_id": a["gate_run_id"], "actor": actor, "at": _now(),
                "policy_version": a["policy_version"], "rule_id": a["rule_id"], "severity": a["severity"],
                "route": a["route"]}

    for a in pending:
        inc = _latest_incident(bq, a["incident_id"]) or {}
        dec = decisions.get(a["approval_id"])
        aid = a["approval_id"]
        if inc.get("status") != "AWAITING_APPROVAL":
            cards.append(card_row(a, "CANCELLED", f"incident is now {inc.get('status') or 'missing'}; card closed"))
            print(f"  CANCELLED {aid}  incident {a['incident_id']} is {inc.get('status')}")
        elif dec is None:
            if now >= a["expires_at"]:
                cards.append(card_row(a, "EXPIRED", "nobody answered in time"))
                incs.append(inc_row(a, inc, "ESCALATED", inc.get("attempt") or 0, "escalate",
                                    f"approval {aid} expired unanswered; publication stays held"))
                print(f"  EXPIRED   {aid}  → incident {a['incident_id']} ESCALATED")
            else:
                left = round((a["expires_at"] - now).total_seconds() / 60)
                print(f"  PENDING   {aid}  {a['incident_key']}  ({left} min left)")
        elif dec["decision"] == "REJECT":
            cards.append(card_row(a, "REJECTED", "rejected by the data owner", dec))
            incs.append(inc_row(a, inc, "ESCALATED", inc.get("attempt") or 0, "escalate",
                                f"rejected by {dec['decided_by']}: {dec.get('reason') or '-'}"))
            print(f"  REJECTED  {aid}  by {dec['decided_by']} → incident {a['incident_id']} ESCALATED")
        else:
            problems = recheck(bq, a, dec, policy)
            if problems:
                cards.append(card_row(a, "STALE", "approved, but re-check failed: " + "; ".join(problems), dec))
                incs.append(inc_row(a, inc, "ESCALATED", inc.get("attempt") or 0, "escalate",
                                    f"approval {aid} not executed: " + "; ".join(problems)))
                print(f"  STALE     {aid}  " + "; ".join(problems))
                continue
            p = json.loads(a["params"])
            cards.append(card_row(a, "APPROVED", "re-check passed; executing", dec))
            incs.append(inc_row(a, inc, "ACTING", a["attempt"], "approve",
                                f"approved by {dec['decided_by']} ({aid}); re-extract {p['dataset']} "
                                f"{p['business_date']}"))
            actions.append({"incident_id": a["incident_id"], "incident_key": a["incident_key"],
                            "dataset": p["dataset"], "business_date": p["business_date"], "dag_id": p["dag_id"],
                            "attempt": a["attempt"], "gate_run_id": a["gate_run_id"], "action": a["action"],
                            "rule_id": a["rule_id"], "severity": a["severity"], "route": a["route"],
                            "policy_version": a["policy_version"], "mode": "approve", "approval_id": aid})
            print(f"  APPROVED  {aid}  by {dec['decided_by']} → re-extract {p['dataset']} {p['business_date']}")
    record_approvals(bq, cards)
    record(bq, incs)
    return actions


def list_approvals() -> None:
    bq = client_as(ENGINE)
    for a in _latest_approvals(bq)[:30]:
        print(f"{a['approval_id']}  {a['status']:9} {a['incident_id']}  {a['dataset']:12} {a['business_date']}  "
              f"{a['action']}  policy {a['policy_version']}  by {a['decided_by'] or '-'}  {a['note'] or ''}")


def status() -> None:
    bq = client_as(ENGINE)
    rows = _q(bq, f"""
        with h as (select *, min(`at`) over (partition by incident_id) opened_at
                   from `{bq.project}.{INCIDENT_DS}.incidents`)
        select * except (opened_at), opened_at,
               timestamp_diff(`at`, opened_at, second) as seconds_open
        from h qualify row_number() over (partition by incident_id order by `at` desc) = 1
        order by opened_at desc limit 30""")
    for r in rows:
        mins = round((r["seconds_open"] or 0) / 60, 1)
        print(f"{r['incident_id']}  {r['business_date']}  {r['dataset'] or '-':12} {r['failure_type']:18} "
              f"{r['status']:11} attempt {r['attempt']}  {mins:>5} min  "
              f"{r.get('severity') or '-'} {r.get('rule_id') or '-'} policy {r.get('policy_version') or '-'}  "
              f"{r['note'] or ''}")


def show_policy() -> None:
    p = load_policy()
    print(f"policy {p['version']} from {POLICY_FILE} · kill switch {'ON' if p['kill_switch'] else 'off'} "
          f"· attempt cap {p['limits']['attempt_cap']} · never_auto {p['never_auto']}")
    for kind in ("partial_extract", "offsetting_errors", "amount_mismatch", "raw_duplicates", "missing_receipt",
                 "checks_incomplete", "unit_or_range", "staging_mismatch", "build_error", "check_failed",
                 "extract_failed", "stale_feed", "late_data", "value_mismatch", "archived_history",
                 "count_excess", "late_change", "late_change_old", "young_orphan", "archived_parent", "true_orphan",
                 "merged_patient", "extract_failed+breaker", "check_failed+release"):
        f = {"type": kind.split("+")[0], "breaker": "INC-example" if "+breaker" in kind else None,
             "release": {"release_id": "REL-example"} if "+release" in kind else None}
        d = gates(f, p, checks_ran=kind != "checks_incomplete", attempts=0)
        print(f"  {kind:18} → {d['stage']:16} {d['rule_id']:20} {d['action']:20} {d['mode']:9} "
              f"{d['severity']} route {d['route']}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    e = sub.add_parser("evaluate")
    e.add_argument("--date", required=True)
    e.add_argument("--dry-run", action="store_true")
    e.add_argument("--fake-agent", choices=None, default=None,
                   help="test project only: send unknown incidents to the fake Diagnosis agent in this mode")
    e.add_argument("--agent-provider", choices=["vertex"], default=None,
                   help="ask the real agent (only if policy allows: confirmation, or synthetic trial in test)")
    d = sub.add_parser("diagnose")
    d.add_argument("--dataset", required=True)
    d.add_argument("--date", required=True)
    sub.add_parser("status")
    sub.add_parser("policy")
    sub.add_parser("approvals")
    pa = sub.add_parser("process-approvals")
    pa.add_argument("--pretend-later-minutes", type=int, default=0,
                    help="demo only: expire cards as if this many minutes had passed (cannot approve anything)")
    a = ap.parse_args()
    if a.cmd == "evaluate":
        evaluate(a.date, a.dry_run, fake_agent_mode=a.fake_agent, agent_provider=a.agent_provider)
    elif a.cmd == "diagnose":
        bd = dt.date.fromisoformat(a.date)
        days = [bd - dt.timedelta(days=i) for i in range(LOOKBACK_DAYS - 1, -1, -1)]
        for r in diagnose(client_as(ENGINE), a.dataset, days):
            print(r)
    elif a.cmd == "status":
        status()
    elif a.cmd == "approvals":
        list_approvals()
    elif a.cmd == "process-approvals":
        acts = process_approvals(a.pretend_later_minutes)
        if acts:  # same as the approval_worker DAG, except the rebuild is triggered by hand
            reextract(acts)
            print("re-extract done — now rebuild and re-verify: airflow dags trigger dbt_build "
                  f"--conf '{{\"business_date\": \"{max(x['business_date'] for x in acts)}\"}}'")
    else:
        show_policy()


if __name__ == "__main__":
    main()
