"""Phase 4 — recovery engine (deterministic rules, no AI), driven by policy/policy.yml.

After every dbt_build the engine reads what the gate recorded, opens an incident per problem,
diagnoses it against the source receipt, picks the rule from policy.yml (stage 1 safety overrides,
then stage 2 ordinary rules, first match wins), applies the five gates and — only when the rule's
route is `auto` and the action is safe — re-extracts the bad day and asks dbt_build to rebuild and
re-verify. Every decision records the policy version, rule, severity and route.

    python -m reconcile_ingest.engine evaluate --date 2026-10-14 [--dry-run]
    python -m reconcile_ingest.engine diagnose --dataset encounters --date 2026-10-14
    python -m reconcile_ingest.engine status
    python -m reconcile_ingest.engine policy        # show what the engine reads from policy.yml
"""
from __future__ import annotations

import argparse
import datetime as dt
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
IMPLEMENTED_ACTIONS = {"reextract_from_raw"}
# diagnoses that mean the raw data itself is wrong (a re-extract can help)
RAW_PROBLEMS = {"partial_extract", "missing_load", "offsetting_errors", "amount_mismatch", "missing_receipt"}
OPEN_STATES = {"OPEN", "ACTING", "RECOMMENDED", "ESCALATED"}

INCIDENT_SCHEMA = [
    bigquery.SchemaField("incident_id", "STRING"),
    bigquery.SchemaField("incident_key", "STRING"),
    bigquery.SchemaField("business_date", "DATE"),
    bigquery.SchemaField("dataset", "STRING"),
    bigquery.SchemaField("failure_type", "STRING"),
    bigquery.SchemaField("status", "STRING"),   # OPEN | ACTING | RECOMMENDED | ESCALATED | RESOLVED
    bigquery.SchemaField("attempt", "INT64"),
    bigquery.SchemaField("action", "STRING"),
    bigquery.SchemaField("mode", "STRING"),     # auto | recommend | escalate | none
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


def gates(finding: dict, policy: dict, checks_ran: bool, attempts: int) -> dict:
    """The five gates, in order, driven by policy.yml. Returns the decision."""
    facts = facts_for(finding["type"])
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
         "answer": "receipt and raw counts read from BigQuery; no breaker open" if facts["control_totals_present"]
         else "no receipt for the loaded batch — the data cannot be verified", "pass": facts["control_totals_present"]},
        {"gate": 3, "question": "Is the difference expected?",
         "answer": "yes — log as expected" if action == "log_expected" else "no expected-difference rule matched",
         "pass": True},
        {"gate": 4, "question": "Which rule matches?",
         "answer": f"{stage} · {rule['id']} → {action} (route {route}, {severity}){window_note}",
         "pass": route == "auto"},
        {"gate": 5, "question": "Do guardrails allow a fix?", "answer": "", "pass": False},
    ]

    def done(mode, answer, ok=False):
        trace[4]["answer"], trace[4]["pass"] = answer, ok
        return {"trace": trace, "action": action, "mode": mode, "rule_id": rule["id"], "stage": stage,
                "severity": severity, "route": route}

    if not checks_ran and rule["id"] != "checks_did_not_run":
        return done("escalate", "checks did not run — never act on incomplete evidence")
    if route != "auto":
        return done("escalate", f"route {route}: a person decides (approvals arrive in Phase 5 stage 1)")
    if action in never or (catalog.get(action) or {}).get("mode") != "auto":
        return done("escalate", f"{action} is never automatic or needs approval")
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
            return bigquery.ArrayQueryParameter(k, "DATE", v)
        return bigquery.ScalarQueryParameter(k, "STRING", v)
    cfg = bigquery.QueryJobConfig(query_parameters=[param(k, v) for k, v in params.items()])
    return [dict(r) for r in bq.query(sql, job_config=cfg).result()]


def ensure_table(bq) -> None:
    t = bigquery.Table(f"{bq.project}.{INCIDENT_DS}.incidents", schema=INCIDENT_SCHEMA)
    t.time_partitioning = bigquery.TimePartitioning(field="business_date")
    bq.create_table(t, exists_ok=True)


def record(bq, rows: list[dict]) -> None:
    if rows:
        cfg = bigquery.LoadJobConfig(
            schema=INCIDENT_SCHEMA, write_disposition="WRITE_APPEND",
            schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION])  # adds the v5.2 columns
        bq.load_table_from_json(rows, f"{bq.project}.{INCIDENT_DS}.incidents", job_config=cfg).result()


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
            "business_date": str(d), "diagnosis": classify(r, raw), "batch_id": got["batch_id"] or (r and r["batch_id"]),
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
                      where incident_id = @i and status = 'ACTING'""", i=incident_id)
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
        if f["resource_type"] == "operation":
            out.append({"key": f"checks_incomplete:{days[-1]}", "dataset": None, "date": days[-1],
                        "type": "checks_incomplete", "evidence": {"check": name, "status": f["status"]}})
        elif name.startswith(("reconcile_to_control_totals", "control_totals_present")) and _dataset_of(f):
            receipt_datasets.add(_dataset_of(f))
        elif f["resource_type"] == "model":
            out.append({"key": f"build_error:{name}:{days[-1]}", "dataset": None, "date": days[-1],
                        "type": "build_error", "evidence": {"model": name}})
        else:
            kind = "unit_or_range" if "units_and_ranges" in name else "check_failed"
            out.append({"key": f"{kind}:{name}:{days[-1]}", "dataset": _dataset_of(f), "date": days[-1],
                        "type": kind, "evidence": {"check": name, "failures": f["failures"]}})
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


# ── 4. evaluate: one deterministic pass ─────────────────────────────────────
def evaluate(business_date, dry_run: bool = False, policy: dict | None = None) -> list[dict]:
    """Returns the list of re-extract actions to run (empty when nothing to do)."""
    policy = policy or load_policy()
    bd = dt.date.fromisoformat(str(business_date))
    days = [bd - dt.timedelta(days=i) for i in range(LOOKBACK_DAYS - 1, -1, -1)]
    bq = client_as(ENGINE)
    if not dry_run:
        ensure_table(bq)
    gate_run, failures = latest_failures(bq, bd)
    print(f"Recovery engine · business_date {bd} · gate run {gate_run} · {len(failures)} blocking failure(s) "
          f"· policy {policy['version']} · kill switch {'ON' if policy['kill_switch'] else 'off'}")
    current = findings(bq, failures, days)
    checks_ran = not any(f["type"] == "checks_incomplete" for f in current)
    existing = open_incidents(bq, days) if not dry_run else {}
    actor, rows, actions = service_account(ENGINE), [], []

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
        if key not in current_keys and inc["gate_run_id"] != gate_run:
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
        if d["mode"] == "auto":
            status, attempt, note = "ACTING", attempts + 1, f"re-extract {f['dataset']} {f['date']}"
            actions.append({"incident_id": inc_id, "incident_key": f["key"], "dataset": f["dataset"],
                            "business_date": str(f["date"]), "dag_id": SOURCE_DAG[f["dataset"]],
                            "attempt": attempt, "gate_run_id": gate_run, "action": d["action"],
                            "rule_id": d["rule_id"], "severity": d["severity"], "route": d["route"],
                            "policy_version": policy["version"]})
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
        record(bq, rows)
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
        error, loaded = None, {}
        try:
            res = pipeline.ingest_dataset(s, wh, ar, a["dataset"], day, [],
                                          run_id=f"recovery__{a['incident_id']}__a{a['attempt']}")
            loaded = getattr(res, "as_dict", lambda: {})()
        except Exception as exc:  # noqa: BLE001 — recorded in the journal, then re-raised below
            error = str(exc).splitlines()[0][:300]
        after = diagnose(bq, a["dataset"], [day])[0]
        ok = error is None and after["diagnosis"] not in RAW_PROBLEMS
        note = (f"re-extract failed: {error}" if error else
                f"re-extract done (batch {loaded.get('batch_id', '?')}); raw now {after['raw_rows']} rows "
                f"vs receipt {after['receipt_rows']} → {after['diagnosis']}")
        record(bq, [{
            "incident_id": a["incident_id"], "incident_key": a["incident_key"],
            "business_date": a["business_date"], "dataset": a["dataset"],
            "failure_type": a["incident_key"].split(":")[0], "status": "ACTING",
            "attempt": a["attempt"], "action": a["action"], "mode": "auto",
            "evidence": json.dumps(after, default=str), "gates": None, "note": note,
            "gate_run_id": a.get("gate_run_id"), "actor": service_account(ENGINE), "at": _now(),
            "policy_version": a.get("policy_version"), "rule_id": a.get("rule_id"),
            "severity": a.get("severity"), "route": a.get("route")}])
        print(("✔ " if ok else "✘ ") + note)
        results.append({**a, "ok": ok, "after": after})
    if not all(r["ok"] for r in results):
        raise RuntimeError("re-extract did not restore the data; the incident stays open")
    return results


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
                 "checks_incomplete", "unit_or_range", "staging_mismatch", "build_error", "check_failed"):
        d = gates({"type": kind}, p, checks_ran=kind != "checks_incomplete", attempts=0)
        print(f"  {kind:18} → {d['stage']:16} {d['rule_id']:20} {d['action']:20} {d['mode']:9} "
              f"{d['severity']} route {d['route']}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    e = sub.add_parser("evaluate")
    e.add_argument("--date", required=True)
    e.add_argument("--dry-run", action="store_true")
    d = sub.add_parser("diagnose")
    d.add_argument("--dataset", required=True)
    d.add_argument("--date", required=True)
    sub.add_parser("status")
    sub.add_parser("policy")
    a = ap.parse_args()
    if a.cmd == "evaluate":
        evaluate(a.date, a.dry_run)
    elif a.cmd == "diagnose":
        bd = dt.date.fromisoformat(a.date)
        days = [bd - dt.timedelta(days=i) for i in range(LOOKBACK_DAYS - 1, -1, -1)]
        for r in diagnose(client_as(ENGINE), a.dataset, days):
            print(r)
    elif a.cmd == "status":
        status()
    else:
        show_policy()


if __name__ == "__main__":
    main()
