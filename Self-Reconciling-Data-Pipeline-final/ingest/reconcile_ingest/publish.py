"""Phase 3 — publication gate (Write-Audit-Publish).

dbt builds and tests into `audit`. This module decides, per report table (mart) and
business date, whether that day may be shown to users, and copies only approved days
into `analytics` as sa-publish — the only identity allowed to write there.

    python -m reconcile_ingest.publish gate     --date 2026-10-14
    python -m reconcile_ingest.publish status   [--days 30]
    python -m reconcile_ingest.publish rollback --dataset fct_daily_claims [--snapshot NAME]

Phase 6: the gate also respects the SOURCES. A report is HELD while an outage breaker is open for any
source it is built from (or the GLOBAL breaker), while that source's feed is stale, or while one of its
datasets has far fewer rows than usual (late data). The breakers are re-read immediately before every
copy, so a breaker that opens during a publish still stops it (T20).
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import uuid
from pathlib import Path

import google.auth
from google.api_core.exceptions import NotFound
from google.auth import impersonated_credentials
from google.cloud import bigquery

SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]
LOCATION = os.environ.get("BQ_LOCATION", "asia-south1")
LOOKBACK_DAYS = int(os.environ.get("LOOKBACK_DAYS", "3"))
DEFAULT_TARGET_DIR = os.environ.get("DBT_AUDIT_TARGET_PATH", "/tmp/dbt-target/audit")
VALUE_TARGET_DIR = os.environ.get("DBT_VALUERECON_TARGET_PATH", "/tmp/dbt-target/valuerecon")
VALUE_CHECKED = {"lab_results"}   # datasets with a value-level check (models/value_recon/value_recon_<dataset>)
SNAPSHOT_DAYS = 7
STATE_DS = os.environ.get("PUBLICATION_DATASET", "publication")  # gate records + snapshots
NON_BLOCKING = {"warning", "warn", "caveat", "informational"}

CHECK_SCHEMA = [
    bigquery.SchemaField("gate_run_id", "STRING"),
    bigquery.SchemaField("dbt_invocation_id", "STRING"),
    bigquery.SchemaField("business_date", "DATE"),
    bigquery.SchemaField("unique_id", "STRING"),
    bigquery.SchemaField("name", "STRING"),
    bigquery.SchemaField("resource_type", "STRING"),
    bigquery.SchemaField("status", "STRING"),
    bigquery.SchemaField("failures", "INT64"),
    bigquery.SchemaField("policy_outcome", "STRING"),
    bigquery.SchemaField("attached_to", "STRING"),
    bigquery.SchemaField("recorded_at", "TIMESTAMP"),
]
STATE_SCHEMA = [
    bigquery.SchemaField("dataset", "STRING"),
    bigquery.SchemaField("business_date", "DATE"),
    bigquery.SchemaField("state", "STRING"),  # PENDING | HELD | PUBLISHED | ROLLED_BACK
    bigquery.SchemaField("reasons", "STRING"),
    bigquery.SchemaField("gate_run_id", "STRING"),
    bigquery.SchemaField("dbt_invocation_id", "STRING"),
    bigquery.SchemaField("snapshot", "STRING"),
    bigquery.SchemaField("actor", "STRING"),
    bigquery.SchemaField("decided_at", "TIMESTAMP"),
]


# ── identities ──────────────────────────────────────────────────────────────
def service_account(name: str) -> str:
    explicit = os.environ.get(f"SA_{name.upper()}")
    if explicit:
        return explicit
    return os.environ["SA_DBT"].replace("sa-dbt@", f"sa-{name}@")


def project_id() -> str:
    for key in ("GCP_PROJECT_ID", "PROJECT_ID", "GOOGLE_CLOUD_PROJECT"):
        if os.environ.get(key):
            return os.environ[key]
    return service_account("publish").split("@", 1)[1].split(".", 1)[0]


def client_as(name: str) -> bigquery.Client:
    """BigQuery client that acts as sa-<name> (impersonation, no key files)."""
    source, _ = google.auth.default(scopes=SCOPES)
    creds = impersonated_credentials.Credentials(
        source_credentials=source, target_principal=service_account(name),
        target_scopes=SCOPES, lifetime=3600)
    return bigquery.Client(project=project_id(), credentials=creds, location=LOCATION)


def _now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


# ── state tables in BigQuery `publication` ──────────────────────────────────────────
def ensure_tables(bq: bigquery.Client) -> None:
    for name, schema in (("check_results", CHECK_SCHEMA), ("publication_state", STATE_SCHEMA)):
        table = bigquery.Table(f"{bq.project}.{STATE_DS}.{name}", schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(field="business_date")
        bq.create_table(table, exists_ok=True)


def append(bq: bigquery.Client, name: str, rows: list[dict], schema) -> None:
    if rows:
        cfg = bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_APPEND")
        bq.load_table_from_json(rows, f"{bq.project}.{STATE_DS}.{name}", job_config=cfg).result()


def latest_state(bq: bigquery.Client, dataset: str, day: dt.date):
    sql = f"""select state, gate_run_id from `{bq.project}.{STATE_DS}.publication_state`
              where dataset = @ds and business_date = @d
              qualify row_number() over (order by decided_at desc) = 1"""
    cfg = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("ds", "STRING", dataset),
        bigquery.ScalarQueryParameter("d", "DATE", day)])
    rows = list(bq.query(sql, job_config=cfg).result())
    return (rows[0].state, rows[0].gate_run_id) if rows else (None, None)


# ── reading dbt's own record of the run ─────────────────────────────────────
def load_artifacts(target_dir: str, not_before: dt.datetime | None = None):
    t = Path(target_dir)
    manifest = json.loads((t / "manifest.json").read_text())
    try:
        results = json.loads((t / "run_results.json").read_text())
    except FileNotFoundError:
        return manifest, None
    generated = results.get("metadata", {}).get("generated_at", "").replace("Z", "+00:00")
    if not_before and generated and dt.datetime.fromisoformat(generated) < not_before:
        return manifest, None  # left over from an earlier run — never trust it
    return manifest, results


def _policy(node: dict) -> str:
    return ((node.get("config") or {}).get("meta") or {}).get("policy_outcome") \
        or (node.get("meta") or {}).get("policy_outcome") or "blocking"


def _upstream(manifest: dict, uid: str) -> set[str]:
    nodes = {**manifest["nodes"], **manifest.get("sources", {})}
    seen, stack = set(), [uid]
    while stack:
        for dep in nodes.get(stack.pop(), {}).get("depends_on", {}).get("nodes", []):
            if dep.split(".")[0] in ("model", "source", "seed", "snapshot") and dep not in seen:
                seen.add(dep)
                stack.append(dep)
    return seen


def mart_upstream(manifest: dict) -> dict[str, tuple[set[str], set[str]]]:
    """{mart: (source systems, raw datasets)} it is built from, read from dbt's own lineage."""
    from reconcile_ingest.datasets import DATASETS
    out = {}
    for uid, n in manifest["nodes"].items():
        if n["resource_type"] != "model" or "/marts/" not in "/" + n["original_file_path"].replace("\\", "/"):
            continue
        tables = {u.split(".")[-1] for u in _upstream(manifest, uid) if u.startswith("source.")}
        datasets = {t for t in tables if t in DATASETS}
        out[n["name"]] = ({DATASETS[t].source_system for t in datasets}, datasets)
    return out


def decide(manifest: dict, results: dict | None, bd: dt.date, blocked: dict[str, str] | None = None,
           low: dict[str, str] | None = None):
    """Return (window dates, {mart: (state, [reasons])}).
    blocked: {source or GLOBAL: reason} (open breakers, stale feeds); low: {dataset: reason} (late data)."""
    window = [bd - dt.timedelta(days=i) for i in range(LOOKBACK_DAYS - 1, -1, -1)]
    marts = {n["name"]: uid for uid, n in manifest["nodes"].items()
             if n["resource_type"] == "model"
             and "/marts/" in "/" + n["original_file_path"].replace("\\", "/")}

    if results is None:
        global_reasons = ["checks did not run in this run (no fresh run_results.json)"]
        by_uid = {}
    elif str(bd) not in json.dumps(results.get("args", {}).get("vars", {}), default=str):
        global_reasons = [f"dbt results are not for business_date {bd}"]
        by_uid = {}
    else:
        by_uid = {r["unique_id"]: r for r in results["results"]}
        global_reasons = [f"{uid.split('.')[-1]} {r['status']}" for uid, r in by_uid.items()
                          if uid.startswith("operation.") and r["status"] not in ("success", "pass")]

    decisions = {}
    upstream = mart_upstream(manifest) if (blocked or low) else {}
    for name, mid in marts.items():
        scope = {mid} | _upstream(manifest, mid)
        reasons = list(global_reasons)
        if by_uid:
            for uid in scope:
                if uid.startswith("model."):
                    r = by_uid.get(uid)
                    model = uid.split(".")[-1]
                    if r is None:
                        reasons.append(f"{model} was not built")
                    elif r["status"] != "success":
                        reasons.append(f"{model} {r['status']}")
            for uid, node in manifest["nodes"].items():
                if node["resource_type"] != "test" or not scope & set(node["depends_on"]["nodes"]):
                    continue
                r = by_uid.get(uid)
                if r is None:
                    reasons.append(f"{node['name']} did not run")
                elif r["status"] == "skipped":
                    reasons.append(f"{node['name']} skipped")
                elif r["status"] in ("fail", "error") and _policy(node) not in NON_BLOCKING:
                    reasons.append(f"{node['name']} {r['status'].upper()} ({r.get('failures')})")
        if blocked or low:
            from reconcile_ingest.source_state import blocked_reasons
            sources, datasets = upstream.get(name, (set(), set()))
            reasons += blocked_reasons(sources, datasets, blocked or {}, low or {})
        reasons = sorted(set(reasons))
        decisions[name] = ("HELD" if reasons else "PENDING", reasons)
    return window, decisions


def value_check_reasons(vmanifest: dict | None, vresults: dict | None, bd: dt.date) -> dict[str, str]:
    """Phase 6 pkg 2 (G4): {dataset: reason} when its value-level check failed or did not run for this date.
    Reports built from that dataset are held — a wrong lab value must never reach users (T6)."""
    if vresults is None or str(bd) not in json.dumps(vresults.get("args", {}).get("vars", {}), default=str):
        return {d: f"value-level check for {d} did not run in this run" for d in VALUE_CHECKED}
    nodes = (vmanifest or {}).get("nodes", {})
    out: dict[str, str] = {}
    for r in vresults.get("results", []):
        uid = r["unique_id"]
        if not uid.startswith("test.") or r["status"] not in ("fail", "error"):
            continue
        node = nodes.get(uid, {})
        if _policy(node) in NON_BLOCKING:
            continue
        for dep in node.get("depends_on", {}).get("nodes", []):
            model = dep.split(".")[-1]
            if model.startswith("value_recon_"):
                d = model[len("value_recon_"):]
                out[d] = f"value-level check {uid.split('.')[2]} {r['status'].upper()} ({r.get('failures')}) for {d}"
    return out


# ── copying approved days into analytics ────────────────────────────────────
def _take_snapshot(bq: bigquery.Client, mart: str) -> str | None:
    dst = f"{bq.project}.analytics.{mart}"
    try:
        bq.get_table(dst)
    except NotFound:
        return None
    name = f"snap_{mart}_{dt.datetime.now(dt.timezone.utc):%Y%m%d%H%M%S}"
    snap_id = f"{bq.project}.{STATE_DS}.{name}"
    # a plain copy (not CREATE SNAPSHOT): needs only read on analytics + write on publication
    bq.copy_table(dst, snap_id).result()
    snap = bq.get_table(snap_id)
    snap.expires = dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=SNAPSHOT_DAYS)
    snap.description = f"Copy of analytics.{mart} taken just before a publish (for rollback)"
    bq.update_table(snap, ["expires", "description"])
    return name


def _ensure_like(bq: bigquery.Client, src: bigquery.Table, dst_id: str) -> None:
    try:
        bq.get_table(dst_id)
    except NotFound:
        t = bigquery.Table(dst_id, schema=src.schema)
        t.time_partitioning = src.time_partitioning
        t.clustering_fields = src.clustering_fields
        t.description = src.description
        bq.create_table(t)


def publish(bq, decisions, window, gate_run_id, invocation, upstream: dict | None = None) -> list[dict]:
    p, actor, rows = bq.project, service_account("publish"), []

    def row(mart, day, state, reason, snap=None):
        return {"dataset": mart, "business_date": str(day), "state": state, "reasons": reason,
                "gate_run_id": gate_run_id, "dbt_invocation_id": invocation, "snapshot": snap,
                "actor": actor, "decided_at": _now()}

    for mart, (state, _) in sorted(decisions.items()):
        if state != "PENDING":
            continue
        src = bq.get_table(f"{p}.audit.{mart}")
        dst_id = f"{p}.analytics.{mart}"
        field = src.time_partitioning.field if src.time_partitioning else None
        # re-read the decision immediately before copying: a later HOLD always wins
        still_ok = {d: latest_state(bq, mart, d) == ("PENDING", gate_run_id) for d in window}
        # T20: re-read the breakers too — an outage that started during this publish stops the copy
        if upstream is not None:
            from reconcile_ingest.source_state import blocked_reasons, blocked_sources
            now_blocked = blocked_reasons(*upstream.get(mart, (set(), set())), blocked_sources(bq), {})
            if now_blocked:
                rows += [row(mart, d, "HELD", "breaker opened before publish: " + "; ".join(now_blocked))
                         for d in window]
                continue
        snap = _take_snapshot(bq, mart)
        if field:
            _ensure_like(bq, src, dst_id)
            for d in window:
                if not still_ok[d]:
                    rows.append(row(mart, d, "HELD", "state changed before publish"))
                    continue
                cfg = bigquery.QueryJobConfig(query_parameters=[
                    bigquery.ScalarQueryParameter("d", "DATE", d)])
                n = list(bq.query(f"select count(*) n from `{src.full_table_id.replace(':', '.')}` "
                                  f"where date({field}) = @d", job_config=cfg).result())[0].n
                if n:
                    bq.copy_table(f"{p}.audit.{mart}${d:%Y%m%d}", f"{dst_id}${d:%Y%m%d}",
                                  job_config=bigquery.CopyJobConfig(
                                      write_disposition="WRITE_TRUNCATE")).result()
                rows.append(row(mart, d, "PUBLISHED", f"{n} rows copied", snap))
        elif all(still_ok.values()):
            bq.copy_table(f"{p}.audit.{mart}", dst_id, job_config=bigquery.CopyJobConfig(
                write_disposition="WRITE_TRUNCATE")).result()
            rows += [row(mart, d, "PUBLISHED", "whole table copied", snap) for d in window]
        else:
            rows += [row(mart, d, "HELD", "state changed before publish") for d in window]
    append(bq, "publication_state", rows, STATE_SCHEMA)
    return rows


# ── entry points ────────────────────────────────────────────────────────────
def run_gate(business_date, target_dir: str = DEFAULT_TARGET_DIR, gate_run_id: str | None = None,
             not_before: dt.datetime | None = None, dry_run: bool = False,
             value_target_dir: str | None = None) -> dict:
    bd = dt.date.fromisoformat(str(business_date))
    gate_run_id = gate_run_id or f"gate-{uuid.uuid4().hex[:12]}"
    manifest, results = load_artifacts(target_dir, not_before)
    bq = None if dry_run else client_as("publish")
    blocked, low = {}, {}
    if bq is not None:   # Phase 6: what the sources say (breakers, stale feeds, late data) — read from ops
        from reconcile_ingest.source_state import blocked_sources, low_volume_datasets
        blocked, low = blocked_sources(bq), low_volume_datasets(bq, bd)
    vmanifest = vresults = None
    if value_target_dir:   # Phase 6 pkg 2: the value-level check (dbt target valuerecon) is part of the gate
        try:
            vmanifest, vresults = load_artifacts(value_target_dir, not_before)
        except FileNotFoundError:
            vmanifest, vresults = None, None
        low = {**value_check_reasons(vmanifest, vresults, bd), **low}
    window, decisions = decide(manifest, results, bd, blocked, low)
    invocation = (results or {}).get("metadata", {}).get("invocation_id")

    print(f"Publication gate {gate_run_id} · business_date {bd} · window {window[0]} .. {window[-1]}")
    for mart, (state, reasons) in sorted(decisions.items()):
        print(f"  {state:9} {mart}" + ("" if not reasons else "  ← " + "; ".join(reasons)))
    if dry_run:
        return {"decisions": decisions}

    ensure_tables(bq)
    nodes = manifest["nodes"]
    append(bq, "check_results", [{
        "gate_run_id": gate_run_id, "dbt_invocation_id": invocation, "business_date": str(bd),
        "unique_id": r["unique_id"], "name": (r["unique_id"].split(".") + [""] * 3)[2],
        "resource_type": r["unique_id"].split(".")[0], "status": r["status"],
        "failures": r.get("failures"),
        "policy_outcome": _policy(nodes.get(r["unique_id"], {})),
        "attached_to": ",".join(d.split(".")[-1] for d in
                                nodes.get(r["unique_id"], {}).get("depends_on", {}).get("nodes", [])),
        "recorded_at": _now()} for r in (results or {}).get("results", [])], CHECK_SCHEMA)
    vnodes = (vmanifest or {}).get("nodes", {})
    append(bq, "check_results", [{   # value-level results go to the same record, so the engine sees them (G4)
        "gate_run_id": gate_run_id, "dbt_invocation_id": (vresults or {}).get("metadata", {}).get("invocation_id"),
        "business_date": str(bd), "unique_id": r["unique_id"], "name": (r["unique_id"].split(".") + [""] * 3)[2],
        "resource_type": r["unique_id"].split(".")[0], "status": r["status"], "failures": r.get("failures"),
        "policy_outcome": _policy(vnodes.get(r["unique_id"], {})),
        "attached_to": ",".join(d.split(".")[-1] for d in
                                vnodes.get(r["unique_id"], {}).get("depends_on", {}).get("nodes", [])),
        "recorded_at": _now()} for r in (vresults or {}).get("results", [])
        if r["unique_id"].startswith(("test.", "model."))], CHECK_SCHEMA)
    append(bq, "publication_state", [{
        "dataset": mart, "business_date": str(d), "state": state, "reasons": "; ".join(reasons),
        "gate_run_id": gate_run_id, "dbt_invocation_id": invocation, "snapshot": None,
        "actor": service_account("publish"), "decided_at": _now()}
        for mart, (state, reasons) in decisions.items() for d in window], STATE_SCHEMA)

    published = publish(bq, decisions, window, gate_run_id, invocation, upstream=mart_upstream(manifest))
    for r in published:
        print(f"  {r['state']:9} {r['dataset']} {r['business_date']}  {r['reasons']}")
    held = sorted(m for m, (s, _) in decisions.items() if s == "HELD")
    return {"gate_run_id": gate_run_id, "held": held,
            "published": sorted({r["dataset"] for r in published if r["state"] == "PUBLISHED"})}


def status(days: int = 30) -> None:
    bq = client_as("publish")
    sql = f"""select dataset, business_date, state, reasons, decided_at
              from `{bq.project}.{STATE_DS}.publication_state`
              where business_date >= date_sub(current_date('Asia/Kolkata'), interval {int(days)} day)
              qualify row_number() over (partition by dataset, business_date
                                         order by decided_at desc) = 1
              order by business_date, dataset"""
    for r in bq.query(sql).result():
        print(f"{r.business_date}  {r.dataset:20} {r.state:11} {r.reasons or ''}")


def rollback(dataset: str, snapshot: str | None = None) -> None:
    bq = client_as("publish")
    p = bq.project
    if snapshot is None:
        snaps = sorted(t.table_id for t in bq.list_tables(f"{p}.{STATE_DS}")
                       if t.table_id.startswith(f"snap_{dataset}_"))
        if not snaps:
            raise SystemExit(f"No snapshot found for {dataset}")
        snapshot = snaps[-1]
    bq.copy_table(f"{p}.{STATE_DS}.{snapshot}", f"{p}.analytics.{dataset}",
                  job_config=bigquery.CopyJobConfig(write_disposition="WRITE_TRUNCATE")).result()
    cfg = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("s", "STRING", snapshot)])
    days = [r.business_date for r in bq.query(
        f"select distinct business_date from `{p}.{STATE_DS}.publication_state` where snapshot = @s",
        job_config=cfg).result()]
    append(bq, "publication_state", [{
        "dataset": dataset, "business_date": str(d), "state": "ROLLED_BACK",
        "reasons": f"restored from {STATE_DS}.{snapshot}", "gate_run_id": f"rollback-{uuid.uuid4().hex[:8]}",
        "dbt_invocation_id": None, "snapshot": snapshot, "actor": service_account("publish"),
        "decided_at": _now()} for d in days], STATE_SCHEMA)
    print(f"analytics.{dataset} restored from {STATE_DS}.{snapshot}; dates marked ROLLED_BACK: "
          f"{', '.join(map(str, sorted(days))) or '-'}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    g = sub.add_parser("gate")
    g.add_argument("--date", required=True)
    g.add_argument("--target-dir", default=DEFAULT_TARGET_DIR)
    g.add_argument("--dry-run", action="store_true")
    s = sub.add_parser("status")
    s.add_argument("--days", type=int, default=30)
    r = sub.add_parser("rollback")
    r.add_argument("--dataset", required=True)
    r.add_argument("--snapshot")
    a = ap.parse_args()
    if a.cmd == "gate":
        run_gate(a.date, a.target_dir, dry_run=a.dry_run)
    elif a.cmd == "status":
        status(a.days)
    else:
        rollback(a.dataset, a.snapshot)


if __name__ == "__main__":
    main()
