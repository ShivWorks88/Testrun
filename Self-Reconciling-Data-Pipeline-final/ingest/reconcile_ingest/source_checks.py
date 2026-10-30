"""Phase 6 packages 3 and 4 — checks that compare what we loaded with what the SOURCE says now.

Package 3 — late changes and expected differences (`source drift`, earlier days only, not the business date):
    archived_history       raw has MORE rows than the source, and the whole day is before the EHR archive
                           cutoff → expected (the source archived it) → log_expected, no alarm
    count_excess           raw has more rows than the source, NOT explained by the archive → a person looks
    late_change            the source changed a day we already loaded (more rows, or a change signature such
                           as reversed claims) inside the look-back window → rebuild_partition (that day)
    late_change_old        the same, older than the dbt look-back window (e.g. a claim reversed 5 days later)
                           → rebuild_partition with a dbt build for THAT day (look-back rebuild)
Package 4 — orphan records (lab results whose encounter is not in raw; claims/labs on a merged patient ID):
    young_orphan           parent missing, result collected in the last N hours of the day and the next day
                           not loaded yet → linked to the unknown member, re-linked when the parent arrives
    archived_parent        parent is in the EHR archive index → expected, log_expected
    true_orphan            parent exists nowhere → a person looks
    merged_patient         rows use a patient ID the MPI retired → remap_merged_patient (data owner approves);
                           the remap is a mapping table (ops.patient_remaps) applied in staging — raw never changes
Every finding carries counts only (no record or patient identifiers — T26). The classifiers are pure functions
(unit-tested); `run()` does the reads: BigQuery as sa-engine, the source replica read-only with the ingest DSN.
"""
from __future__ import annotations

import datetime as dt
from contextlib import contextmanager
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Asia/Kolkata")
DRIFT_TYPES = {"archived_history", "count_excess", "late_change", "late_change_old"}
ORPHAN_TYPES = {"young_orphan", "archived_parent", "true_orphan", "merged_patient"}
# what a "change after load" looks like per dataset (same expression on both sides)
SIGNATURES = {
    "claims": ("count(*) filter (where status = 'reversed')", "countif(status = 'reversed')"),
    "encounters": ("count(*) filter (where discharge_ts is not null)", "countif(discharge_ts is not null)"),
}
EVENT_COLUMN = {"claims": "submitted_ts", "encounters": "admit_ts", "lab_results": "resulted_ts"}
SOURCE_TABLE = {"claims": "billing.claims", "encounters": "ehr.encounters", "lab_results": "lis.lab_results"}
MERGE_DATASETS = ("claims", "lab_results")
REMAPS_TABLE = "patient_remaps"         # in the ops dataset; read by dbt staging, written only by the engine
REMAP_SCHEMA = [("retired_id", "STRING", "REQUIRED"), ("surviving_id", "STRING", "REQUIRED"),
                ("approval_id", "STRING", "NULLABLE"), ("applied_at", "TIMESTAMP", "REQUIRED")]


def detectors(policy: dict) -> dict:
    d = policy.get("detectors") or {}
    drift = {"enabled": True, "window_days": 7, "datasets": ["encounters", "claims"], **(d.get("source_drift") or {})}
    orphans = {"enabled": True, "young_hours": 2, **(d.get("orphans") or {})}
    merged = {"enabled": True, **(d.get("merged_patients") or {})}
    return {"drift": drift, "orphans": orphans, "merged": merged}


# ── pure classifiers ────────────────────────────────────────────────────────
def classify_drift(day: dt.date, bd: dt.date, lookback: int, src_rows: int, raw_rows: int,
                   src_sig: int | None, raw_sig: int | None, archive_cutoff: dt.date | None) -> str | None:
    if src_rows < raw_rows:
        return "archived_history" if archive_cutoff and day < archive_cutoff else "count_excess"
    if src_rows > raw_rows or (src_sig is not None and raw_sig is not None and src_sig != raw_sig):
        return "late_change_old" if day < bd - dt.timedelta(days=lookback - 1) else "late_change"
    return None


def classify_orphan(parent_archived: bool, collected_at: dt.datetime | None, bd: dt.date, next_day_loaded: bool,
                    young_hours: float) -> str:
    if parent_archived:
        return "archived_parent"
    end_of_day = dt.datetime.combine(bd + dt.timedelta(days=1), dt.time(), tzinfo=TZ)
    young = collected_at is not None and collected_at >= end_of_day - dt.timedelta(hours=young_hours)
    return "young_orphan" if young and not next_day_loaded else "true_orphan"


def drift_finding(kind: str, ds: str, day: dt.date, ev: dict) -> dict:
    return {"key": f"{kind}:{ds}:{day}", "dataset": ds, "date": day, "type": kind, "evidence": ev}


# ── reads ───────────────────────────────────────────────────────────────────
@contextmanager
def source():
    """Read-only connection to the source replica (same DSN as ingest)."""
    import psycopg

    from reconcile_ingest.settings import Settings
    with psycopg.connect(Settings.from_env().source_dsn, autocommit=True) as conn:
        conn.execute("set default_transaction_read_only = on")
        yield conn


def _bounds(day: dt.date) -> tuple[dt.datetime, dt.datetime]:
    start = dt.datetime.combine(day, dt.time(), tzinfo=TZ)
    return start, start + dt.timedelta(days=1)


def archive_cutoff(src) -> dt.date | None:
    if not src.execute("select to_regclass('ehr.archive_policy')").fetchone()[0]:
        return None
    row = src.execute("select cutoff_date from ehr.archive_policy where id = 1").fetchone()
    return row[0] if row else None


def drift(bq, src, bd: dt.date, policy: dict, lookback: int) -> list[dict]:
    from reconcile_ingest.engine import _q
    cfg = detectors(policy)["drift"]
    if not cfg["enabled"]:
        return []
    cutoff, out = archive_cutoff(src), []
    days = [bd - dt.timedelta(days=i) for i in range(int(cfg["window_days"]), 0, -1)]   # earlier days only
    for ds in cfg["datasets"]:
        src_sig_sql, raw_sig_sql = SIGNATURES.get(ds, ("null", "null"))
        raw = {r["d"]: r for r in _q(bq, f"""
            select business_date d, count(*) n, {raw_sig_sql} sig from `{bq.project}.raw.{ds}`
            where business_date between @a and @b
              and _batch_id in (select max(_batch_id) from `{bq.project}.raw.{ds}`
                                where business_date between @a and @b group by business_date)
            group by business_date""", a=days[0], b=days[-1])}   # the latest batch of each day = current raw
        for day in days:
            if day not in raw:
                continue                       # never loaded: nothing to compare
            s, e = _bounds(day)
            n, sig = src.execute(f"select count(*), {src_sig_sql} from {SOURCE_TABLE[ds]} "
                                 f"where {EVENT_COLUMN[ds]} >= %s and {EVENT_COLUMN[ds]} < %s", (s, e)).fetchone()
            kind = classify_drift(day, bd, lookback, int(n), int(raw[day]["n"]),
                                  None if sig is None else int(sig),
                                  None if raw[day]["sig"] is None else int(raw[day]["sig"]), cutoff)
            if kind:
                out.append(drift_finding(kind, ds, day, {
                    "source_rows": int(n), "raw_rows": int(raw[day]["n"]),
                    "source_changes": None if sig is None else int(sig),
                    "raw_changes": None if raw[day]["sig"] is None else int(raw[day]["sig"]),
                    "archive_cutoff": str(cutoff) if cutoff else None, "lookback_days": lookback}))
    return out


def orphans(bq, src, bd: dt.date, policy: dict) -> list[dict]:
    from reconcile_ingest.engine import _q
    cfg = detectors(policy)["orphans"]
    if not cfg["enabled"]:
        return []
    p = bq.project
    rows = _q(bq, f"""
        with r as (select encounter_id, max(collected_at) collected_at, count(*) n from `{p}.raw.lab_results`
                   where business_date = @d and encounter_id is not null
                     and _batch_id = (select max(_batch_id) from `{p}.raw.lab_results` where business_date = @d)
                   group by encounter_id)
        select r.* from r left join (select distinct encounter_id from `{p}.raw.encounters`) e using (encounter_id)
        where e.encounter_id is null""", d=bd)
    if not rows:
        return []
    ids = [r["encounter_id"] for r in rows]
    archived = set()
    if src.execute("select to_regclass('ehr.archive_index')").fetchone()[0]:
        archived = {x[0] for x in src.execute("select encounter_id from ehr.archive_index where encounter_id = any(%s)",
                                              (ids,)).fetchall()}
    nxt = _q(bq, f"select count(*) n from `{p}.raw.encounters` where business_date = @d",
             d=bd + dt.timedelta(days=1))[0]["n"]
    counts: dict[str, dict] = {}
    for r in rows:
        kind = classify_orphan(r["encounter_id"] in archived, r["collected_at"], bd, bool(nxt),
                               float(cfg["young_hours"]))
        c = counts.setdefault(kind, {"parents_missing": 0, "results": 0})
        c["parents_missing"] += 1
        c["results"] += int(r["n"])
    return [{"key": f"{k}:lab_results:{bd}", "dataset": "lab_results", "date": bd, "type": k,
             "evidence": {**v, "young_hours": cfg["young_hours"], "next_day_loaded": bool(nxt)}}
            for k, v in sorted(counts.items())]


def merged_ids(src) -> dict[str, str]:
    if not src.execute("select to_regclass('mpi.patient_merges')").fetchone()[0]:
        return {}
    return dict(src.execute("select retired_id, surviving_id from mpi.patient_merges").fetchall())


def merged(bq, src, bd: dt.date, policy: dict, lookback: int) -> list[dict]:
    from reconcile_ingest.engine import _q
    if not detectors(policy)["merged"]["enabled"]:
        return []
    retired = sorted(merged_ids(src))
    if not retired:
        return []
    out = []
    for ds in MERGE_DATASETS:
        r = _q(bq, f"""select count(*) n, count(distinct patient_id) ids from `{bq.project}.audit.stg_{ds}`
                       where business_date between @a and @b and patient_id in unnest(@ids)""",
               a=bd - dt.timedelta(days=lookback - 1), b=bd, ids=retired)[0]
        if r["n"]:
            out.append({"key": f"merged_patient:{ds}:{bd}", "dataset": ds, "date": bd, "type": "merged_patient",
                        "evidence": {"rows_on_retired_ids": int(r["n"]), "retired_ids": int(r["ids"])}})
    return out


def run(bq, bd: dt.date, policy: dict, lookback: int) -> list[dict]:
    """All package 3 and 4 findings for this business date. Any failure here never blocks the engine."""
    try:
        with source() as src:
            return drift(bq, src, bd, policy, lookback) + orphans(bq, src, bd, policy) + \
                merged(bq, src, bd, policy, lookback)
    except Exception as exc:  # noqa: BLE001 — source unreachable is the breaker's job, not this check's
        print(f"  (source checks skipped: {type(exc).__name__}: {str(exc).splitlines()[0][:120]})")
        return []


def init_remaps() -> str:
    """Create ops.patient_remaps (empty) as sa-engine. Run once before the first dbt build of package 4:
    staging reads it as a dbt source, so it must exist even when nothing is remapped yet."""
    from google.cloud import bigquery

    from reconcile_ingest.engine import ENGINE, client_as
    from reconcile_ingest.source_state import OPS_DS
    bq = client_as(ENGINE)
    table = bigquery.Table(f"{bq.project}.{OPS_DS}.{REMAPS_TABLE}",
                           schema=[bigquery.SchemaField(n, t, mode=m) for n, t, m in REMAP_SCHEMA])
    table.description = "Approved MPI remaps (retired -> surviving patient ID), applied in staging. Engine-written."
    t = bq.create_table(table, exists_ok=True)
    return f"{t.full_table_id} ready ({bq.get_table(t).num_rows} rows)"


# ── executor for remap_merged_patient (runs only after a data owner approved) ─
def apply_remaps(bq, dataset: str, day: dt.date, approval_id: str | None, lookback: int) -> tuple[bool, str]:
    """Write retired → surviving pairs for the IDs that occur in staging into ops.patient_remaps.
    staging (stg_claims, stg_lab_results) maps patient_id through that table on the next build."""
    from google.cloud import bigquery

    from reconcile_ingest.engine import _now, _q
    with source() as src:
        pairs = merged_ids(src)
    present = {r["patient_id"] for r in _q(bq, f"""
        select distinct patient_id from `{bq.project}.audit.stg_{dataset}`
        where business_date between @a and @b and patient_id in unnest(@ids)""",
        a=day - dt.timedelta(days=lookback - 1), b=day, ids=sorted(pairs) or ["-"])}
    done = {r["retired_id"] for r in _q(bq, f"select retired_id from `{bq.project}.ops.patient_remaps`")}
    rows = [{"retired_id": k, "surviving_id": pairs[k], "approval_id": approval_id, "applied_at": _now()}
            for k in sorted(present - done)]
    if rows:
        cfg = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        bq.load_table_from_json(rows, f"{bq.project}.ops.patient_remaps", job_config=cfg).result()
    return True, f"{len(rows)} retired patient ID(s) mapped to their surviving IDs (staging applies it on rebuild)"


if __name__ == "__main__":
    import sys
    if sys.argv[1:] == ["init"]:
        print(init_remaps())
    else:
        print("usage: python -m reconcile_ingest.source_checks init")
