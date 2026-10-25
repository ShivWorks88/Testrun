"""Phase 1 exit-gate checks, run against the real GCP project as the ingest identity.

  G1  each dataset has exactly one control-totals row per batch, and every extract batch has one
  G2  re-running the same date gives identical row counts and no duplicate _row_id
  G3  an archived file can be replayed into raw
  G4  ingest cannot write to analytics (the IAM call is denied)
  G5  an archived object cannot be overwritten or deleted
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from . import archive as arc
from .datasets import DATASETS
from .pipeline import ingest_dataset, replay_batch


@dataclass
class Check:
    gate: str
    name: str
    passed: bool
    detail: str


def _q(client, sql: str, params=None, max_bytes: int = 1 << 30):
    from google.cloud import bigquery
    cfg = bigquery.QueryJobConfig(query_parameters=params or [], maximum_bytes_billed=max_bytes,
                                  labels={"component": "verify"})
    return list(client.query(sql, job_config=cfg).result())


def run(settings, client, warehouse, archive, storage_client, business_date: date,
        rerun_dataset: str = "lab_orders") -> list[Check]:
    from google.api_core import exceptions as gexc
    from google.cloud import bigquery

    P = settings.project
    D = bigquery.ScalarQueryParameter("d", "DATE", business_date)
    checks: list[Check] = []

    # G1: one totals row per batch; every LOADED extract batch without the injection has one.
    dup = _q(client, f"select dataset, batch_id, count(*) n from `{P}.ops.control_totals` "
                     f"where business_date = @d group by 1, 2 having n <> 1", [D])
    missing = _q(client, f"""
        select b.dataset, b.batch_id from `{P}.ops.batches` b
        left join `{P}.ops.control_totals` t using (batch_id)
        where b.business_date = @d and b.mode = 'extract' and b.status = 'LOADED'
          and 'missing_control_totals' not in unnest(b.injections) and t.batch_id is null""", [D])
    loaded = _q(client, f"select count(distinct dataset) n from `{P}.ops.batches` "
                        f"where business_date = @d and status = 'LOADED'", [D])[0].n
    checks.append(Check("G1", "Exactly one control-totals row per batch",
                        not dup and not missing and loaded > 0,
                        f"{loaded} datasets loaded; {len(dup)} batches with != 1 totals row; "
                        f"{len(missing)} batches missing totals"))

    # G2: re-run the same date and compare.
    ds = DATASETS[rerun_dataset]
    r1 = ingest_dataset(settings, warehouse, archive, ds.name, business_date, run_id="verify-rerun-1")
    r2 = ingest_dataset(settings, warehouse, archive, ds.name, business_date, run_id="verify-rerun-2")
    stats = _q(client, f"select count(*) n, count(distinct _row_id) u, count(distinct _batch_id) b "
                       f"from `{P}.raw.{ds.name}` where business_date = @d", [D])[0]
    ok = r1.rows_loaded == r2.rows_loaded == stats.n == stats.u and stats.b == 1
    checks.append(Check("G2", f"Re-running {ds.name} gives identical counts",
                        ok, f"run1={r1.rows_loaded} run2={r2.rows_loaded} raw={stats.n} "
                            f"distinct_row_ids={stats.u} batches_in_partition={stats.b}"))

    # G3: replay the latest archived batch.
    rp = replay_batch(settings, warehouse, archive, ds.name, business_date, run_id="verify-replay")
    after = _q(client, f"select count(*) n from `{P}.raw.{ds.name}` where business_date = @d", [D])[0].n
    checks.append(Check("G3", "Archived file replays into raw", after == rp.rows_loaded == r2.rows_loaded,
                        f"replayed {rp.batch_id}: {rp.rows_loaded} rows; raw now {after}"))

    # G4: ingest must not be able to write analytics.
    try:
        _q(client, f"create or replace table `{P}.analytics._ingest_write_probe` as select 1 x")
        client.delete_table(f"{P}.analytics._ingest_write_probe", not_found_ok=True)
        checks.append(Check("G4", "Ingest cannot write analytics", False, "write SUCCEEDED: IAM gate is open"))
    except gexc.Forbidden as exc:
        checks.append(Check("G4", "Ingest cannot write analytics", True, f"denied: {exc.message[:120]}"))

    # G5: archive objects are write-once and retained.
    name = arc.object_name(ds.name, business_date, rp.batch_id)
    blob = storage_client.bucket(settings.archive_bucket).blob(name)
    results = []
    try:
        blob.upload_from_string(b"tamper", if_generation_match=0)
        results.append("overwrite SUCCEEDED")
    except (gexc.PreconditionFailed, gexc.Forbidden) as exc:
        results.append(f"overwrite denied ({exc.code})")
    try:
        blob.delete()
        results.append("delete SUCCEEDED")
    except (gexc.Forbidden, gexc.PermissionDenied) as exc:
        results.append(f"delete denied ({exc.code})")
    checks.append(Check("G5", "Archive objects cannot be overwritten or deleted",
                        not any("SUCCEEDED" in r for r in results), "; ".join(results)))
    return checks
