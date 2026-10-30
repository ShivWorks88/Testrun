"""Phase 0 exit gate, last item — prove the environment can be rebuilt and the data comes back the same.

    python -m reconcile_ingest.rebuild_proof snapshot before --days 2026-10-12 2026-10-14
    ...  terraform destroy + apply (everything except the archive bucket), redeploy — see the runbook  ...
    python -m reconcile_ingest.rebuild_proof replay --days 2026-10-12 2026-10-14    # raw from the archive only
    python -m reconcile_ingest.rebuild_proof snapshot after --days 2026-10-12 2026-10-14
    python -m reconcile_ingest.rebuild_proof compare before after

A snapshot is a FINGERPRINT per table and business day: the row count and an order-independent hash of every
business column (metadata columns starting with "_" are left out, because a replay writes new load times).
  raw     each dataset's newest batch for the day (what the pipeline built the reports from)
  marts   what users see: analytics.<mart> for the day
Equal fingerprints before and after = the rebuilt environment holds exactly the same data. Fingerprints are
numbers only; no rows leave BigQuery. Files go to logs/rebuild_<label>.json (never committed).
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
from pathlib import Path

MARTS = ["dim_encounters", "fct_bed_occupancy", "fct_daily_claims", "fct_lab_results_daily"]
# load / build / publish bookkeeping: differs after any rebuild, says nothing about the data
VOLATILE = {"published_at", "gate_run_id", "built_at", "loaded_at", "dbt_run_id", "run_id", "snapshot",
            "invocation_id", "updated_at_build"}
LOG_DIR = Path(os.environ.get("REBUILD_PROOF_DIR", "/opt/reconcile/logs"))


def days_between(a: str, b: str) -> list[dt.date]:
    d0, d1 = dt.date.fromisoformat(a), dt.date.fromisoformat(b)
    if d1 < d0:
        d0, d1 = d1, d0
    return [d0 + dt.timedelta(days=i) for i in range((d1 - d0).days + 1)]


def compare(before: dict, after: dict) -> tuple[list[str], list[str]]:
    """Returns (differences, missing). Pure; unit-tested."""
    diff, missing = [], []
    for key, fp in sorted(before.items()):
        if key not in after:
            missing.append(key)
        elif fp != after[key]:
            diff.append(f"{key}: rows {fp['rows']} → {after[key]['rows']}"
                        + ("" if fp["rows"] != after[key]["rows"] else " (same rows, different values)"))
    return diff, missing


def _columns(bq, dataset: str, table: str) -> list[str]:
    from reconcile_ingest.engine import _q
    rows = _q(bq, f"""select column_name from `{bq.project}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
                      where table_name = @t order by ordinal_position""", t=table)
    return [r["column_name"] for r in rows
            if not r["column_name"].startswith("_") and r["column_name"].lower() not in VOLATILE]


def fingerprint(bq, dataset: str, table: str, day: dt.date, latest_batch: bool) -> dict | None:
    from reconcile_ingest.engine import _q
    cols = _columns(bq, dataset, table)
    if not cols or "business_date" not in cols:
        return None
    body = ", ".join(f"`{c}`" for c in cols)
    batch = (f"and _batch_id = (select max(_batch_id) from `{bq.project}.{dataset}.{table}` "
             f"where business_date = @d)") if latest_batch else ""
    r = _q(bq, f"""select count(*) n, cast(bit_xor(farm_fingerprint(to_json_string(struct({body})))) as string) h
                   from `{bq.project}.{dataset}.{table}` where business_date = @d {batch}""", d=day)[0]
    return {"rows": int(r["n"]), "hash": r["h"]}


def snapshot(bq, days: list[dt.date]) -> dict:
    from reconcile_ingest.datasets import DATASETS
    out: dict = {}
    for day in days:
        for ds in DATASETS:
            try:
                fp = fingerprint(bq, "raw", ds, day, latest_batch=True)
            except Exception:  # noqa: BLE001 — a dataset never loaded has no raw table
                fp = None
            if fp and fp["rows"]:
                out[f"raw.{ds}@{day}"] = fp
        for m in MARTS:
            try:
                fp = fingerprint(bq, "analytics", m, day, latest_batch=False)
            except Exception:  # noqa: BLE001
                fp = None
            if fp and fp["rows"]:
                out[f"analytics.{m}@{day}"] = fp
    return out


def replay(days: list[dt.date]) -> None:
    """Raw from the archive only (the source is never read), then one dbt build per day."""
    import subprocess

    from reconcile_ingest import pipeline
    from reconcile_ingest.datasets import DATASETS
    from reconcile_ingest.engine import _ingest_components
    s, wh, ar = _ingest_components()
    for day in days:
        for ds in DATASETS:
            try:
                r = pipeline.replay_batch(s, wh, ar, ds, day, run_id=f"rebuild_proof__{day}")
                print(f"  replayed {ds:14} {day}  batch {r.batch_id}  rows {r.rows_loaded}")
            except FileNotFoundError:
                print(f"  (no archived batch for {ds} {day})")
        conf = json.dumps({"business_date": str(day), "dbt_inject": []})
        subprocess.run(["airflow", "dags", "trigger", "dbt_build", "--conf", conf], check=True,
                       capture_output=True, text=True)
        print(f"  dbt_build triggered for {day} — wait for the gate to publish before the 'after' snapshot")


def main() -> int:
    from reconcile_ingest.engine import ENGINE, client_as
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("snapshot", "replay"):
        p = sub.add_parser(name)
        if name == "snapshot":
            p.add_argument("label")
        p.add_argument("--days", nargs=2, required=True, metavar=("FROM", "TO"))
    c = sub.add_parser("compare")
    c.add_argument("before")
    c.add_argument("after")
    a = ap.parse_args()
    if a.cmd == "snapshot":
        snap = snapshot(client_as(ENGINE), days_between(*a.days))
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        path = LOG_DIR / f"rebuild_{a.label}.json"
        path.write_text(json.dumps({"taken_at": dt.datetime.now(dt.timezone.utc).isoformat(), "days": a.days,
                                    "fingerprints": snap}, indent=1))
        print(f"{len(snap)} fingerprints → {path}")
        return 0
    if a.cmd == "replay":
        replay(days_between(*a.days))
        return 0
    b = json.loads((LOG_DIR / f"rebuild_{a.before}.json").read_text())["fingerprints"]
    f = json.loads((LOG_DIR / f"rebuild_{a.after}.json").read_text())["fingerprints"]
    diff, missing = compare(b, f)
    for line in diff:
        print(f"  ❌ {line}")
    for key in missing:
        print(f"  ❌ {key}: missing after the rebuild")
    same = len(b) - len(diff) - len(missing)
    print(f"Rebuild proof {'PASSED' if not diff and not missing and b else 'FAILED'}: "
          f"{same} of {len(b)} fingerprints identical (raw and published reports)")
    return 0 if not diff and not missing and b else 1


if __name__ == "__main__":
    sys.exit(main())
