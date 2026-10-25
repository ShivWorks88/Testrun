"""CLI: python -m reconcile_ingest {extract,replay,probe,verify,datasets}

Runs as the identity in IMPERSONATE_SERVICE_ACCOUNT (normally sa-ingest) on your own ADC login.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date, timedelta

from . import gcp, pipeline
from .archive import GcsArchive
from .datasets import DATASETS, SOURCE_SYSTEMS, datasets_for
from .settings import Settings


def _env():
    s = Settings.from_env()
    s.require_gcp()
    bq = gcp.bigquery_client(s.project, s.impersonate, s.location)
    st = gcp.storage_client(s.project, s.impersonate)
    from .warehouse import BigQueryWarehouse
    return s, bq, st, BigQueryWarehouse(bq, s.project, s.max_bytes_billed), GcsArchive(st, s.archive_bucket)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    yesterday = date.today() - timedelta(days=1)
    p = argparse.ArgumentParser(prog="reconcile_ingest")
    sub = p.add_subparsers(dest="cmd", required=True)

    e = sub.add_parser("extract", help="Extract, archive and load datasets for a business date")
    e.add_argument("--date", type=date.fromisoformat, default=yesterday)
    e.add_argument("--source", choices=list(SOURCE_SYSTEMS), help="All datasets of one source system")
    e.add_argument("--dataset", action="append", choices=list(DATASETS), help="Repeatable")
    e.add_argument("--inject", action="append", default=[], choices=list(pipeline.INJECTIONS))

    r = sub.add_parser("replay", help="Reload raw from an archived batch (no source access)")
    r.add_argument("--dataset", required=True, choices=list(DATASETS))
    r.add_argument("--date", type=date.fromisoformat, required=True)
    r.add_argument("--batch-id")

    pr = sub.add_parser("probe", help="Record source health for each source system")
    pr.add_argument("--source", choices=list(SOURCE_SYSTEMS))

    v = sub.add_parser("verify", help="Run the Phase 1 exit-gate checks for a business date")
    v.add_argument("--date", type=date.fromisoformat, default=yesterday)
    v.add_argument("--rerun-dataset", default="lab_orders", choices=list(DATASETS))

    sub.add_parser("datasets", help="Print the dataset registry")
    args = p.parse_args(argv)

    if args.cmd == "datasets":
        print(json.dumps({n: {"source": d.source_system, "table": d.table, "key": d.key,
                              "business_date_from": d.event_column, "amount": d.amount_column}
                          for n, d in DATASETS.items()}, indent=2))
        return 0

    s, bq, st, wh, ar = _env()
    if args.cmd == "extract":
        names = args.dataset or ([d.name for d in datasets_for(args.source)] if args.source else list(DATASETS))
        for n in names:
            res = pipeline.ingest_dataset(s, wh, ar, n, args.date, args.inject, run_id="cli")
            print(json.dumps(res.as_dict()))
    elif args.cmd == "replay":
        print(json.dumps(pipeline.replay_batch(s, wh, ar, args.dataset, args.date, args.batch_id, "cli").as_dict()))
    elif args.cmd == "probe":
        for system in [args.source] if args.source else list(SOURCE_SYSTEMS):
            print(system, json.dumps(pipeline.probe_source(s, wh, system), default=str))
    elif args.cmd == "verify":
        from . import verify
        checks = verify.run(s, bq, wh, ar, st, args.date, args.rerun_dataset)
        width = max(len(c.name) for c in checks)
        for c in checks:
            print(f"{c.gate}  {'PASS' if c.passed else 'FAIL'}  {c.name:<{width}}  {c.detail}")
        return 0 if all(c.passed for c in checks) else 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
