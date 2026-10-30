"""CLI: python -m datagen {init,generate,archive,status}"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, timedelta

from . import generator

DEFAULT_DSN = "postgresql://reconcile:reconcile@localhost:5433/source_replica"


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="datagen", description="Synthetic EHR/LIS/Billing source generator")
    p.add_argument("--dsn", default=os.environ.get("SOURCE_DSN", DEFAULT_DSN))
    p.add_argument("--seed", type=int, default=int(os.environ.get("DATAGEN_SEED", generator.DEFAULT_SEED)))
    p.add_argument("--scale", type=float, default=float(os.environ.get("DATAGEN_SCALE", "1.0")),
                   help="Volume multiplier (1.0 = 10,000 encounters/day)")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("init", help="Create source schemas and the base patient population")
    g = sub.add_parser("generate", help="Generate one or more business dates (ending at --date)")
    g.add_argument("--date", type=date.fromisoformat, default=date.today() - timedelta(days=1))
    g.add_argument("--days", type=int, default=1)
    a = sub.add_parser("archive", help="Archive encounters admitted before a date (EHR retention)")
    a.add_argument("--before", type=date.fromisoformat, required=True)
    sub.add_parser("status", help="Show generated dates and row counts")

    args = p.parse_args(argv)
    if args.cmd == "init":
        n = generator.init(args.dsn, args.seed, args.scale)
        print(f"source schemas ready; base patients: {n:,}")
    elif args.cmd == "generate":
        generator.init(args.dsn, args.seed, args.scale)
        for i in range(args.days - 1, -1, -1):
            d = args.date - timedelta(days=i)
            res = generator.generate_day(args.dsn, d, args.seed, args.scale)
            print(f"{d}  " + "  ".join(f"{k}={v:,}" for k, v in res.counts.items()))
    elif args.cmd == "archive":
        moved = generator.archive(args.dsn, args.before)
        print(f"archived {moved:,} encounters admitted before {args.before}")
    elif args.cmd == "status":
        print(json.dumps(generator.status(args.dsn), indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
