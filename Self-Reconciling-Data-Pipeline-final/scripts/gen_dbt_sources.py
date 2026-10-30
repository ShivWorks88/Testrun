#!/usr/bin/env python3
"""Generate dbt/reconcile/models/sources/_sources.yml from the ingest dataset registry,
so dbt sources and ingest never drift apart. CI fails if the committed file is stale (--check)."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "ingest"))

from reconcile_ingest.datasets import DATASETS, SOURCE_SYSTEMS  # noqa: E402

OUT = ROOT / "dbt" / "reconcile" / "models" / "sources" / "_sources.yml"


def render() -> str:
    L: list[str] = []
    w = L.append
    w("# Generated from ingest/reconcile_ingest/datasets.py by scripts/gen_dbt_sources.py. Do not edit by hand.")
    w("version: 2")
    w("")
    w("sources:")
    w("  - name: raw")
    w("    description: Raw extracts, one partition per facility-local business date. Written by ingest only.")
    w("    schema: raw")
    w("    config:")
    w("      loaded_at_field: _loaded_at")
    w("      freshness:")
    w("        warn_after: {count: 26, period: hour}")
    w("        error_after: {count: 50, period: hour}")
    w("    tables:")
    for n, d in DATASETS.items():
        w(f"      - name: {n}")
        w(f"        description: \"{SOURCE_SYSTEMS[d.source_system]['label']} {d.table}; "
          f"business date from {d.event_column}\"")
        w("        config:")
        w("          meta:")
        w(f"            source_system: {d.source_system}")
        w(f"            criticality: {d.criticality}")
        w(f"            phi: {str(d.phi).lower()}")
        w(f"            key: {d.key}")
        if d.critical_fields:
            w(f"            critical_fields: [{', '.join(d.critical_fields)}]")
        w("        columns:")
        for col in (d.key, "_batch_id"):
            w(f"          - name: {col}")
            w("            data_tests:")
            w("              - not_null:")
            w("                  config: {meta: {policy_outcome: blocking}}")
    w("  - name: ops")
    w("    description: Pipeline metadata written by ingest. No clinical values.")
    w("    schema: ops")
    w("    tables:")
    w("      - name: control_totals")
    w("        description: Totals each extract computed in the same snapshot as its rows. "
      "One row per dataset, business date and batch.")
    w("        config:")
    w("          loaded_at_field: extracted_at")
    w("          freshness:")
    w("            warn_after: {count: 26, period: hour}")
    w("            error_after: {count: 50, period: hour}")
    w("      - name: batches")
    w("        description: Append-only log of every extract and replay outcome.")
    w("      - name: source_health")
    w("        description: Source probes written at the start of every ingest run.")
    return "\n".join(L) + "\n"


def main() -> int:
    text = render()
    if "--check" in sys.argv:
        if not OUT.exists() or OUT.read_text() != text:
            print("dbt sources are stale: run python scripts/gen_dbt_sources.py")
            return 1
        print("dbt sources up to date")
        return 0
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(text)
    print(f"wrote {OUT.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
