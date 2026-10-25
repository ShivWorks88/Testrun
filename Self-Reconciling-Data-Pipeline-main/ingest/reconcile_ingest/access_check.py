"""Phases 3–4 — which identity may read / write which dataset?

Creates and immediately deletes a tiny `_access_check` table; changes nothing else.
    python -m reconcile_ingest.access_check
"""
from __future__ import annotations

from google.cloud import bigquery

from reconcile_ingest.publish import client_as

CHECKS = [
    ("publish", "read", "audit", "ALLOWED"),
    ("publish", "write", "publication", "ALLOWED"),
    ("publish", "write", "ops", "DENIED"),     # publish must not touch ingest receipts
    ("publish", "write", "analytics", "ALLOWED"),
    ("dbt", "write", "analytics", "DENIED"),   # T9: only sa-publish may write analytics
    ("dbt", "write", "audit", "ALLOWED"),
    # Phase 4 — the recovery engine reads the evidence and keeps its incident log
    ("engine", "read", "ops", "ALLOWED"),
    ("engine", "read", "raw", "ALLOWED"),
    ("engine", "read", "audit", "ALLOWED"),
    ("engine", "read", "publication", "ALLOWED"),
    ("engine", "write", "ops", "ALLOWED"),
    ("engine", "write", "analytics", "DENIED"),
]


def _try(sa: str, action: str, dataset: str) -> tuple[bool, str]:
    try:
        bq = client_as(sa)
        ds = f"{bq.project}.{dataset}"
        if action == "read":
            tables = list(bq.list_tables(ds, max_results=1))
            if not tables:
                return True, "dataset readable (no tables yet)"
            t = tables[0]
            bq.query(f"select * from `{t.project}.{t.dataset_id}.{t.table_id}` limit 1",
                     job_config=bigquery.QueryJobConfig(dry_run=True))
            return True, f"can query {t.table_id}"
        table_id = f"{ds}._access_check_{sa}"
        bq.create_table(bigquery.Table(table_id, schema=[bigquery.SchemaField("x", "INT64")]))
        bq.delete_table(table_id, not_found_ok=True)
        return True, "created and deleted a test table"
    except Exception as exc:  # noqa: BLE001 — we want the reason, whatever it is
        return False, str(exc).splitlines()[0][:140]


def main() -> None:
    ok_all = True
    for sa, action, dataset, expected in CHECKS:
        allowed, detail = _try(sa, action, dataset)
        got = "ALLOWED" if allowed else "DENIED"
        mark = "OK  " if got == expected else "FIX "
        ok_all &= got == expected
        print(f"{mark} sa-{sa:8} {action:5} {dataset:12} {got:8} (expected {expected})  {detail}")
    print("\nAll access rules as expected." if ok_all else "\nSome rules differ — send this output.")


if __name__ == "__main__":
    main()
