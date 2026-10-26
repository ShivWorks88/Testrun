"""Approval cards (engine-written) and data-owner decisions (console-written).

sa-api reads recovery.approvals and recovery.approval_decisions, and may INSERT into
recovery.approval_decisions only (table-level IAM in infra/approvals.tf). It never acts:
the approval_worker DAG re-checks every decision and runs the fix as sa-engine / sa-ingest."""
from __future__ import annotations

from typing import Protocol

from ..core.config import Settings


class ApprovalsRepository(Protocol):
    def approvals(self, limit: int = 100) -> list[dict]: ...
    def approval(self, approval_id: str) -> dict | None: ...
    def decision(self, approval_id: str) -> dict | None: ...
    def decisions(self, approval_ids: list[str]) -> dict[str, dict]: ...
    def insert_decision(self, row: dict) -> None: ...


class BigQueryApprovalsRepository:
    def __init__(self, client, settings: Settings):
        self.c = client
        self.p = settings.gcp_project
        self.max_bytes = settings.bq_max_bytes_billed

    def _rows(self, sql: str, params: list | None = None) -> list[dict]:
        from google.api_core.exceptions import NotFound
        from google.cloud import bigquery
        cfg = bigquery.QueryJobConfig(query_parameters=params or [], maximum_bytes_billed=self.max_bytes,
                                      labels={"component": "api"}, use_query_cache=False)
        try:
            return [dict(r.items()) for r in self.c.query(sql, job_config=cfg).result()]
        except NotFound:  # the engine has not written a card yet / Terraform not applied yet
            return []

    def approvals(self, limit: int = 100) -> list[dict]:
        return self._rows(f"""
            select * from `{self.p}.recovery.approvals`
            qualify row_number() over (partition by approval_id order by `at` desc) = 1
            order by requested_at desc limit {int(limit)}""")

    def approval(self, approval_id: str) -> dict | None:
        from google.cloud import bigquery
        rows = self._rows(f"""
            select * from `{self.p}.recovery.approvals` where approval_id = @a
            qualify row_number() over (order by `at` desc) = 1""",
            [bigquery.ScalarQueryParameter("a", "STRING", approval_id)])
        return rows[0] if rows else None

    def decisions(self, approval_ids: list[str]) -> dict[str, dict]:
        from google.cloud import bigquery
        if not approval_ids:
            return {}
        rows = self._rows(f"""
            select * from `{self.p}.recovery.approval_decisions` where approval_id in unnest(@ids)
            qualify row_number() over (partition by approval_id order by decided_at) = 1""",
            [bigquery.ArrayQueryParameter("ids", "STRING", approval_ids)])
        return {r["approval_id"]: r for r in rows}

    def decision(self, approval_id: str) -> dict | None:
        return self.decisions([approval_id]).get(approval_id)

    def insert_decision(self, row: dict) -> None:
        errors = self.c.insert_rows_json(f"{self.p}.recovery.approval_decisions", [row],
                                         row_ids=[row["decision_id"]])
        if errors:
            raise RuntimeError(f"could not record the decision: {errors}")
