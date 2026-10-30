"""Phase 6 Operations screen: read-only BigQuery views of breakers, agent calls, approval cards and freshness.

sa-api already reads ops (batches, breakers), publication and recovery (incidents, approvals, agent_calls).
The only write is the assistant's provenance row in recovery.agent_calls (table-level IAM, infra/assistant.tf),
exactly like the engine records its own agent calls. Nothing here reads raw data or patient rows.
"""
from __future__ import annotations

from typing import Protocol

from ..core.config import Settings


class OperationsRepository(Protocol):
    def open_breakers(self) -> list[dict]: ...
    def agent_calls(self, limit: int = 20) -> list[dict]: ...
    def approvals(self, limit: int = 30) -> list[dict]: ...
    def freshness(self) -> dict: ...
    def record_agent_call(self, row: dict) -> None: ...


class BigQueryOperationsRepository:
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
        except NotFound:   # the breaker / agents have not written anything yet
            return []

    def open_breakers(self) -> list[dict]:
        """Newest row per source system; only the OPEN ones (GLOBAL included)."""
        return [r for r in self._rows(f"""
            select * from `{self.p}.ops.breakers`
            qualify row_number() over (partition by source_system order by `at` desc) = 1""")
                if r.get("state") == "OPEN"]

    def agent_calls(self, limit: int = 20) -> list[dict]:
        # never the model output or the request payload: status, reason and timing only
        return self._rows(f"""
            select call_id, incident_id, agent_id, provider, model, prompt_version, validation, reason,
                   proposed_action, outcome, latency_ms, `at`
            from `{self.p}.recovery.agent_calls` order by `at` desc limit {int(limit)}""")

    def approvals(self, limit: int = 30) -> list[dict]:
        return self._rows(f"""
            select approval_id, incident_id, action, status, requested_at, expires_at, decided_by, `at`
            from `{self.p}.recovery.approvals`
            qualify row_number() over (partition by approval_id order by `at` desc) = 1
            order by requested_at desc limit {int(limit)}""")

    def freshness(self) -> dict:
        out = {}
        for key, sql in {
            "last_batch_at": f"select max(finished_at) t from `{self.p}.ops.batches`",
            "last_gate_at": f"select max(decided_at) t from `{self.p}.publication.publication_state`",
            "last_engine_at": f"select max(`at`) t from `{self.p}.recovery.incidents`",
        }.items():
            rows = self._rows(sql)
            out[key] = rows[0]["t"] if rows else None
        return out

    def record_agent_call(self, row: dict) -> None:
        errors = self.c.insert_rows_json(f"{self.p}.recovery.agent_calls", [row], row_ids=[row["call_id"]])
        if errors:
            raise RuntimeError(f"could not record the assistant call: {len(errors)} error(s)")
