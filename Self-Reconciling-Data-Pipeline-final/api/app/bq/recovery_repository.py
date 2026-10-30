"""Read-only BigQuery views of the publication gate (Phase 3) and the recovery engine (Phase 4).

sa-api reads `ops` (receipts, batches), `publication` (gate decisions, check results) and
`recovery` (incident journal). It never reads raw or evidence_restricted, and never writes."""
from __future__ import annotations

from datetime import date
from typing import Protocol

from ..core.config import Settings

LOOKBACK_DAYS = 3  # dbt rebuilds and the gate decides bd-2 .. bd together


class RecoveryRepository(Protocol):
    def recon_batches(self, d: date) -> list[dict]: ...
    def recon_checks(self, d: date) -> list[dict]: ...
    def publication_cells(self, n_dates: int = 14) -> list[dict]: ...
    def publication_log(self, limit: int = 40) -> list[dict]: ...
    def incident_events(self, incident_id: str | None = None, limit: int = 2000) -> list[dict]: ...


class BigQueryRecoveryRepository:
    def __init__(self, client, settings: Settings):
        self.c = client
        self.p = settings.gcp_project
        self.max_bytes = settings.bq_max_bytes_billed

    def _rows(self, sql: str, params: list | None = None) -> list[dict]:
        from google.api_core.exceptions import NotFound
        from google.cloud import bigquery
        cfg = bigquery.QueryJobConfig(query_parameters=params or [], maximum_bytes_billed=self.max_bytes,
                                      labels={"component": "api"}, use_query_cache=True)
        try:
            return [dict(r.items()) for r in self.c.query(sql, job_config=cfg).result()]
        except NotFound:  # the gate / engine has not written anything yet
            return []

    def recon_batches(self, d: date) -> list[dict]:
        """Latest batch per dataset for the day, with its receipt (same batch)."""
        from google.cloud import bigquery
        return self._rows(f"""
            with b as (
              select * from `{self.p}.ops.batches` where business_date = @d
              qualify row_number() over (partition by dataset order by finished_at desc) = 1),
            r as (
              select * from `{self.p}.ops.control_totals` where business_date = @d
              qualify row_number() over (partition by dataset order by extracted_at desc) = 1)
            select b.dataset, b.source_system, b.batch_id, b.status, b.mode, b.rows_loaded,
                   if(r.batch_id = b.batch_id, r.row_count, null)     as receipt_rows,
                   if(r.batch_id = b.batch_id, r.distinct_keys, null) as receipt_keys,
                   if(r.batch_id = b.batch_id, r.amount_total, null)  as receipt_amount,
                   b.injections, b.finished_at
            from b left join r using (dataset)
            order by b.source_system, b.dataset""",
            [bigquery.ScalarQueryParameter("d", "DATE", d)])

    def recon_checks(self, d: date) -> list[dict]:
        """Check results of the newest gate run whose 3-day window covers the day."""
        from google.cloud import bigquery
        return self._rows(f"""
            with run as (
              select gate_run_id from `{self.p}.publication.check_results`
              where business_date between @d and date_add(@d, interval {LOOKBACK_DAYS - 1} day)
              order by recorded_at desc limit 1)
            select c.* from `{self.p}.publication.check_results` c join run using (gate_run_id)
            order by c.resource_type, c.unique_id""",
            [bigquery.ScalarQueryParameter("d", "DATE", d)])

    def publication_cells(self, n_dates: int = 14) -> list[dict]:
        return self._rows(f"""
            with d as (
              select distinct business_date from `{self.p}.publication.publication_state`
              order by business_date desc limit {int(n_dates)})
            select dataset, business_date, state, reasons, decided_at, snapshot
            from `{self.p}.publication.publication_state`
            where business_date in (select business_date from d)
            qualify row_number() over (partition by dataset, business_date order by decided_at desc) = 1""")

    def publication_log(self, limit: int = 40) -> list[dict]:
        return self._rows(f"""
            select dataset, business_date, state, reasons, decided_at, snapshot, gate_run_id, actor
            from `{self.p}.publication.publication_state`
            order by decided_at desc limit {int(limit)}""")

    def incident_events(self, incident_id: str | None = None, limit: int = 2000) -> list[dict]:
        from google.cloud import bigquery
        where, params = "", []
        if incident_id:
            where, params = "where incident_id = @i", [bigquery.ScalarQueryParameter("i", "STRING", incident_id)]
        return self._rows(f"""
            select * from `{self.p}.recovery.incidents` {where}
            order by `at` desc limit {int(limit)}""", params)
