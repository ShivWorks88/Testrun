"""Read-only BigQuery queries for the console. The API identity can read ops and analytics only."""
from __future__ import annotations

from datetime import date
from typing import Protocol

from ..core.config import Settings


class OpsRepository(Protocol):
    def latest_health(self) -> list[dict]: ...
    def source_activity(self) -> list[dict]: ...
    def batches(self, business_date: date) -> list[dict]: ...
    def available_dates(self, limit: int = 14) -> list[date]: ...
    def latest_loaded(self) -> list[dict]: ...


class BigQueryOpsRepository:
    def __init__(self, client, settings: Settings):
        self.c = client
        self.p = settings.gcp_project
        self.max_bytes = settings.bq_max_bytes_billed

    def _rows(self, sql: str, params: list | None = None) -> list[dict]:
        from google.cloud import bigquery
        cfg = bigquery.QueryJobConfig(query_parameters=params or [], maximum_bytes_billed=self.max_bytes,
                                      labels={"component": "api"}, use_query_cache=True)
        return [dict(r.items()) for r in self.c.query(sql, job_config=cfg).result()]

    def latest_health(self) -> list[dict]:
        return self._rows(f"""
            select * except (rn) from (
              select *, row_number() over (partition by source_system order by probed_at desc) rn
              from `{self.p}.ops.source_health`
              where probed_at > timestamp_sub(current_timestamp(), interval 30 day))
            where rn = 1""")

    def source_activity(self) -> list[dict]:
        return self._rows(f"""
            with latest as (
              select source_system, max(business_date) last_business_date, max(finished_at) last_batch_at
              from `{self.p}.ops.batches` where status = 'LOADED' group by 1)
            select l.*, (select count(distinct dataset) from `{self.p}.ops.batches` b
                         where b.source_system = l.source_system and b.business_date = l.last_business_date
                           and b.status = 'LOADED') datasets_loaded
            from latest l""")

    def batches(self, business_date: date) -> list[dict]:
        from google.cloud import bigquery
        return self._rows(f"""
            with b as (
              select * except (rn) from (
                select *, row_number() over (partition by dataset order by finished_at desc) rn
                from `{self.p}.ops.batches` where business_date = @d)
              where rn = 1),
            t as (
              select batch_id, count(*) totals_rows, any_value(row_count) control_total_rows,
                     any_value(distinct_keys) distinct_keys, any_value(amount_total) amount_total
              from `{self.p}.ops.control_totals` where business_date = @d group by batch_id)
            select b.batch_id, b.dataset, b.source_system, b.business_date, b.mode, b.status, b.rows_extracted,
                   b.rows_loaded, t.control_total_rows, t.distinct_keys, t.amount_total,
                   coalesce(t.totals_rows, 0) totals_rows, b.archive_uri, b.injections, b.finished_at, b.error
            from b left join t using (batch_id)
            order by b.source_system, b.dataset""",
            [bigquery.ScalarQueryParameter("d", "DATE", business_date)])

    def available_dates(self, limit: int = 14) -> list[date]:
        rows = self._rows(f"""
            select distinct business_date from `{self.p}.ops.batches`
            where business_date > date_sub(current_date(), interval 120 day)
            order by business_date desc limit {int(limit)}""")
        return [r["business_date"] for r in rows]

    def latest_loaded(self) -> list[dict]:
        return self._rows(f"""
            select dataset, source_system, business_date, rows_loaded, finished_at from (
              select *, row_number() over (partition by dataset order by business_date desc, finished_at desc) rn
              from `{self.p}.ops.batches` where status = 'LOADED' and mode = 'extract')
            where rn = 1""")
