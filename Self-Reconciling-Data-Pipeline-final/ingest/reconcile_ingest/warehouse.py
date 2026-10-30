"""Raw-layer loading in BigQuery.

A partition load is one BigQuery multi-statement transaction:
    DELETE the business date from raw.<dataset>
    INSERT the staged batch
    INSERT its control-totals row        (extract mode only)
    INSERT the batch outcome
so re-running a date never double-loads, and the totals row exists if and only if the rows do.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Protocol

import pyarrow as pa

from .datasets import Dataset
from .extract import ControlTotals


@dataclass
class BatchRecord:
    batch_id: str
    dataset: str
    source_system: str
    business_date: date
    mode: str                 # extract | replay
    status: str               # LOADED | FAILED
    started_at: datetime
    finished_at: datetime
    rows_extracted: int | None = None
    rows_loaded: int | None = None
    control_totals_written: bool | None = None
    archive_uri: str | None = None
    injections: list[str] = field(default_factory=list)
    error: str | None = None
    orchestrator_run_id: str | None = None


class Warehouse(Protocol):
    def ensure_raw_table(self, ds: Dataset) -> None: ...
    def next_batch_seq(self, dataset: str, business_date: date) -> int: ...
    def load_partition(self, ds: Dataset, business_date: date, table: pa.Table, archive_uri: str,
                       totals: ControlTotals | None, record: BatchRecord) -> int: ...
    def record_batch(self, record: BatchRecord) -> None: ...
    def record_source_health(self, source_system: str, probe: dict) -> None: ...


def _sanitize(s: str) -> str:
    return "".join(ch if ch.isalnum() else "_" for ch in s)


class BigQueryWarehouse:
    def __init__(self, client, project: str, max_bytes_billed: int, raw_dataset: str = "raw", ops_dataset: str = "ops"):
        self.client = client
        self.project = project
        self.raw = raw_dataset
        self.ops = ops_dataset
        self.max_bytes = max_bytes_billed

    # ------------------------------------------------------------------ helpers
    def _fq(self, dataset: str, table: str) -> str:
        return f"`{self.project}.{dataset}.{table}`"

    def _query(self, sql: str, params: list | None = None):
        from google.cloud import bigquery
        cfg = bigquery.QueryJobConfig(query_parameters=params or [], maximum_bytes_billed=self.max_bytes,
                                      labels={"component": "ingest"})
        return self.client.query(sql, job_config=cfg).result()

    # ------------------------------------------------------------------ DDL
    def ensure_raw_table(self, ds: Dataset) -> None:
        from google.cloud import bigquery
        schema = [bigquery.SchemaField(f["name"], f["type"], mode=f["mode"]) for f in ds.bq_schema()]
        table = bigquery.Table(f"{self.project}.{self.raw}.{ds.name}", schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY,
                                                            field="business_date")
        table.clustering_fields = [ds.key]
        table.description = f"Raw {ds.source_system} {ds.name}. Written by ingest only; mirrors the archive."
        table.labels = {"source_system": ds.source_system.lower(), "criticality": ds.criticality,
                        "phi": str(ds.phi).lower()}
        self.client.create_table(table, exists_ok=True)

    # ------------------------------------------------------------------ batches
    def next_batch_seq(self, dataset: str, business_date: date) -> int:
        from google.cloud import bigquery
        rows = list(self._query(
            f"select count(distinct batch_id) n from {self._fq(self.ops, 'batches')} "
            f"where dataset = @dataset and business_date = @d and mode = 'extract'",
            [bigquery.ScalarQueryParameter("dataset", "STRING", dataset),
             bigquery.ScalarQueryParameter("d", "DATE", business_date)]))
        return int(rows[0].n) + 1

    def _batch_insert_sql(self) -> str:
        return (
            f"INSERT INTO {self._fq(self.ops, 'batches')} (batch_id, dataset, source_system, business_date, mode, "
            "status, rows_extracted, rows_loaded, control_totals_written, archive_uri, injections, started_at, "
            "finished_at, error, orchestrator_run_id) VALUES (@b_batch_id, @b_dataset, @b_source_system, "
            "@b_business_date, @b_mode, @b_status, @b_rows_extracted, @b_rows_loaded, @b_ct_written, "
            "@b_archive_uri, @b_injections, @b_started_at, @b_finished_at, @b_error, @b_run_id);"
        )

    def _batch_params(self, r: BatchRecord) -> list:
        from google.cloud import bigquery as bq
        S = bq.ScalarQueryParameter
        return [
            S("b_batch_id", "STRING", r.batch_id), S("b_dataset", "STRING", r.dataset),
            S("b_source_system", "STRING", r.source_system), S("b_business_date", "DATE", r.business_date),
            S("b_mode", "STRING", r.mode), S("b_status", "STRING", r.status),
            S("b_rows_extracted", "INT64", r.rows_extracted), S("b_rows_loaded", "INT64", r.rows_loaded),
            S("b_ct_written", "BOOL", r.control_totals_written), S("b_archive_uri", "STRING", r.archive_uri),
            bq.ArrayQueryParameter("b_injections", "STRING", r.injections),
            S("b_started_at", "TIMESTAMP", r.started_at), S("b_finished_at", "TIMESTAMP", r.finished_at),
            S("b_error", "STRING", r.error), S("b_run_id", "STRING", r.orchestrator_run_id),
        ]

    def record_batch(self, record: BatchRecord) -> None:
        self._query(self._batch_insert_sql(), self._batch_params(record))

    # ------------------------------------------------------------------ load
    def load_partition(self, ds: Dataset, business_date: date, table: pa.Table, archive_uri: str,
                       totals: ControlTotals | None, record: BatchRecord) -> int:
        from google.cloud import bigquery as bq
        S = bq.ScalarQueryParameter

        stage_id = f"{self.project}.{self.raw}._stage_{ds.name}_{_sanitize(record.batch_id)}"
        schema = [bq.SchemaField(f["name"], f["type"], mode=f["mode"]) for f in ds.bq_schema()]
        load_cfg = bq.LoadJobConfig(source_format=bq.SourceFormat.PARQUET, schema=schema,
                                    write_disposition=bq.WriteDisposition.WRITE_TRUNCATE,
                                    labels={"component": "ingest"})
        # Load the archived object itself, so raw is exactly what the archive holds.
        job = self.client.load_table_from_uri(archive_uri, stage_id, job_config=load_cfg)
        job.result()
        stage = self.client.get_table(stage_id)
        stage.expires = datetime.now(timezone.utc) + timedelta(days=1)
        self.client.update_table(stage, ["expires"])
        rows_loaded = int(stage.num_rows)

        record.rows_loaded = rows_loaded
        cols = ", ".join(f"`{c['name']}`" for c in ds.bq_schema())
        stmts = [
            "BEGIN TRANSACTION;",
            f"DELETE FROM {self._fq(self.raw, ds.name)} WHERE business_date = @business_date;",
            f"INSERT INTO {self._fq(self.raw, ds.name)} ({cols}) SELECT {cols} FROM `{stage_id}`;",
        ]
        params = [S("business_date", "DATE", business_date)]
        if totals is not None:
            stmts.append(
                f"INSERT INTO {self._fq(self.ops, 'control_totals')} (dataset, source_system, business_date, "
                "batch_id, row_count, distinct_keys, null_amount_count, amount_total, key_column, amount_column, "
                "snapshot_ts, extracted_at, archive_uri, extractor_version) VALUES (@t_dataset, @t_source_system, "
                "@business_date, @t_batch_id, @t_row_count, @t_distinct_keys, @t_null_amount_count, "
                "@t_amount_total, @t_key_column, @t_amount_column, @t_snapshot_ts, @t_extracted_at, "
                "@t_archive_uri, @t_extractor_version);")
            params += [
                S("t_dataset", "STRING", totals.dataset), S("t_source_system", "STRING", totals.source_system),
                S("t_batch_id", "STRING", totals.batch_id), S("t_row_count", "INT64", totals.row_count),
                S("t_distinct_keys", "INT64", totals.distinct_keys),
                S("t_null_amount_count", "INT64", totals.null_amount_count),
                S("t_amount_total", "NUMERIC", totals.amount_total), S("t_key_column", "STRING", totals.key_column),
                S("t_amount_column", "STRING", totals.amount_column),
                S("t_snapshot_ts", "TIMESTAMP", totals.snapshot_ts),
                S("t_extracted_at", "TIMESTAMP", totals.extracted_at),
                S("t_archive_uri", "STRING", totals.archive_uri),
                S("t_extractor_version", "STRING", totals.extractor_version),
            ]
        record.finished_at = datetime.now(timezone.utc)
        stmts.append(self._batch_insert_sql())
        params += self._batch_params(record)
        stmts.append("COMMIT TRANSACTION;")
        self._query("\n".join(stmts), params)
        self.client.delete_table(stage_id, not_found_ok=True)
        return rows_loaded

    # ------------------------------------------------------------------ health
    def record_source_health(self, source_system: str, probe: dict) -> None:
        from google.cloud import bigquery as bq
        S = bq.ScalarQueryParameter
        self._query(
            f"INSERT INTO {self._fq(self.ops, 'source_health')} (source_system, probed_at, reachable, latency_ms, "
            "replica_lag_seconds, archive_cutoff_date, error) VALUES (@s, CURRENT_TIMESTAMP(), @r, @l, @lag, @c, @e)",
            [S("s", "STRING", source_system), S("r", "BOOL", probe["reachable"]),
             S("l", "FLOAT64", probe["latency_ms"]), S("lag", "FLOAT64", probe["replica_lag_seconds"]),
             S("c", "DATE", probe["archive_cutoff_date"]), S("e", "STRING", probe["error"])])


class InMemoryWarehouse:
    """Test double with the same semantics (delete + insert per business date, one totals row per batch)."""

    def __init__(self):
        self.raw: dict[str, dict[date, pa.Table]] = {}
        self.control_totals: list[ControlTotals] = []
        self.batches: list[BatchRecord] = []
        self.health: list[tuple[str, dict]] = []
        self.archive = None  # set by tests to resolve archive URIs

    def ensure_raw_table(self, ds: Dataset) -> None:
        self.raw.setdefault(ds.name, {})

    def next_batch_seq(self, dataset: str, business_date: date) -> int:
        return len({b.batch_id for b in self.batches
                    if b.dataset == dataset and b.business_date == business_date and b.mode == "extract"}) + 1

    def load_partition(self, ds, business_date, table, archive_uri, totals, record) -> int:
        self.raw.setdefault(ds.name, {})[business_date] = table      # delete + insert
        if totals is not None:
            self.control_totals.append(totals)
        record.rows_loaded = table.num_rows
        record.finished_at = datetime.now(timezone.utc)
        self.batches.append(record)
        return table.num_rows

    def record_batch(self, record: BatchRecord) -> None:
        self.batches.append(record)

    def record_source_health(self, source_system: str, probe: dict) -> None:
        self.health.append((source_system, probe))
