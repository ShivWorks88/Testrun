"""Ingest steps used by the Airflow DAGs and the CLI.

    ingest_dataset : snapshot extract -> control totals -> archive (write-once) -> idempotent raw load
    replay_batch   : archive -> raw (no source access), for recovery after the source has moved on
    probe_source   : source health row for the console
"""
from __future__ import annotations

import json
import logging
import random
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal

import pyarrow as pa

from . import archive as arc
from .datasets import DATASETS, Dataset
from .extract import extract, probe
from .settings import EXTRACTOR_VERSION, Settings
from .warehouse import BatchRecord, Warehouse

log = logging.getLogger("reconcile_ingest")

# Failure injectors (test project only, ALLOW_INJECTIONS=true). Each simulates a real defect
# AFTER the control totals were taken, so Phase 2 reconciliation must catch it.
INJECTIONS = {
    "drop_hour": "Drop rows whose event falls in 02:00-03:00 facility time (partial extract, T3)",
    "duplicate_rows": "Append 12 duplicated rows, as a retried batch would (T4)",
    "missing_control_totals": "Do not write the control-totals row (T2)",
    "offsetting_errors": "Drop 50 rows and load 50 others twice, so the row count still matches (T5)",
    "unit_change": "Lab results: glucose (GLU) arrives in mmol/L instead of mg/dL, value / 18.016 (T11)",
}

GLUCOSE_MG_DL_PER_MMOL_L = 18.016


@dataclass
class IngestResult:
    dataset: str
    business_date: str
    batch_id: str
    mode: str
    rows_extracted: int
    rows_loaded: int
    control_totals_written: bool
    archive_uri: str
    injections: list[str]

    def as_dict(self) -> dict:
        return self.__dict__.copy()


def batch_id_for(business_date: date, dataset: str, seq: int) -> str:
    return f"b-{business_date:%Y%m%d}-{dataset}-{seq:02d}"


def _with_new_row_ids(rows: pa.Table, batch_id: str, label: str) -> pa.Table:
    """Give injected copies their own _row_id, as a real retried load would.
    Keeps the original _row_id field definition so the copies can be appended to the table."""
    idx = rows.schema.get_field_index("_row_id")
    field = rows.schema.field(idx)
    row_ids = pa.array([f"{batch_id}-{label}-{i:07d}" for i in range(rows.num_rows)], type=field.type)
    return rows.set_column(idx, field, row_ids)


def _apply_injections(ds: Dataset, table: pa.Table, injections: list[str], tz, batch_id: str) -> pa.Table:
    if "drop_hour" in injections:
        hours = [None if v is None else v.astimezone(tz).hour for v in table[ds.event_column].to_pylist()]
        keep = pa.array([h != 2 for h in hours])
        table = table.filter(keep)

    if "duplicate_rows" in injections and table.num_rows:
        rng = random.Random(batch_id)
        idx = sorted(rng.sample(range(table.num_rows), min(12, table.num_rows)))
        table = pa.concat_tables([table, _with_new_row_ids(table.take(pa.array(idx)), batch_id, "retry")])

    if "offsetting_errors" in injections and table.num_rows >= 100:
        # 50 rows lost + 50 other rows loaded twice: row count unchanged, distinct keys and amount are not.
        rng = random.Random(f"{batch_id}-offset")
        picked = rng.sample(range(table.num_rows), 100)
        dropped, doubled = set(picked[:50]), sorted(picked[50:])
        kept = table.filter(pa.array([i not in dropped for i in range(table.num_rows)]))
        table = pa.concat_tables([kept, _with_new_row_ids(table.take(pa.array(doubled)), batch_id, "dup")])

    if "unit_change" in injections and "test_code" in table.column_names:
        # The lab system switches glucose to mmol/L: same rows and counts, values ~18x smaller.
        value_type = table.schema.field("result_value").type
        unit_type = table.schema.field("result_unit").type
        codes = table["test_code"].to_pylist()
        values = table["result_value"].to_pylist()
        units = table["result_unit"].to_pylist()
        new_values, new_units = [], []
        for code, value, unit in zip(codes, values, units, strict=False):
            if code == "GLU":
                if value is not None:
                    value = round(float(value) / GLUCOSE_MG_DL_PER_MMOL_L, 3)
                    if pa.types.is_decimal(value_type):
                        value = Decimal(str(value))
                unit = "mmol/L"
            new_values.append(value)
            new_units.append(unit)
        table = table.set_column(table.schema.get_field_index("result_value"),
                                 table.schema.field("result_value"), pa.array(new_values, type=value_type))
        table = table.set_column(table.schema.get_field_index("result_unit"),
                                 table.schema.field("result_unit"), pa.array(new_units, type=unit_type))
    return table


def ingest_dataset(settings: Settings, warehouse: Warehouse, archive: arc.Archive, dataset: str,
                   business_date: date, injections: list[str] | None = None,
                   run_id: str | None = None) -> IngestResult:
    ds = DATASETS[dataset]
    injections = sorted(set(injections or []))
    unknown = set(injections) - set(INJECTIONS)
    if unknown:
        raise ValueError(f"unknown injections: {sorted(unknown)}")
    if injections and not settings.allow_injections:
        raise PermissionError("failure injection is disabled (set ALLOW_INJECTIONS=true in the test project only)")

    started = datetime.now(timezone.utc)
    warehouse.ensure_raw_table(ds)
    batch_id = batch_id_for(business_date, ds.name, warehouse.next_batch_seq(ds.name, business_date))
    record = BatchRecord(batch_id=batch_id, dataset=ds.name, source_system=ds.source_system,
                         business_date=business_date, mode="extract", status="FAILED", started_at=started,
                         finished_at=started, injections=injections, orchestrator_run_id=run_id)
    try:
        ex = extract(settings.source_dsn, ds, business_date, batch_id, settings.facility_tz, loaded_at=started)
        table = _apply_injections(ds, ex.table, injections, settings.facility_tz, batch_id)
        totals = ex.totals
        totals.extractor_version = EXTRACTOR_VERSION
        manifest = {"batch_id": batch_id, "dataset": ds.name, "business_date": business_date.isoformat(),
                    "control_totals": totals.as_json(), "extractor_version": EXTRACTOR_VERSION,
                    "injections": injections}
        parquet_uri, _ = arc.write_batch(archive, table, manifest, ds.name, business_date, batch_id)
        totals.archive_uri = parquet_uri

        write_totals = "missing_control_totals" not in injections
        record.status = "LOADED"
        record.rows_extracted = ex.totals.row_count
        record.archive_uri = parquet_uri
        record.control_totals_written = write_totals
        rows = warehouse.load_partition(ds, business_date, table, parquet_uri,
                                        totals if write_totals else None, record)
        log.info("loaded %s %s batch=%s rows=%s totals=%s", ds.name, business_date, batch_id, rows,
                 totals.row_count)
        return IngestResult(ds.name, business_date.isoformat(), batch_id, "extract", ex.totals.row_count, rows,
                            write_totals, parquet_uri, injections)
    except Exception as exc:
        record.status = "FAILED"
        record.error = f"{type(exc).__name__}: {exc}"[:2000]
        record.finished_at = datetime.now(timezone.utc)
        try:
            warehouse.record_batch(record)
        except Exception:  # noqa: BLE001 - never hide the original failure
            log.exception("could not record failed batch %s", batch_id)
        raise

def replay_batch(settings: Settings, warehouse: Warehouse, archive: arc.Archive, dataset: str,
                 business_date: date, batch_id: str | None = None, run_id: str | None = None) -> IngestResult:
    """Reload raw for one business date from an archived batch. The source is not touched, so this
    works even after the source has archived or changed the records."""
    ds = DATASETS[dataset]
    batch_id = batch_id or arc.latest_batch_id(archive, ds.name, business_date)
    if not batch_id:
        raise FileNotFoundError(f"no archived batch for {dataset} {business_date}")
    started = datetime.now(timezone.utc)
    table, manifest = arc.read_batch(archive, ds.name, business_date, batch_id)
    uri = archive.uri(arc.object_name(ds.name, business_date, batch_id))
    warehouse.ensure_raw_table(ds)
    record = BatchRecord(batch_id=batch_id, dataset=ds.name, source_system=ds.source_system,
                         business_date=business_date, mode="replay", status="LOADED", started_at=started,
                         finished_at=started, rows_extracted=manifest.get("rows"), archive_uri=uri,
                         control_totals_written=False, injections=manifest.get("injections", []),
                         orchestrator_run_id=run_id)
    rows = warehouse.load_partition(ds, business_date, table, uri, None, record)
    return IngestResult(ds.name, business_date.isoformat(), batch_id, "replay", int(manifest.get("rows", rows)),
                        rows, False, uri, manifest.get("injections", []))


def probe_source(settings: Settings, warehouse: Warehouse, source_system: str) -> dict:
    result = probe(settings.source_dsn)
    warehouse.record_source_health(source_system, result)
    return {**result, "archive_cutoff_date": str(result["archive_cutoff_date"] or "") or None}


def emit_run_complete(publisher, project: str, source_system: str, business_date: date,
                      results: list[dict], run_id: str | None) -> str:
    """Tell the engine (Phase 4) that this source's batches for the date have landed."""
    topic = publisher.topic_path(project, "run-complete")
    payload = {"type": "ingest.run_complete", "source_system": source_system,
               "business_date": business_date.isoformat(), "run_id": run_id, "batches": results,
               "emitted_at": datetime.now(timezone.utc).isoformat()}
    future = publisher.publish(topic, json.dumps(payload, default=str).encode(),
                               type="ingest.run_complete", source_system=source_system,
                               business_date=business_date.isoformat())
    return future.result(timeout=30)
