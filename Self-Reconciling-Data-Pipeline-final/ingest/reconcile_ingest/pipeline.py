"""Ingest steps used by the Airflow DAGs and the CLI.

    ingest_dataset : snapshot extract -> control totals -> archive (write-once) -> idempotent raw load
    replay_batch   : archive -> raw (no source access), for recovery after the source has moved on
    probe_source   : source health row for the console
"""
from __future__ import annotations

import dataclasses
import json
import logging
import random
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal

import pyarrow as pa
import pyarrow.compute as pc

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
# Phase 6 — these simulate the SOURCE itself (not a defect after extraction), so the receipt describes
# what the source really had. Kept apart from INJECTIONS, which must all be caught by reconciliation.
SOURCE_INJECTIONS = {
    "source_down": "The source accepts connections but every extract times out (outage, T19-T21)",
    "late_documentation": "The source has only the first 35% of the day yet; the rest is documented later",
    "orphan_results": "Lab results: 2 morning results point at archived encounters, 3 at encounters that exist "
                      "nowhere, 2 late-evening ones at encounters of the next day (orphans, Phase 6 pkg 4)",
}
# encounter IDs the orphan_results injection uses (the suite registers the archived ones in ehr.archive_index)
ARCHIVED_TEST_ENCOUNTERS = ("E-20261001-77770", "E-20261001-77771")
# handled by the ingest DAG, never by ingest_dataset: source_timeout (task attempt 1), stale_feed (probe)
DAG_SIDE = {"source_timeout", "stale_feed"}
STALE_FEED_LAG_HOURS = 30

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


def _orphan_results(table: pa.Table, tz, business_date: date) -> pa.Table:
    """Re-point a few lab results at parents raw does not have. Keys, counts, nulls and amounts are unchanged,
    so reconciliation passes — only the source-aware orphan check can see it (as in real life).
    2 morning results → archived encounters, 3 more → encounters that exist nowhere, and 2 results collected
    late in the evening → encounters of the next day (young). For the young ones the collection time is set to
    23:15 on the business date in the facility time zone the orphan check uses (synthetic test data only), so
    the case does not depend on what the generator produced for that evening."""
    from .source_checks import TZ as FACILITY_TZ  # the same clock the orphan classifier uses
    if "encounter_id" not in table.column_names or "collected_at" not in table.column_names:
        return table
    local = [None if v is None else v.astimezone(FACILITY_TZ) for v in table["collected_at"].to_pylist()]
    morning = [i for i, h in enumerate(local) if h is not None and h.hour < 12]
    rest = sorted((i for i, h in enumerate(local) if h is not None and h.hour >= 12),
                  key=lambda i: local[i], reverse=True)
    evening = rest[:2]
    late = datetime.combine(business_date, datetime.min.time().replace(hour=23, minute=15), tzinfo=FACILITY_TZ)
    collected = table["collected_at"].to_pylist()
    for i in evening:
        collected[i] = late.astimezone(timezone.utc)
    cidx = table.schema.get_field_index("collected_at")
    ctype = table.schema.field(cidx).type
    table = table.set_column(cidx, table.schema.field(cidx), pa.array(collected, type=ctype))
    nxt = date.fromordinal(business_date.toordinal() + 1)
    plan = dict(zip(morning[:2], ARCHIVED_TEST_ENCOUNTERS, strict=False))
    plan.update(zip(morning[2:5], [f"E-{business_date:%Y%m%d}-9999{k}" for k in range(3)], strict=False))
    plan.update(zip(evening, [f"E-{nxt:%Y%m%d}-8888{k}" for k in range(2)], strict=False))
    ids = table["encounter_id"].to_pylist()
    for i, new in plan.items():
        ids[i] = new
    idx = table.schema.get_field_index("encounter_id")
    return table.set_column(idx, table.schema.field(idx), pa.array(ids, type=table.schema.field(idx).type))


def _late_documentation(ds: Dataset, ex):
    """Keep the earliest 35% of the day's rows (by event time) and describe THAT in the receipt — the
    source simply has not got the rest yet. Receipt and raw agree; only the volume is low."""
    n = ex.table.num_rows
    if n == 0:
        return ex
    ts = ex.table[ds.event_column].to_pylist()
    keep = sorted(range(n), key=lambda i: (ts[i] is None, ts[i].timestamp() if ts[i] else 0.0))[: max(1, int(n * 0.35))]
    table = ex.table.take(pa.array(sorted(keep)))
    nullc, amt = ds.counted_null_column, ds.amount_column
    amount = pc.sum(table[amt]).as_py() if amt else None
    totals = dataclasses.replace(
        ex.totals, row_count=table.num_rows, distinct_keys=pc.count_distinct(table[ds.key]).as_py(),
        null_amount_count=table[nullc].null_count if nullc else None,
        amount_total=None if amount is None else Decimal(str(amount)))
    return dataclasses.replace(ex, table=table, totals=totals)


def ingest_dataset(settings: Settings, warehouse: Warehouse, archive: arc.Archive, dataset: str,
                   business_date: date, injections: list[str] | None = None,
                   run_id: str | None = None) -> IngestResult:
    ds = DATASETS[dataset]
    injections = sorted(set(injections or []))
    unknown = set(injections) - set(INJECTIONS) - set(SOURCE_INJECTIONS)
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
        if "source_down" in injections:   # recorded as a FAILED batch, exactly like a real timeout
            raise ConnectionError(f"simulated source outage: {ds.source_system} extract of {ds.name} timed out")
        ex = extract(settings.source_dsn, ds, business_date, batch_id, settings.facility_tz, loaded_at=started)
        if "late_documentation" in injections:
            ex = _late_documentation(ds, ex)
        table = _apply_injections(ds, ex.table, injections, settings.facility_tz, batch_id)
        if "orphan_results" in injections and ds.name == "lab_results":
            table = _orphan_results(table, settings.facility_tz, business_date)
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


def probe_source(settings: Settings, warehouse: Warehouse, source_system: str,
                 inject: list[str] | None = None) -> dict:
    result = probe(settings.source_dsn)
    if "stale_feed" in (inject or []):   # test only: the replica stopped applying changes 30 h ago
        if not settings.allow_injections:
            raise PermissionError("failure injection is disabled (ALLOW_INJECTIONS=true in test only)")
        result = {**result, "replica_lag_seconds": STALE_FEED_LAG_HOURS * 3600.0}
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
