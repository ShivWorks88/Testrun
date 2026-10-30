from __future__ import annotations

import dataclasses
from datetime import date, datetime, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

import psycopg
import pytest
from reconcile_ingest import archive as arc
from reconcile_ingest.datasets import DATASETS
from reconcile_ingest.extract import extract
from reconcile_ingest.pipeline import ingest_dataset, replay_batch

from .conftest import BUSINESS_DATE

IST = ZoneInfo("Asia/Kolkata")


@pytest.mark.parametrize("name", list(DATASETS))
def test_control_totals_describe_the_extracted_rows(settings, name):
    ds = DATASETS[name]
    ex = extract(settings.source_dsn, ds, BUSINESS_DATE, "b-test", IST)
    t = ex.table
    assert t.num_rows == ex.totals.row_count
    assert len(set(t[ds.key].to_pylist())) == ex.totals.distinct_keys
    if ds.amount_column:
        vals = [v for v in t[ds.amount_column].to_pylist() if v is not None]
        assert sum(vals, Decimal(0)) == ex.totals.amount_total
    if ds.counted_null_column:
        assert t[ds.counted_null_column].null_count == ex.totals.null_amount_count
    # every row belongs to the facility-local business date
    assert all(v.astimezone(IST).date() == BUSINESS_DATE for v in t[ds.event_column].to_pylist())


def test_business_date_is_facility_local_not_utc(settings):
    ds = DATASETS["encounters"]
    ex = extract(settings.source_dsn, ds, BUSINESS_DATE, "b-test", IST)
    utc_dates = {v.astimezone(timezone.utc).date() for v in ex.table["admit_ts"].to_pylist()}
    assert date(2026, 10, 12) in utc_dates      # 00:00-05:30 IST is still 12 Oct in UTC
    assert BUSINESS_DATE in utc_dates


def test_t1_same_date_twice_is_idempotent(settings, warehouse, archive):
    r1 = ingest_dataset(settings, warehouse, archive, "claims", BUSINESS_DATE)
    r2 = ingest_dataset(settings, warehouse, archive, "claims", BUSINESS_DATE)
    assert r1.batch_id.endswith("-01") and r2.batch_id.endswith("-02")
    raw = warehouse.raw["claims"][BUSINESS_DATE]
    assert raw.num_rows == r1.rows_loaded == r2.rows_loaded
    assert len(set(raw["_row_id"].to_pylist())) == raw.num_rows
    assert set(raw["_batch_id"].to_pylist()) == {r2.batch_id}
    per_batch = [t.batch_id for t in warehouse.control_totals]
    assert per_batch == [r1.batch_id, r2.batch_id]          # exactly one totals row per batch


def test_t2_missing_control_totals_is_recorded(settings, warehouse, archive):
    r = ingest_dataset(settings, warehouse, archive, "lab_orders", BUSINESS_DATE, ["missing_control_totals"])
    assert r.control_totals_written is False
    assert not [t for t in warehouse.control_totals if t.batch_id == r.batch_id]
    assert warehouse.batches[-1].control_totals_written is False
    assert warehouse.batches[-1].injections == ["missing_control_totals"]


def test_t3_drop_hour_leaves_totals_from_the_full_snapshot(settings, warehouse, archive):
    r = ingest_dataset(settings, warehouse, archive, "encounters", BUSINESS_DATE, ["drop_hour"])
    raw = warehouse.raw["encounters"][BUSINESS_DATE]
    totals = warehouse.control_totals[-1]
    assert raw.num_rows < totals.row_count == r.rows_extracted
    assert not [v for v in raw["admit_ts"].to_pylist() if v.astimezone(IST).hour == 2]


def test_t4_duplicate_rows_are_extra_rows_with_repeated_keys(settings, warehouse, archive):
    ingest_dataset(settings, warehouse, archive, "claims", BUSINESS_DATE, ["duplicate_rows"])
    raw = warehouse.raw["claims"][BUSINESS_DATE]
    totals = warehouse.control_totals[-1]
    assert raw.num_rows == totals.row_count + 12
    assert len(set(raw["claim_id"].to_pylist())) == totals.distinct_keys
    assert len(set(raw["_row_id"].to_pylist())) == raw.num_rows


def test_injections_require_explicit_opt_in(settings, warehouse, archive):
    locked = dataclasses.replace(settings, allow_injections=False)
    with pytest.raises(PermissionError):
        ingest_dataset(locked, warehouse, archive, "claims", BUSINESS_DATE, ["drop_hour"])


def test_archive_is_write_once_and_replay_restores_raw(settings, warehouse, archive):
    r = ingest_dataset(settings, warehouse, archive, "lab_results", BUSINESS_DATE)
    with pytest.raises(FileExistsError):
        archive.put(arc.object_name("lab_results", BUSINESS_DATE, r.batch_id), b"x", "text/plain")
    original = warehouse.raw["lab_results"][BUSINESS_DATE]
    warehouse.raw["lab_results"][BUSINESS_DATE] = original.slice(0, 5)        # damage raw
    rp = replay_batch(settings, warehouse, archive, "lab_results", BUSINESS_DATE)
    assert rp.batch_id == r.batch_id and rp.mode == "replay"
    assert warehouse.raw["lab_results"][BUSINESS_DATE].equals(original)
    assert warehouse.batches[-1].mode == "replay"


def test_archive_manifest_carries_totals_and_checksum(settings, warehouse, archive):
    r = ingest_dataset(settings, warehouse, archive, "payments", BUSINESS_DATE)
    table, manifest = arc.read_batch(archive, "payments", BUSINESS_DATE, r.batch_id)
    assert manifest["rows"] == table.num_rows == r.rows_loaded
    assert manifest["control_totals"]["batch_id"] == r.batch_id
    assert len(manifest["sha256"]) == 64


def test_failed_extract_is_recorded_and_raised(settings, warehouse, archive):
    broken = dataclasses.replace(settings, source_dsn="postgresql://nobody@127.0.0.1:1/none")
    with pytest.raises(psycopg.OperationalError):
        ingest_dataset(broken, warehouse, archive, "claims", BUSINESS_DATE)
    assert warehouse.batches[-1].status == "FAILED"
    assert "OperationalError" in warehouse.batches[-1].error


def test_snapshot_ignores_rows_committed_during_the_extract(settings, monkeypatch):
    """Rows inserted by another transaction after the snapshot started are invisible to both
    the totals and the rows (repeatable read)."""
    ds = DATASETS["payments"]
    import reconcile_ingest.extract as ex_mod

    real_connect = psycopg.connect
    inserted = {"done": False}

    class Hooked:
        def __init__(self, conn):
            self._c = conn

        def __getattr__(self, k):
            return getattr(self._c, k)

        def __enter__(self):
            self._c.__enter__()
            return self

        def __exit__(self, *a):
            return self._c.__exit__(*a)

        def execute(self, sql, *a, **kw):
            res = self._c.execute(sql, *a, **kw)
            if sql.startswith("select count(*)") and not inserted["done"]:
                with real_connect(settings.source_dsn, autocommit=True) as other:
                    other.execute("insert into billing.payments values ('Y-SNAPSHOT-TEST', 'C-x', 1.00, 'UPI', %s)",
                                  (datetime(2026, 10, 13, 12, 0, tzinfo=IST),))
                inserted["done"] = True
            return res

    monkeypatch.setattr(ex_mod.psycopg, "connect", lambda *a, **kw: Hooked(real_connect(*a, **kw)))
    try:
        ex = extract(settings.source_dsn, ds, BUSINESS_DATE, "b-snap", IST)
        assert inserted["done"]
        assert "Y-SNAPSHOT-TEST" not in ex.table["payment_id"].to_pylist()
        assert ex.table.num_rows == ex.totals.row_count
    finally:
        with real_connect(settings.source_dsn, autocommit=True) as c:
            c.execute("delete from billing.payments where payment_id = 'Y-SNAPSHOT-TEST'")


@pytest.mark.parametrize("name", list(DATASETS))
def test_parquet_schema_matches_bigquery_modes(settings, warehouse, archive, name):
    """The archived file must load into the raw table: same columns, types and REQUIRED modes."""
    r = ingest_dataset(settings, warehouse, archive, name, BUSINESS_DATE)
    table, _ = arc.read_batch(archive, name, BUSINESS_DATE, r.batch_id)
    ds = DATASETS[name]
    bq = {f["name"]: f for f in ds.bq_schema()}
    assert table.schema.names == list(bq)
    for field in table.schema:
        assert (bq[field.name]["mode"] == "REQUIRED") == (not field.nullable), field.name
