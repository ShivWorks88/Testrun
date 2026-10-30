"""Extract one dataset for one business date from the source replica.

The rows and their control totals are read inside ONE read-only, repeatable-read transaction,
so both describe the same snapshot even while the source keeps changing.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

import psycopg
import pyarrow as pa

from .datasets import Dataset


@dataclass
class ControlTotals:
    dataset: str
    source_system: str
    business_date: date
    batch_id: str
    row_count: int
    distinct_keys: int
    null_amount_count: int | None
    amount_total: Decimal | None
    key_column: str
    amount_column: str | None
    snapshot_ts: datetime
    extracted_at: datetime
    archive_uri: str = ""
    extractor_version: str = ""

    def as_json(self) -> dict:
        d = asdict(self)
        for k, v in d.items():
            if isinstance(v, (date, datetime)):
                d[k] = v.isoformat()
            elif isinstance(v, Decimal):
                d[k] = str(v)
        return d


@dataclass
class Extract:
    table: pa.Table
    totals: ControlTotals
    snapshot_ts: datetime


def day_bounds(d: date, tz: ZoneInfo) -> tuple[datetime, datetime]:
    start = datetime.combine(d, time(0, 0), tzinfo=tz)
    return start, start + timedelta(days=1)


def extract(dsn: str, ds: Dataset, business_date: date, batch_id: str, tz: ZoneInfo,
            loaded_at: datetime | None = None, fetch_size: int = 20_000) -> Extract:
    start, end = day_bounds(business_date, tz)
    cols = ", ".join(ds.column_names)
    where = f"{ds.event_column} >= %(start)s and {ds.event_column} < %(end)s"
    nullc = ds.counted_null_column
    totals_sql = (
        f"select count(*), count(distinct {ds.key}), "
        f"{f'count(*) filter (where {nullc} is null)' if nullc else 'null'}, "
        f"{f'sum({ds.amount_column})' if ds.amount_column else 'null'} "
        f"from {ds.table} where {where}"
    )
    loaded_at = loaded_at or datetime.now(timezone.utc)

    with psycopg.connect(dsn, autocommit=False) as conn:
        conn.execute("set transaction isolation level repeatable read, read only")
        snapshot_ts = conn.execute("select transaction_timestamp()").fetchone()[0]
        n, distinct, nulls, amount = conn.execute(totals_sql, {"start": start, "end": end}).fetchone()
        columns: dict[str, list] = {c: [] for c in ds.column_names}
        with conn.cursor(name=f"x_{ds.name}") as cur:   # server-side cursor, same snapshot
            cur.itersize = fetch_size
            cur.execute(f"select {cols} from {ds.table} where {where} order by {ds.key}",
                        {"start": start, "end": end})
            for row in cur:
                for c, v in zip(ds.column_names, row, strict=True):
                    columns[c].append(v)
        conn.rollback()  # read-only; nothing to commit

    rows = len(columns[ds.key])
    data = {c: pa.array(columns[c], type=t) for c, t in ds.columns}
    data["business_date"] = pa.array([business_date] * rows, type=pa.date32())
    data["_batch_id"] = pa.array([batch_id] * rows, type=pa.string())
    data["_row_id"] = pa.array([f"{batch_id}-{i:07d}" for i in range(rows)], type=pa.string())
    data["_loaded_at"] = pa.array([loaded_at] * rows, type=pa.timestamp("us", tz="UTC"))
    data["_source_snapshot_ts"] = pa.array([snapshot_ts] * rows, type=pa.timestamp("us", tz="UTC"))
    table = pa.Table.from_pydict(data, schema=ds.arrow_schema)

    totals = ControlTotals(
        dataset=ds.name, source_system=ds.source_system, business_date=business_date, batch_id=batch_id,
        row_count=int(n), distinct_keys=int(distinct),
        null_amount_count=None if nulls is None else int(nulls),
        amount_total=None if amount is None else Decimal(amount),
        key_column=ds.key, amount_column=ds.amount_column, snapshot_ts=snapshot_ts,
        extracted_at=datetime.now(timezone.utc),
    )
    if rows != totals.row_count:   # cannot happen inside one snapshot; guard anyway
        raise RuntimeError(f"{ds.name}: snapshot mismatch, {rows} rows read vs {totals.row_count} counted")
    return Extract(table=table, totals=totals, snapshot_ts=snapshot_ts)


def probe(dsn: str, timeout_s: int = 10) -> dict:
    """Source health: reachability, latency, replica lag and the EHR archive cutoff."""
    t0 = datetime.now(timezone.utc)
    try:
        with psycopg.connect(dsn, connect_timeout=timeout_s) as conn:
            lag = conn.execute(
                "select case when pg_is_in_recovery() "
                "then extract(epoch from now() - pg_last_xact_replay_timestamp()) else 0 end").fetchone()[0]
            cutoff = None
            if conn.execute("select to_regclass('ehr.archive_policy')").fetchone()[0]:
                cutoff = conn.execute("select cutoff_date from ehr.archive_policy where id = 1").fetchone()[0]
        latency = (datetime.now(timezone.utc) - t0).total_seconds() * 1000
        return {"reachable": True, "latency_ms": round(latency, 1),
                "replica_lag_seconds": float(lag or 0), "archive_cutoff_date": cutoff, "error": None}
    except Exception as exc:  # noqa: BLE001 - a probe reports, it does not raise
        return {"reachable": False, "latency_ms": None, "replica_lag_seconds": None,
                "archive_cutoff_date": None, "error": str(exc)[:500]}
