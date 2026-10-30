"""Dataset registry: what each extract reads, how its business date is defined, and what its
control totals measure. Shared by ingest, the console API and (later) dbt sources."""
from __future__ import annotations

from dataclasses import dataclass, field

import pyarrow as pa

# (column, arrow type, BigQuery type)
STR, TS, DATE, INT = pa.string(), pa.timestamp("us", tz="UTC"), pa.date32(), pa.int64()
NUM12_3, NUM14_2 = pa.decimal128(12, 3), pa.decimal128(14, 2)

_BQ = {STR: "STRING", TS: "TIMESTAMP", DATE: "DATE", INT: "INT64", NUM12_3: "NUMERIC", NUM14_2: "NUMERIC"}

# Metadata every raw row carries. business_date is the facility-local day of event_column.
META_COLUMNS: list[tuple[str, pa.DataType]] = [
    ("business_date", DATE),
    ("_batch_id", STR),
    ("_row_id", STR),
    ("_loaded_at", TS),
    ("_source_snapshot_ts", TS),
]


@dataclass(frozen=True)
class Dataset:
    name: str
    source_system: str          # EHR | LIS | BILLING
    table: str                  # source table (schema.table)
    key: str                    # primary key
    event_column: str           # its facility-local date is the business date
    columns: list[tuple[str, pa.DataType]]
    amount_column: str | None = None   # summed in control totals (and null-counted)
    null_column: str | None = None     # critical column null-counted when there is no amount
    criticality: str = "medium"
    phi: bool = True
    critical_fields: list[str] = field(default_factory=list)

    @property
    def column_names(self) -> list[str]:
        return [c for c, _ in self.columns]

    @property
    def required_columns(self) -> set[str]:
        return {self.key, "business_date", "_batch_id", "_row_id", "_loaded_at"}

    @property
    def arrow_schema(self) -> pa.Schema:
        # Parquet nullability matches the BigQuery modes exactly, so loads never hit a mode mismatch.
        req = self.required_columns
        return pa.schema([pa.field(c, t, nullable=c not in req) for c, t in self.columns + META_COLUMNS])

    def bq_schema(self) -> list[dict]:
        req = self.required_columns
        return [{"name": c, "type": _BQ[t], "mode": "REQUIRED" if c in req else "NULLABLE"}
                for c, t in self.columns + META_COLUMNS]

    @property
    def counted_null_column(self) -> str | None:
        return self.amount_column or self.null_column


DATASETS: dict[str, Dataset] = {d.name: d for d in [
    # ---------------------------------------------------------------- EHR
    Dataset("patients", "EHR", "ehr.patients", "patient_id", "updated_at", [
        ("patient_id", STR), ("mrn", STR), ("given_name", STR), ("family_name", STR), ("birth_date", DATE),
        ("sex", STR), ("facility_id", STR), ("created_at", TS), ("updated_at", TS)],
        criticality="high"),
    Dataset("encounters", "EHR", "ehr.encounters", "encounter_id", "admit_ts", [
        ("encounter_id", STR), ("patient_id", STR), ("facility_id", STR), ("encounter_type", STR), ("ward", STR),
        ("admit_ts", TS), ("discharge_ts", TS), ("status", STR), ("updated_at", TS)],
        criticality="high"),
    Dataset("diagnoses", "EHR", "ehr.diagnoses", "diagnosis_id", "recorded_ts", [
        ("diagnosis_id", STR), ("encounter_id", STR), ("icd10_code", STR), ("rank", INT), ("recorded_ts", TS)]),
    # ---------------------------------------------------------------- LIS
    Dataset("lab_orders", "LIS", "lis.lab_orders", "lab_order_id", "ordered_ts", [
        ("lab_order_id", STR), ("encounter_id", STR), ("patient_id", STR), ("priority", STR),
        ("ordering_dept", STR), ("ordered_ts", TS)]),
    Dataset("lab_panels", "LIS", "lis.lab_panels", "lab_panel_id", "ordered_ts", [
        ("lab_panel_id", STR), ("lab_order_id", STR), ("panel_code", STR), ("panel_name", STR),
        ("ordered_ts", TS)]),
    Dataset("lab_results", "LIS", "lis.lab_results", "lab_result_id", "resulted_ts", [
        ("lab_result_id", STR), ("lab_order_id", STR), ("lab_panel_id", STR), ("encounter_id", STR),
        ("patient_id", STR), ("test_code", STR), ("test_name", STR), ("result_value", NUM12_3),
        ("result_unit", STR), ("reference_low", NUM12_3), ("reference_high", NUM12_3), ("collected_at", TS),
        ("resulted_ts", TS)],
        null_column="result_value", criticality="high",
        critical_fields=["result_value", "result_unit", "collected_at"]),
    # ---------------------------------------------------------------- Billing
    Dataset("claims", "BILLING", "billing.claims", "claim_id", "submitted_ts", [
        ("claim_id", STR), ("encounter_id", STR), ("patient_id", STR), ("payer", STR), ("claim_amount", NUM14_2),
        ("status", STR), ("submitted_ts", TS), ("adjudicated_ts", TS)],
        amount_column="claim_amount", criticality="high"),
    Dataset("payments", "BILLING", "billing.payments", "payment_id", "paid_ts", [
        ("payment_id", STR), ("claim_id", STR), ("paid_amount", NUM14_2), ("method", STR), ("paid_ts", TS)],
        amount_column="paid_amount"),
    Dataset("adjustments", "BILLING", "billing.adjustments", "adjustment_id", "adjusted_ts", [
        ("adjustment_id", STR), ("claim_id", STR), ("adjustment_amount", NUM14_2), ("reason_code", STR),
        ("adjusted_ts", TS)],
        amount_column="adjustment_amount", criticality="low"),
]}

SOURCE_SYSTEMS: dict[str, dict] = {
    "EHR": {"label": "EHR", "description": "Encounters, patients, diagnoses",
            "access": "Read replica · repeatable-read snapshot · facility local time"},
    "LIS": {"label": "LIS", "description": "Lab results, panels, orders",
            "access": "Interface replica · critical fields: value, unit, collected time"},
    "BILLING": {"label": "Billing", "description": "Claims, payments, adjustments",
                "access": "Read replica · amounts in ₹, 2 dp · 3-day lookback for reversals"},
}


def datasets_for(system: str) -> list[Dataset]:
    return [d for d in DATASETS.values() if d.source_system == system]
