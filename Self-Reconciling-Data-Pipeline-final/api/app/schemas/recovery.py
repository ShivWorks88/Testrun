"""API contract for the Reconciliation, Publication and Incidents screens (Phases 2–4)."""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any, Literal

from pydantic import BaseModel, Field


class ReconCheck(BaseModel):
    name: str
    resource_type: str
    status: str
    failures: int | None = None
    policy_outcome: str | None = None
    attached_to: str | None = None


class ReconDataset(BaseModel):
    dataset: str
    source_system: str
    batch_id: str
    status: str
    mode: str
    rows_loaded: int | None
    receipt_rows: int | None = Field(description="row_count on this batch's receipt")
    receipt_keys: int | None
    receipt_amount: Decimal | None
    injections: list[str]
    finished_at: datetime | None
    counts_match: bool | None = Field(description="rows landed == receipt rows (None: no receipt)")
    result: Literal["pass", "warn", "fail", "not_checked"]
    checks: list[ReconCheck]


class Reconciliation(BaseModel):
    business_date: date
    available_dates: list[date]
    gate_run_id: str | None
    checked_business_date: date | None = Field(description="business_date of the dbt run that checked this day")
    checked_at: datetime | None
    datasets: list[ReconDataset]
    other_checks: list[ReconCheck]
    summary: dict[str, int]


class PublicationCell(BaseModel):
    dataset: str
    business_date: date
    state: str
    reasons: str | None
    decided_at: datetime
    snapshot: str | None = None


class PublicationEvent(PublicationCell):
    gate_run_id: str | None = None
    actor: str | None = None


class Publication(BaseModel):
    dates: list[date]
    datasets: list[str]
    cells: list[PublicationCell]
    log: list[PublicationEvent]
    last_verified: dict[str, date | None]


class Gate(BaseModel):
    gate: int
    question: str
    answer: str
    passed: bool = Field(alias="pass")

    model_config = {"populate_by_name": True}


class IncidentSummary(BaseModel):
    incident_id: str
    business_date: date
    dataset: str | None
    failure_type: str
    status: str
    attempt: int
    action: str | None
    mode: str | None
    note: str | None
    opened_at: datetime
    updated_at: datetime
    minutes_open: float
    events: int
    severity: str | None = None
    rule_id: str | None = None
    policy_version: str | None = None


class IncidentEvent(BaseModel):
    at: datetime
    status: str
    attempt: int | None
    action: str | None
    mode: str | None
    note: str | None
    evidence: Any = None
    gates: list[Gate] | None = None
    gate_run_id: str | None = None
    rule_id: str | None = None
    severity: str | None = None
    route: str | None = None
    policy_version: str | None = None


class IncidentDetail(BaseModel):
    summary: IncidentSummary
    events: list[IncidentEvent]
