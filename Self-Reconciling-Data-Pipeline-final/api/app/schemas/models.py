"""API contract. The console's TypeScript types are generated from this via OpenAPI."""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, Field

State = Literal["ready", "warning", "missing", "error", "off", "pending"]
Access = Literal["Write", "Read", "Masked", "—"]


class CheckResult(BaseModel):
    ok: bool
    detail: str


class Health(BaseModel):
    status: Literal["ok", "degraded"]
    version: str
    checks: dict[str, CheckResult]


class Resource(BaseModel):
    name: str
    service: str
    state: State
    label: str = Field(description="Short state text shown in the pill, e.g. 'Ready', 'Locked'")
    detail: str = ""


class IdentityRow(BaseModel):
    component: str
    email: str
    access: dict[str, Access]


class SyntheticRow(BaseModel):
    dataset: str
    source_system: str
    business_date: date | None
    rows: int | None
    edge_cases: str


class PolicyInfo(BaseModel):
    version: str
    kill_switch: bool
    agents_enabled: bool
    residency_confirmed: bool
    actions: int
    rules: int


class Environment(BaseModel):
    project: str
    region: str
    runs_as: str
    resources: list[Resource]
    identity_columns: list[str]
    identities: list[IdentityRow]
    synthetic: list[SyntheticRow]
    policy: PolicyInfo
    build: dict[str, str]


class SourceHealth(BaseModel):
    system: str
    label: str
    description: str
    access: str
    reachable: bool | None
    last_probe_at: datetime | None
    latency_ms: float | None
    replica_lag_seconds: float | None
    archive_cutoff_date: date | None
    error: str | None
    datasets: int
    datasets_loaded: int
    last_batch_at: datetime | None
    last_business_date: date | None


class Batch(BaseModel):
    batch_id: str
    dataset: str
    source_system: str
    business_date: date
    mode: str
    status: str
    rows_extracted: int | None
    rows_loaded: int | None
    control_total_rows: int | None = Field(description="row_count from the control-totals row")
    distinct_keys: int | None
    amount_total: Decimal | None
    totals_rows: int = Field(description="How many control-totals rows exist for this batch (must be 1)")
    archive_uri: str | None
    injections: list[str]
    finished_at: datetime
    error: str | None


class Batches(BaseModel):
    business_date: date
    batches: list[Batch]
    available_dates: list[date]


class ReplayRequest(BaseModel):
    business_date: date
    batch_id: str | None = None
    reason: str = Field(default="", max_length=500)


class ReplayAccepted(BaseModel):
    request_id: str
    status: Literal["queued"]
    topic: str
    message_id: str
    note: str
