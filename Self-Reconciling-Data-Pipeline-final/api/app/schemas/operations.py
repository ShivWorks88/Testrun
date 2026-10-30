"""API contract for the Operations screen (Phase 6) and the read-only "Ask the pipeline" assistant."""
from __future__ import annotations

from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, Field

from .recovery import IncidentSummary

Kind = Literal["auto", "hold", "quiet", "approve"]


class Breaker(BaseModel):
    source_system: str
    opened_at: datetime | None
    reason: str | None
    failing_datasets: list[str]
    parent_incident_id: str | None


class PolicyBrief(BaseModel):
    version: str
    kill_switch: bool
    agents_enabled: bool
    attempt_cap: int | None = None


class Shell(BaseModel):
    """Polled by every page: the outage banner and the 'waiting for you' badge."""
    open_breakers: list[Breaker]
    pending_cards: int
    next_expiry: datetime | None
    policy: PolicyBrief


class ReportState(BaseModel):
    dataset: str
    business_date: date | None
    state: str
    reasons: str | None
    decided_at: datetime | None


class BoardRow(BaseModel):
    dataset: str
    cells: list[ReportState]


class Freshness(BaseModel):
    last_batch_at: datetime | None
    last_gate_at: datetime | None
    last_engine_at: datetime | None


class Kpis(BaseModel):
    incidents: int
    open_incidents: int
    resolved: int
    median_minutes_to_resolve: float | None
    without_a_person: int = Field(description="incidents fixed automatically or recorded as expected")
    held_report_days: int = Field(description="report-days held (users kept the last verified figures)")


class MixRow(BaseModel):
    kind: Kind
    count: int
    failure_types: list[str]


class DayRow(BaseModel):
    day: date
    counts: dict[str, int]


class ResolveTime(BaseModel):
    failure_type: str
    kind: Kind
    median_minutes: float
    incidents: int


class OpsIncident(IncidentSummary):
    kind: Kind


class AgentCall(BaseModel):
    call_id: str | None
    incident_id: str | None
    agent_id: str | None
    provider: str | None
    model: str | None
    prompt_version: str | None
    validation: str | None
    reason: str | None
    proposed_action: str | None
    outcome: str | None
    latency_ms: int | None
    at: datetime


class Operations(BaseModel):
    generated_at: datetime
    window_days: int
    latest_business_date: date | None
    today: list[ReportState]
    board_dates: list[date]
    board: list[BoardRow]
    freshness: Freshness
    kpis: Kpis
    mix: list[MixRow]
    per_day: list[DayRow]
    resolve_times: list[ResolveTime]
    incidents: list[OpsIncident]
    agent_calls: list[AgentCall]
    shell: Shell
    assistant_available: bool


class AskIn(BaseModel):
    question: str = Field(min_length=3, max_length=500)


class AskOut(BaseModel):
    status: Literal["answered", "refused", "unavailable"]
    answer: str
    cited: list[str] = []
    reason: str | None = None
    call_id: str | None = None
