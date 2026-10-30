"""API contract for the Agents and approvals screen (Phase 5 stage 1)."""
from __future__ import annotations

from datetime import date, datetime
from typing import Any, Literal

from pydantic import BaseModel, Field


class ApprovalCard(BaseModel):
    """Facts written by the engine. The console shows them; nobody types them."""
    title: str = ""
    what_happened: str = ""
    evidence: dict[str, Any] = {}
    rule: str = ""
    impact: str = ""
    if_approved: str = ""
    if_rejected: str = ""
    guardrails: list[str] = []
    # Phase 5: plain-English wording by the Communication agent, written only from the facts above (optional)
    summary: str = ""
    summary_by: str = ""


class Decision(BaseModel):
    decision: Literal["APPROVE", "REJECT"]
    decided_by: str
    decided_at: datetime
    reason: str | None = None


class Approval(BaseModel):
    approval_id: str
    incident_id: str
    business_date: date
    dataset: str | None
    action: str
    params: dict[str, Any]
    status: str = Field(description="PENDING | APPROVED | REJECTED | EXPIRED | STALE | CANCELLED")
    shown_status: str = Field(description="status, or DECIDED while the worker has not picked the decision up")
    fingerprint: str
    policy_version: str | None
    rule_id: str | None
    severity: str | None
    route: str | None
    attempt: int | None
    card: ApprovalCard
    requested_by: str | None
    requested_at: datetime
    expires_at: datetime
    note: str | None
    decided_by: str | None = None
    decision: Decision | None = None
    can_decide: bool = Field(description="this user may approve or reject this card now")
    why_not: str | None = Field(None, description="why this user cannot decide, when can_decide is false")


class DecisionIn(BaseModel):
    decision: Literal["APPROVE", "REJECT"]
    fingerprint: str = Field(description="fingerprint of the card the person is looking at")
    reason: str = Field("", max_length=500)


class DecisionAccepted(BaseModel):
    approval_id: str
    decision: Literal["APPROVE", "REJECT"]
    decided_by: str
    decided_at: datetime
    next: str
