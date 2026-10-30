"""Phase 5 stage 1 — approval cards and data-owner decisions.

The console only RECORDS a decision. It runs nothing. The approval_worker DAG picks the decision up,
re-checks fingerprint, incident state, policy version, kill switch and attempt cap, and only then acts.
Rules enforced here (and again by the worker):
  - only the data_owner role decides; the requester (sa-engine) and service accounts never do
  - the card must still be PENDING and not expired, with no earlier decision
  - the person must have seen the current card (fingerprint match); a rejection needs a reason
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from fastapi.concurrency import run_in_threadpool

from ..bq.approvals_repository import ApprovalsRepository, BigQueryApprovalsRepository
from ..core import gcp
from ..core.auth import User, current_user, require
from ..core.config import Settings, get_settings
from ..schemas.approvals import Approval, ApprovalCard, Decision, DecisionAccepted, DecisionIn

router = APIRouter(tags=["approvals"])


def get_approvals_repo(settings: Settings = Depends(get_settings)) -> ApprovalsRepository:
    return BigQueryApprovalsRepository(gcp.bigquery(), settings)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _json(v, default):
    if not v:
        return default
    try:
        return json.loads(v)
    except (TypeError, ValueError):
        return default


def blocked_reason(row: dict, dec: dict | None, user: User, now: datetime) -> str | None:
    """None when this user may decide this card now; otherwise the reason, in plain words."""
    if not user.has("data_owner"):
        return "only a data owner can approve or reject"
    if user.email.lower().endswith(".gserviceaccount.com"):
        return "service accounts and agents cannot approve"
    if user.email.lower() == (row.get("requested_by") or "").lower():
        return "the requester cannot approve its own card"
    if row.get("status") != "PENDING":
        return f"card is {row.get('status')}"
    if dec is not None:
        return f"already decided by {dec.get('decided_by')}"
    if now >= row["expires_at"]:
        return "card has expired"
    return None


def to_model(row: dict, dec: dict | None, user: User, now: datetime) -> Approval:
    why = blocked_reason(row, dec, user, now)
    shown = row["status"]
    if shown == "PENDING" and dec is not None:
        shown = "DECIDED"            # recorded; the approval_worker has not picked it up yet
    elif shown == "PENDING" and now >= row["expires_at"]:
        shown = "EXPIRING"           # past expiry; the worker will mark it EXPIRED on its next run
    return Approval(
        approval_id=row["approval_id"], incident_id=row["incident_id"], business_date=row["business_date"],
        dataset=row.get("dataset"), action=row["action"], params=_json(row.get("params"), {}),
        status=row["status"], shown_status=shown, fingerprint=row["fingerprint"],
        policy_version=row.get("policy_version"), rule_id=row.get("rule_id"), severity=row.get("severity"),
        route=row.get("route"), attempt=row.get("attempt"), card=ApprovalCard(**_json(row.get("card"), {})),
        requested_by=row.get("requested_by"), requested_at=row["requested_at"], expires_at=row["expires_at"],
        note=row.get("note"), decided_by=row.get("decided_by") or (dec and dec.get("decided_by")),
        decision=Decision(decision=dec["decision"], decided_by=dec["decided_by"], decided_at=dec["decided_at"],
                          reason=dec.get("reason")) if dec else None,
        can_decide=why is None, why_not=why)


@router.get("/approvals", response_model=list[Approval])
async def approvals(repo: ApprovalsRepository = Depends(get_approvals_repo), user: User = Depends(current_user)):
    rows = await run_in_threadpool(repo.approvals)
    decs = await run_in_threadpool(repo.decisions, [r["approval_id"] for r in rows if r["status"] == "PENDING"])
    now = _now()
    return [to_model(r, decs.get(r["approval_id"]), user, now) for r in rows]


@router.post("/approvals/{approval_id}/decision", response_model=DecisionAccepted, status_code=202)
async def decide(approval_id: str, body: DecisionIn, repo: ApprovalsRepository = Depends(get_approvals_repo),
                 user: User = Depends(require("data_owner"))):
    row = await run_in_threadpool(repo.approval, approval_id)
    if row is None:
        raise HTTPException(404, f"unknown approval {approval_id}")
    dec = await run_in_threadpool(repo.decision, approval_id)
    now = _now()
    why = blocked_reason(row, dec, user, now)
    if why:
        raise HTTPException(409, why)
    if body.fingerprint != row["fingerprint"]:
        raise HTTPException(409, "the card changed since you opened it — reload and read it again")
    reason = body.reason.strip()
    if body.decision == "REJECT" and len(reason) < 3:
        raise HTTPException(422, "a rejection needs a short reason")
    await run_in_threadpool(repo.insert_decision, {
        "decision_id": uuid.uuid4().hex, "approval_id": approval_id, "decision": body.decision,
        "fingerprint": body.fingerprint, "reason": reason or None, "decided_by": user.email,
        "roles": ",".join(sorted(user.roles)), "decided_at": now.isoformat()})
    nxt = ("the approval worker re-checks the facts, then re-extracts and rebuilds (within 5 minutes)"
           if body.decision == "APPROVE" else "the approval worker escalates the incident (within 5 minutes)")
    return DecisionAccepted(approval_id=approval_id, decision=body.decision, decided_by=user.email,
                            decided_at=now, next=nxt)
