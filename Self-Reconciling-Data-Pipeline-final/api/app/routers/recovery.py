"""Phase 2–4 read-only views: reconciliation, publication ledger, incidents.

Everything here reads what the pipeline already recorded (receipts, gate decisions, the incident
journal). Nothing is computed from patient data and nothing is changed."""
from __future__ import annotations

import json
from collections import Counter
from datetime import date, datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.concurrency import run_in_threadpool

from ..bq.recovery_repository import BigQueryRecoveryRepository, RecoveryRepository
from ..bq.repository import OpsRepository
from ..core import gcp
from ..core.auth import User, current_user
from ..core.config import Settings, get_settings
from ..deps import get_repo
from ..schemas.recovery import (
    Gate,
    IncidentDetail,
    IncidentEvent,
    IncidentSummary,
    Publication,
    PublicationCell,
    PublicationEvent,
    ReconCheck,
    Reconciliation,
    ReconDataset,
)

router = APIRouter(tags=["recovery"])


def get_recovery_repo(settings: Settings = Depends(get_settings)) -> RecoveryRepository:
    return BigQueryRecoveryRepository(gcp.bigquery(), settings)


# ── reconciliation ──────────────────────────────────────────────────────────
def _check_name(row: dict) -> str:
    parts = (row.get("unique_id") or "").split(".")
    return parts[2] if len(parts) > 2 else (row.get("name") or row.get("unique_id") or "?")


def build_reconciliation(d: date, dates: list[date], batches: list[dict], checks: list[dict]) -> Reconciliation:
    all_checks = [ReconCheck(name=_check_name(c), resource_type=c.get("resource_type") or "",
                             status=c.get("status") or "",
                             failures=c.get("failures"), policy_outcome=c.get("policy_outcome"),
                             attached_to=c.get("attached_to"))
                  for c in checks if c.get("resource_type") in ("test", "operation")]
    used: set[int] = set()
    out = []
    for b in batches:
        ds = b["dataset"]
        mine = [c for c in all_checks if c.resource_type == "test"
                and {f"stg_{ds}", ds} & set((c.attached_to or "").split(","))]
        used.update(id(c) for c in mine)
        statuses = {c.status for c in mine}
        if statuses & {"fail", "error"}:
            result = "fail"
        elif "warn" in statuses:
            result = "warn"
        elif mine:
            result = "pass"
        else:
            result = "not_checked"
        receipt = b.get("receipt_rows")
        out.append(ReconDataset(
            dataset=ds, source_system=b.get("source_system") or "", batch_id=b.get("batch_id") or "",
            status=b.get("status") or "", mode=b.get("mode") or "", rows_loaded=b.get("rows_loaded"),
            receipt_rows=receipt, receipt_keys=b.get("receipt_keys"), receipt_amount=b.get("receipt_amount"),
            injections=list(b.get("injections") or []), finished_at=b.get("finished_at"),
            counts_match=None if receipt is None else b.get("rows_loaded") == receipt,
            result=result, checks=mine))
    gate = checks[0] if checks else {}
    return Reconciliation(
        business_date=d, available_dates=dates, gate_run_id=gate.get("gate_run_id"),
        checked_business_date=gate.get("business_date"),
        checked_at=max((c["recorded_at"] for c in checks if c.get("recorded_at")), default=None),
        datasets=out, other_checks=[c for c in all_checks if id(c) not in used],
        summary=dict(Counter(r.result for r in out)))


@router.get("/reconciliation", response_model=Reconciliation)
async def reconciliation(date_: date | None = Query(None, alias="date"), repo: OpsRepository = Depends(get_repo),
                         rrepo: RecoveryRepository = Depends(get_recovery_repo), _: User = Depends(current_user)):
    dates = await run_in_threadpool(repo.available_dates)
    d = date_ or (dates[0] if dates else datetime.now(timezone.utc).date())
    batches, checks = await run_in_threadpool(lambda: (rrepo.recon_batches(d), rrepo.recon_checks(d)))
    return build_reconciliation(d, dates, batches, checks)


# ── publication ─────────────────────────────────────────────────────────────
@router.get("/publication", response_model=Publication)
async def publication(dates: int = Query(14, ge=1, le=60), rrepo: RecoveryRepository = Depends(get_recovery_repo),
                      _: User = Depends(current_user)):
    cells, log = await run_in_threadpool(lambda: (rrepo.publication_cells(dates), rrepo.publication_log()))
    datasets = sorted({c["dataset"] for c in cells})
    last_verified = {ds: max((c["business_date"] for c in cells if c["dataset"] == ds and c["state"] == "PUBLISHED"),
                             default=None) for ds in datasets}
    return Publication(dates=sorted({c["business_date"] for c in cells}), datasets=datasets,
                       cells=[PublicationCell(**c) for c in cells], log=[PublicationEvent(**r) for r in log],
                       last_verified=last_verified)


# ── incidents ───────────────────────────────────────────────────────────────
def _summary(events: list[dict]) -> IncidentSummary:
    """events: one incident, newest first."""
    latest, first = events[0], events[-1]
    opened, updated = first["at"], latest["at"]
    return IncidentSummary(
        incident_id=latest["incident_id"], business_date=latest["business_date"], dataset=latest.get("dataset"),
        failure_type=latest.get("failure_type") or "", status=latest.get("status") or "",
        attempt=max(int(e.get("attempt") or 0) for e in events), action=latest.get("action"),
        mode=latest.get("mode"), note=latest.get("note"), opened_at=opened, updated_at=updated,
        minutes_open=round((updated - opened).total_seconds() / 60, 1), events=len(events),
        severity=next((e.get("severity") for e in events if e.get("severity")), None),
        rule_id=next((e.get("rule_id") for e in events if e.get("rule_id")), None),
        policy_version=next((e.get("policy_version") for e in events if e.get("policy_version")), None))


def _loads(v):
    if not v:
        return None
    try:
        return json.loads(v)
    except (TypeError, ValueError):
        return v


@router.get("/incidents", response_model=list[IncidentSummary])
async def incidents(rrepo: RecoveryRepository = Depends(get_recovery_repo), _: User = Depends(current_user)):
    rows = await run_in_threadpool(rrepo.incident_events)
    by_id: dict[str, list[dict]] = {}
    for r in rows:  # newest first
        by_id.setdefault(r["incident_id"], []).append(r)
    return sorted((_summary(ev) for ev in by_id.values()), key=lambda s: s.opened_at, reverse=True)


@router.get("/incidents/{incident_id}", response_model=IncidentDetail)
async def incident(incident_id: str, rrepo: RecoveryRepository = Depends(get_recovery_repo),
                   _: User = Depends(current_user)):
    rows = await run_in_threadpool(rrepo.incident_events, incident_id)
    if not rows:
        raise HTTPException(404, f"unknown incident {incident_id}")
    events = [IncidentEvent(at=r["at"], status=r.get("status") or "", attempt=r.get("attempt"), action=r.get("action"),
                            mode=r.get("mode"), note=r.get("note"), evidence=_loads(r.get("evidence")),
                            gates=[Gate(**g) for g in (_loads(r.get("gates")) or [])] or None,
                            gate_run_id=r.get("gate_run_id"), rule_id=r.get("rule_id"),
                            severity=r.get("severity"), route=r.get("route"),
                            policy_version=r.get("policy_version"))
              for r in reversed(rows)]  # oldest first for the journal
    return IncidentDetail(summary=_summary(rows), events=events)
