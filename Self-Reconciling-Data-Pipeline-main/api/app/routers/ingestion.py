"""Phase 1 endpoints: sources, batches and replay requests.

The API never runs SQL that changes data. A replay is a *request* written to the journal and
queued on Pub/Sub for the engine, which decides under policy (Phase 4)."""
from __future__ import annotations

import json
import uuid
from datetime import date, datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.concurrency import run_in_threadpool

from ..bq.repository import OpsRepository
from ..core.auth import User, current_user, require
from ..core.config import Settings, get_settings
from ..db import session
from ..db.session import RequestJournal
from ..deps import get_publisher, get_repo
from ..schemas.models import Batch, Batches, ReplayAccepted, ReplayRequest, SourceHealth
from ..services import catalog

router = APIRouter(tags=["ingestion"])


@router.get("/sources", response_model=list[SourceHealth])
async def sources(repo: OpsRepository = Depends(get_repo), _: User = Depends(current_user)):
    health, activity = await run_in_threadpool(lambda: (repo.latest_health(), repo.source_activity()))
    h = {r["source_system"]: r for r in health}
    a = {r["source_system"]: r for r in activity}
    out = []
    for system, meta in catalog.SOURCE_SYSTEMS.items():
        hr, ar = h.get(system, {}), a.get(system, {})
        out.append(SourceHealth(
            system=system, label=meta["label"], description=meta["description"], access=meta["access"],
            reachable=hr.get("reachable"), last_probe_at=hr.get("probed_at"), latency_ms=hr.get("latency_ms"),
            replica_lag_seconds=hr.get("replica_lag_seconds"), archive_cutoff_date=hr.get("archive_cutoff_date"),
            error=hr.get("error"),
            datasets=sum(1 for s, _ in catalog.DATASETS.values() if s == system),
            datasets_loaded=ar.get("datasets_loaded") or 0, last_batch_at=ar.get("last_batch_at"),
            last_business_date=ar.get("last_business_date")))
    return out


@router.get("/batches", response_model=Batches)
async def batches(date_: date | None = Query(None, alias="date"), repo: OpsRepository = Depends(get_repo),
                  _: User = Depends(current_user)):
    dates = await run_in_threadpool(repo.available_dates)
    d = date_ or (dates[0] if dates else datetime.now(timezone.utc).date())
    rows = await run_in_threadpool(repo.batches, d)
    return Batches(business_date=d, available_dates=dates,
                   batches=[Batch(**{**r, "injections": list(r.get("injections") or [])}) for r in rows])


@router.post("/batches/{dataset}/replay", response_model=ReplayAccepted, status_code=202)
async def request_replay(dataset: str, body: ReplayRequest, user: User = Depends(require("engineer")),
                         settings: Settings = Depends(get_settings), publisher=Depends(get_publisher)):
    if dataset not in catalog.DATASETS:
        raise HTTPException(404, f"unknown dataset {dataset}")
    request_id = str(uuid.uuid4())
    payload = {"type": "replay_request", "request_id": request_id, "dataset": dataset,
               "business_date": body.business_date.isoformat(), "batch_id": body.batch_id,
               "reason": body.reason, "requested_by": user.email,
               "requested_at": datetime.now(timezone.utc).isoformat()}

    # 1. Journal first, so a request is never queued without a record of who asked.
    factory = session.sessions()
    if factory is not None:
        async with factory() as s, s.begin():
            s.add(RequestJournal(id=request_id, kind="replay_request", payload=payload, requested_by=user.email,
                                 roles=",".join(sorted(user.roles)), status="received"))

    # 2. Queue for the engine.
    topic = publisher.topic_path(settings.gcp_project, "engine-requests")
    message_id = await run_in_threadpool(
        lambda: publisher.publish(topic, json.dumps(payload).encode(), type="replay_request",
                                  dataset=dataset).result(timeout=30))

    if factory is not None:
        async with factory() as s, s.begin():
            row = await s.get(RequestJournal, request_id)
            row.status, row.pubsub_message_id = "queued", message_id

    return ReplayAccepted(request_id=request_id, status="queued", topic="engine-requests", message_id=message_id,
                          note="Queued for the decision engine. Until Phase 4 is deployed, run the "
                               "replay_batch DAG to execute it.")
