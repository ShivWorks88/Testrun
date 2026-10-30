"""Phase 6 Operations screen: one read-only summary of the whole pipeline, plus the read-only assistant.

GET  /api/operations        report status, KPIs, decision mix, resolve times, incidents, agent calls, freshness
GET  /api/operations/shell  outage breakers + cards waiting (polled by every page for the banner and badge)
POST /api/assistant         "Ask the pipeline" — answers from the pipeline's own records; never acts
Everything is computed from what the pipeline already recorded; nothing here changes data.
"""
from __future__ import annotations

import json
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from statistics import median
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, Query
from fastapi.concurrency import run_in_threadpool

from ..bq.operations_repository import BigQueryOperationsRepository, OperationsRepository
from ..bq.recovery_repository import RecoveryRepository
from ..core import gcp
from ..core.auth import User, current_user
from ..core.config import Settings, get_settings
from ..schemas.operations import (
    AgentCall,
    AskIn,
    AskOut,
    BoardRow,
    Breaker,
    DayRow,
    Freshness,
    Kpis,
    MixRow,
    Operations,
    OpsIncident,
    PolicyBrief,
    ReportState,
    ResolveTime,
    Shell,
)
from ..services import assistant
from ..services.environment import load_policy
from .recovery import _summary, get_recovery_repo

router = APIRouter(tags=["operations"])
IST = ZoneInfo("Asia/Kolkata")

# how the policy decided, from the mode on the incident's decision row
MODE_KIND = {"auto": "auto", "approve": "approve", "log": "quiet", "link": "quiet", "wait": "quiet",
             "hold": "hold", "escalate": "hold", "recommend": "hold"}
OPEN = {"OPEN", "ACTING", "RECOMMENDED", "ESCALATED", "AWAITING_APPROVAL", "HELD", "WAITING", "EXPECTED"}


def get_ops_repo(settings: Settings = Depends(get_settings)) -> OperationsRepository:
    return BigQueryOperationsRepository(gcp.bigquery(), settings)


# ── pure helpers (unit-tested) ──────────────────────────────────────────────
def kind_of(events: list[dict]) -> str:
    """events: one incident, any order."""
    for e in sorted(events, key=lambda e: e["at"]):
        k = MODE_KIND.get(e.get("mode") or "")
        if k:
            return k
    statuses = {e.get("status") for e in events}
    if statuses & {"EXPECTED", "WAITING"}:
        return "quiet"
    if "AWAITING_APPROVAL" in statuses:
        return "approve"
    return "hold"


def _list(v) -> list[str]:
    if isinstance(v, list):
        return [str(x) for x in v]
    if isinstance(v, str) and v.strip():
        try:
            parsed = json.loads(v)
            return [str(x) for x in parsed] if isinstance(parsed, list) else [v]
        except ValueError:
            return [x.strip() for x in v.split(",") if x.strip()]
    return []


def shell_from(breakers: list[dict], cards: list[dict], policy, now: datetime) -> Shell:
    waiting = [c for c in cards
               if c.get("status") == "PENDING" and (c.get("expires_at") is None or c["expires_at"] > now)]
    return Shell(
        open_breakers=[Breaker(source_system=b["source_system"], opened_at=b.get("opened_at"), reason=b.get("reason"),
                               failing_datasets=_list(b.get("failing_datasets")),
                               parent_incident_id=b.get("parent_incident_id")) for b in breakers],
        pending_cards=len(waiting),
        next_expiry=min((c["expires_at"] for c in waiting if c.get("expires_at")), default=None),
        policy=policy)


def build_operations(events: list[dict], cells: list[dict], calls: list[dict], fresh: dict, shell: Shell,
                     now: datetime, days: int, assistant_ok: bool) -> Operations:
    by_id: dict[str, list[dict]] = defaultdict(list)
    for e in events:
        by_id[e["incident_id"]].append(e)
    since = now - timedelta(days=days)
    incs = []
    for evs in by_id.values():
        evs.sort(key=lambda e: e["at"], reverse=True)          # newest first, as _summary expects
        s = _summary(evs)
        if s.opened_at >= since or s.status in OPEN - {"EXPECTED"}:
            incs.append(OpsIncident(**s.model_dump(), kind=kind_of(evs)))
    incs.sort(key=lambda i: i.opened_at, reverse=True)
    window = [i for i in incs if i.opened_at >= since]

    resolved = [i for i in window if i.status == "RESOLVED"]
    mix: dict[str, set[str]] = defaultdict(set)
    count: dict[str, int] = defaultdict(int)
    for i in window:
        mix[i.kind].add(i.failure_type)
        count[i.kind] += 1
    per_day: dict[date, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for i in window:
        per_day[i.opened_at.astimezone(IST).date()][i.kind] += 1
    today_ist = now.astimezone(IST).date()
    day_rows = [DayRow(day=d, counts=dict(per_day.get(d, {})))
                for d in (today_ist - timedelta(days=k) for k in range(days - 1, -1, -1))]
    times: dict[tuple[str, str], list[float]] = defaultdict(list)
    for i in resolved:
        times[(i.failure_type, i.kind)].append(i.minutes_open)
    resolve_times = sorted((ResolveTime(failure_type=ft, kind=k, median_minutes=round(median(v), 1), incidents=len(v))
                            for (ft, k), v in times.items()), key=lambda r: r.median_minutes, reverse=True)

    board_dates = sorted({c["business_date"] for c in cells})[-7:]
    datasets = sorted({c["dataset"] for c in cells})
    cell = {(c["dataset"], c["business_date"]): c for c in cells}
    board = [BoardRow(dataset=ds, cells=[ReportState(dataset=ds, business_date=d,
                                                     state=(cell.get((ds, d)) or {}).get("state", "NONE"),
                                                     reasons=(cell.get((ds, d)) or {}).get("reasons"),
                                                     decided_at=(cell.get((ds, d)) or {}).get("decided_at"))
                                         for d in board_dates]) for ds in datasets]
    latest = board_dates[-1] if board_dates else None
    today = [r.cells[-1] for r in board] if latest else []
    held_days = sum(1 for r in board for c in r.cells if c.state == "HELD")

    return Operations(
        generated_at=now, window_days=days, latest_business_date=latest, today=today, board_dates=board_dates,
        board=board, freshness=Freshness(**fresh),
        kpis=Kpis(incidents=len(window), open_incidents=sum(1 for i in incs if i.status in OPEN - {"EXPECTED"}),
                  resolved=len(resolved),
                  median_minutes_to_resolve=round(median([i.minutes_open for i in resolved]), 1) if resolved else None,
                  without_a_person=sum(1 for i in window if i.kind in ("auto", "quiet")), held_report_days=held_days),
        mix=[MixRow(kind=k, count=count[k], failure_types=sorted(mix[k]))
             for k in ("auto", "hold", "quiet", "approve")],
        per_day=day_rows, resolve_times=resolve_times, incidents=incs[:60],
        agent_calls=[AgentCall(**c) for c in calls], shell=shell, assistant_available=assistant_ok)


def _policy_brief(settings: Settings) -> PolicyBrief:
    p = load_policy(settings)
    cap = None
    try:
        import yaml
        with open(settings.policy_file) as f:
            cap = int(((yaml.safe_load(f) or {}).get("limits") or {}).get("attempt_cap"))
    except Exception:  # noqa: BLE001
        pass
    return PolicyBrief(version=p.version, kill_switch=p.kill_switch, agents_enabled=p.agents_enabled, attempt_cap=cap)


# ── routes ──────────────────────────────────────────────────────────────────
@router.get("/operations/shell", response_model=Shell)
async def shell(settings: Settings = Depends(get_settings), orepo: OperationsRepository = Depends(get_ops_repo),
                _: User = Depends(current_user)):
    breakers, cards = await run_in_threadpool(lambda: (orepo.open_breakers(), orepo.approvals(50)))
    return shell_from(breakers, cards, _policy_brief(settings), datetime.now(timezone.utc))


async def _operations(settings, orepo, rrepo, days) -> tuple[Operations, list[dict]]:
    def work():
        return (rrepo.incident_events(limit=4000), rrepo.publication_cells(7), orepo.agent_calls(20),
                orepo.freshness(), orepo.open_breakers(), orepo.approvals(50))
    events, cells, calls, fresh, breakers, cards = await run_in_threadpool(work)
    now = datetime.now(timezone.utc)
    sh = shell_from(breakers, cards, _policy_brief(settings), now)
    ops = build_operations(events, cells, calls, fresh, sh, now, days, assistant.available(settings)[0])
    return ops, cards


@router.get("/operations", response_model=Operations)
async def operations(days: int = Query(7, ge=1, le=30), settings: Settings = Depends(get_settings),
                     orepo: OperationsRepository = Depends(get_ops_repo),
                     rrepo: RecoveryRepository = Depends(get_recovery_repo), _: User = Depends(current_user)):
    ops, _cards = await _operations(settings, orepo, rrepo, days)
    return ops


@router.post("/assistant", response_model=AskOut)
async def ask(body: AskIn, settings: Settings = Depends(get_settings),
              orepo: OperationsRepository = Depends(get_ops_repo),
              rrepo: RecoveryRepository = Depends(get_recovery_repo), user: User = Depends(current_user)):
    ok, why = assistant.available(settings)
    if not ok:
        return AskOut(status="unavailable", answer=why, reason=why)
    ops, cards = await _operations(settings, orepo, rrepo, 7)
    payload = json.loads(ops.model_dump_json())
    payload["cards"] = [{k: (str(v) if isinstance(v, datetime) else v) for k, v in c.items()} for c in cards[:15]]
    out, row = await run_in_threadpool(assistant.ask, settings, body.question, payload, user.email,
                                       ops.shell.policy.version)
    try:
        await run_in_threadpool(orepo.record_agent_call, row)
    except Exception:  # noqa: BLE001 — provenance is required: no record, no answer
        return AskOut(status="refused", answer="The answer could not be recorded, so it is not shown.",
                      reason="not_recorded", call_id=row["call_id"])
    return AskOut(**out)
