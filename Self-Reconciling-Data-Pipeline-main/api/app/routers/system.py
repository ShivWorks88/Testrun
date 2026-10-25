from __future__ import annotations

from fastapi import APIRouter, Depends
from fastapi.concurrency import run_in_threadpool

from ..bq.repository import OpsRepository
from ..core import gcp
from ..core.auth import User, current_user
from ..core.config import Settings, get_settings
from ..db import session
from ..deps import get_repo
from ..schemas.models import CheckResult, Environment, Health, SyntheticRow
from ..services import catalog, environment

router = APIRouter(tags=["system"])


@router.get("/health", response_model=Health)
async def health(settings: Settings = Depends(get_settings)) -> Health:
    """Liveness plus dependency checks. Used by the Phase 0 exit gate (API health green)."""
    checks: dict[str, CheckResult] = {}
    try:
        await run_in_threadpool(lambda: list(gcp.bigquery().query("select 1").result()))
        checks["bigquery"] = CheckResult(ok=True, detail="query ok")
    except Exception as exc:  # noqa: BLE001
        checks["bigquery"] = CheckResult(ok=False, detail=f"{type(exc).__name__}: {str(exc)[:160]}")
    ok, detail = await session.ping()
    checks["cloud_sql"] = CheckResult(ok=ok or detail == "not configured", detail=detail)
    status = "ok" if all(c.ok for c in checks.values()) else "degraded"
    return Health(status=status, version=settings.git_sha, checks=checks)


@router.get("/me")
def me(user: User = Depends(current_user)) -> dict:
    return {"email": user.email, "roles": sorted(user.roles)}


@router.get("/environment", response_model=Environment)
async def get_environment(settings: Settings = Depends(get_settings), repo: OpsRepository = Depends(get_repo),
                          _: User = Depends(current_user)) -> Environment:
    policy = environment.load_policy(settings)
    db_state = await session.ping()

    def work():
        loaded = []
        try:
            loaded = repo.latest_loaded()
        except Exception:  # noqa: BLE001 - empty project: tables exist but nothing loaded
            pass
        last = max((r.get("finished_at") for r in loaded if r.get("finished_at")), default=None)
        res, acl = environment.resources(settings, db_state, last, policy)
        return loaded, res, environment.identity_matrix(settings, acl)

    loaded, res, identities = await run_in_threadpool(work)
    by_ds = {r["dataset"]: r for r in loaded}
    synthetic = [SyntheticRow(dataset=name, source_system=system,
                              business_date=by_ds.get(name, {}).get("business_date"),
                              rows=by_ds.get(name, {}).get("rows_loaded"), edge_cases=notes)
                 for name, (system, notes) in catalog.DATASETS.items()]
    return Environment(project=settings.gcp_project, region=settings.gcp_region, runs_as=gcp.runs_as(),
                       resources=res, identity_columns=environment.DATASETS, identities=identities,
                       synthetic=synthetic, policy=policy,
                       build={"git_sha": settings.git_sha, "built_at": settings.built_at})
