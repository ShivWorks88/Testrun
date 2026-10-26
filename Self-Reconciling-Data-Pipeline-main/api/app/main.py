"""Console API (FastAPI). Read-mostly: it shows verified state and evidence, and turns any
action into a request for the engine. It also serves the built React console, so one private
Cloud Run service hosts both (same origin, no CORS)."""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from .core.config import get_settings
from .db import session
from .routers import approvals, ingestion, recovery, system

log = logging.getLogger("reconcile_api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        await session.init_db()
    except Exception as exc:  # noqa: BLE001 - the console still serves read views without ops state
        log.warning("ops-state database unavailable at startup: %s", exc)
    yield


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(title="Reconcile console API", version="0.3.0", lifespan=lifespan,
                  description="Self-Reconciling Data Pipeline POC: Phases 0–5 (stage 1: approvals)")
    if settings.env == "local":
        app.add_middleware(CORSMiddleware, allow_origins=["http://localhost:5173"], allow_methods=["*"],
                           allow_headers=["*"])
    app.include_router(system.router, prefix="/api")
    app.include_router(ingestion.router, prefix="/api")
    app.include_router(recovery.router, prefix="/api")
    app.include_router(approvals.router, prefix="/api")

    @app.get("/healthz", include_in_schema=False)
    def healthz():
        return {"ok": True}

    static = Path(settings.static_dir)
    if (static / "index.html").exists():
        app.mount("/assets", StaticFiles(directory=static / "assets"), name="assets")

        @app.get("/{path:path}", include_in_schema=False)
        def spa(path: str):
            if path.startswith("api/"):
                return JSONResponse({"detail": "Not Found"}, status_code=404)
            f = static / path
            return FileResponse(f if path and f.is_file() else static / "index.html")
    return app


app = create_app()
