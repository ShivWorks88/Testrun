"""Ops-state database (Cloud SQL for PostgreSQL). Phase 0/1 use it for the request journal;
Phase 2+ add incidents, publication state, locks and the action journal."""
from __future__ import annotations

import ssl
import uuid
from datetime import datetime, timezone

from sqlalchemy import JSON, DateTime, String, text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from ..core.config import get_settings


class Base(DeclarativeBase):
    pass


class RequestJournal(Base):
    """Every write request from the console, with who asked. Written before the request is queued."""
    __tablename__ = "request_journal"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    kind: Mapped[str] = mapped_column(String(64))
    payload: Mapped[dict] = mapped_column(JSON)
    requested_by: Mapped[str] = mapped_column(String(320))
    roles: Mapped[str] = mapped_column(String(128))
    status: Mapped[str] = mapped_column(String(32), default="received")
    pubsub_message_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True),
                                                 default=lambda: datetime.now(timezone.utc))


_engine: AsyncEngine | None = None
_sessions: async_sessionmaker | None = None


def engine() -> AsyncEngine | None:
    global _engine, _sessions
    if _engine is None:
        s = get_settings()
        url = s.sqlalchemy_url
        if not url:
            return None
        connect_args = {}
        if s.db_host and s.db_ssl and not s.database_url:
            ctx = ssl.create_default_context()
            ctx.check_hostname = False          # private IP; Cloud SQL server cert has no IP SAN
            ctx.verify_mode = ssl.CERT_NONE     # traffic stays inside the VPC; ENCRYPTED_ONLY on the server
            connect_args["ssl"] = ctx
        _engine = create_async_engine(url, pool_size=5, max_overflow=2, pool_pre_ping=True,
                                      connect_args=connect_args)
        _sessions = async_sessionmaker(_engine, expire_on_commit=False)
    return _engine


def sessions() -> async_sessionmaker | None:
    engine()
    return _sessions


async def init_db() -> None:
    e = engine()
    if e is None:
        return
    async with e.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def ping() -> tuple[bool, str]:
    e = engine()
    if e is None:
        return False, "not configured"
    try:
        async with e.connect() as conn:
            version = (await conn.execute(text("show server_version"))).scalar_one()
        return True, f"PostgreSQL {version}"
    except Exception as exc:  # noqa: BLE001
        return False, f"{type(exc).__name__}: {str(exc)[:160]}"
