"""Tests run against a real Postgres (the docker-compose one, or any server in TEST_PG_ADMIN_DSN).
A throwaway database is created and filled with a small synthetic business date."""
from __future__ import annotations

import os
import uuid
from datetime import date

import psycopg
import pytest

ADMIN_DSN = os.environ.get("TEST_PG_ADMIN_DSN", "postgresql://reconcile:reconcile@localhost:5433/postgres")
BUSINESS_DATE = date(2026, 10, 13)


@pytest.fixture(scope="session")
def source_dsn():
    try:
        admin = psycopg.connect(ADMIN_DSN, autocommit=True, connect_timeout=3)
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"Postgres not reachable at {ADMIN_DSN}: {exc}")
    name = f"src_test_{uuid.uuid4().hex[:8]}"
    admin.execute(f"create database {name}")
    dsn = ADMIN_DSN.rsplit("/", 1)[0] + f"/{name}"
    from datagen import generator
    generator.init(dsn, scale=0.02)
    generator.generate_day(dsn, date(2026, 10, 12), scale=0.02)
    generator.generate_day(dsn, BUSINESS_DATE, scale=0.02)
    yield dsn
    admin.execute(f"drop database {name} with (force)")
    admin.close()


@pytest.fixture
def settings(source_dsn, monkeypatch):
    from reconcile_ingest.settings import Settings
    monkeypatch.setenv("SOURCE_DSN", source_dsn)
    monkeypatch.setenv("ALLOW_INJECTIONS", "true")
    return Settings.from_env()


@pytest.fixture
def warehouse():
    from reconcile_ingest.warehouse import InMemoryWarehouse
    return InMemoryWarehouse()


@pytest.fixture
def archive(tmp_path):
    from reconcile_ingest.archive import LocalArchive
    return LocalArchive(tmp_path / "archive")
