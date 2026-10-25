from __future__ import annotations

import base64
import json
from datetime import date, datetime, timezone
from decimal import Decimal

import pytest
from app.core.config import Settings, get_settings
from app.deps import get_publisher, get_repo
from app.main import create_app
from fastapi.testclient import TestClient

D = date(2026, 10, 13)
NOW = datetime(2026, 10, 13, 20, 14, tzinfo=timezone.utc)


class FakeRepo:
    def latest_health(self):
        return [{"source_system": "EHR", "probed_at": NOW, "reachable": True, "latency_ms": 12.5,
                 "replica_lag_seconds": 0.0, "archive_cutoff_date": date(2026, 10, 6), "error": None},
                {"source_system": "LIS", "probed_at": NOW, "reachable": False, "latency_ms": None,
                 "replica_lag_seconds": None, "archive_cutoff_date": None, "error": "connection refused"}]

    def source_activity(self):
        return [{"source_system": "EHR", "last_business_date": D, "last_batch_at": NOW, "datasets_loaded": 3}]

    def available_dates(self, limit=14):
        return [D, date(2026, 10, 12)]

    def batches(self, d):
        base = {"source_system": "BILLING", "business_date": d, "mode": "extract", "status": "LOADED",
                "finished_at": NOW, "error": None, "injections": []}
        return [
            {**base, "batch_id": "b-20261013-claims-01", "dataset": "claims", "rows_extracted": 10000,
             "rows_loaded": 10012, "control_total_rows": 10000, "distinct_keys": 10000,
             "amount_total": Decimal("48123330.50"), "totals_rows": 1,
             "archive_uri": "gs://b/raw/claims/load_date=2026-10-13/b-20261013-claims-01.parquet",
             "injections": ["duplicate_rows"]},
            {**base, "source_system": "LIS", "batch_id": "b-20261013-lab_orders-01", "dataset": "lab_orders",
             "rows_extracted": 5228, "rows_loaded": 5228, "control_total_rows": None, "distinct_keys": None,
             "amount_total": None, "totals_rows": 0, "archive_uri": "gs://b/x.parquet",
             "injections": ["missing_control_totals"]},
        ]

    def latest_loaded(self):
        return []


class FakePublisher:
    def __init__(self):
        self.sent = []

    def topic_path(self, project, topic):
        return f"projects/{project}/topics/{topic}"

    def publish(self, topic, data, **attrs):
        self.sent.append((topic, json.loads(data), attrs))

        class F:
            def result(self, timeout=None):
                return "msg-42"
        return F()


def _token(email: str) -> str:
    body = base64.urlsafe_b64encode(json.dumps({"email": email}).encode()).decode().rstrip("=")
    return f"Bearer xx.{body}.sig"


@pytest.fixture
def make_client(tmp_path):
    def _make(env="local", role_map="", db=True):
        settings = Settings(env=env, gcp_project="proj", role_map=role_map,
                            database_url=f"sqlite+aiosqlite:///{tmp_path}/ops.db" if db else "",
                            static_dir=str(tmp_path / "nostatic"))
        get_settings.cache_clear()
        import app.db.session as sess
        sess._engine = None
        sess._sessions = None
        import os
        os.environ["DATABASE_URL"] = settings.database_url
        app = create_app()
        pub = FakePublisher()
        app.dependency_overrides[get_settings] = lambda: settings
        app.dependency_overrides[get_repo] = lambda: FakeRepo()
        app.dependency_overrides[get_publisher] = lambda: pub
        return TestClient(app), pub
    yield _make
    get_settings.cache_clear()


def test_local_user_has_all_roles(make_client):
    c, _ = make_client()
    with c:
        assert c.get("/api/me").json()["roles"] == ["data_owner", "engineer", "viewer"]


def test_cloud_requires_identity_and_maps_roles(make_client):
    c, _ = make_client(env="cloud", role_map="eng@x.com=engineer")
    with c:
        assert c.get("/api/me").status_code == 401
        assert c.get("/api/me", headers={"Authorization": _token("eng@x.com")}).json()["roles"] == \
            ["engineer", "viewer"]
        iap = {"X-Goog-Authenticated-User-Email": "accounts.google.com:someone@x.com"}
        assert c.get("/api/me", headers=iap).json() == {"email": "someone@x.com", "roles": ["viewer"]}


def test_sources_merge_health_and_activity(make_client):
    c, _ = make_client()
    with c:
        body = c.get("/api/sources").json()
    by = {s["system"]: s for s in body}
    assert set(by) == {"EHR", "LIS", "BILLING"}
    assert by["EHR"]["reachable"] is True and by["EHR"]["datasets_loaded"] == 3
    assert by["EHR"]["archive_cutoff_date"] == "2026-10-06"
    assert by["LIS"]["reachable"] is False and by["LIS"]["error"] == "connection refused"
    assert by["BILLING"]["reachable"] is None and by["BILLING"]["datasets"] == 3


def test_batches_show_totals_presence(make_client):
    c, _ = make_client()
    with c:
        body = c.get("/api/batches", params={"date": "2026-10-13"}).json()
    rows = {b["dataset"]: b for b in body["batches"]}
    assert rows["claims"]["totals_rows"] == 1 and rows["claims"]["rows_loaded"] == 10012
    assert rows["lab_orders"]["totals_rows"] == 0          # console shows "Missing"
    assert body["available_dates"][0] == "2026-10-13"


def test_replay_is_journaled_then_queued_never_executed(make_client):
    c, pub = make_client()
    with c:
        r = c.post("/api/batches/encounters/replay", json={"business_date": "2026-10-13", "reason": "gap 02-03"})
        assert r.status_code == 202, r.text
        body = r.json()
        assert body["status"] == "queued" and body["message_id"] == "msg-42"
    topic, payload, attrs = pub.sent[0]
    assert topic.endswith("/engine-requests") and payload["dataset"] == "encounters"
    assert payload["requested_by"] == "local-dev@localhost" and attrs["type"] == "replay_request"

    import asyncio

    from app.db import session as sess
    from app.db.session import RequestJournal

    async def read():
        async with sess.sessions()() as s:
            return await s.get(RequestJournal, body["request_id"])
    row = asyncio.run(read())
    assert row.status == "queued" and row.pubsub_message_id == "msg-42"


def test_replay_requires_engineer_role(make_client):
    c, pub = make_client(env="cloud", role_map="viewer@x.com=viewer")
    with c:
        r = c.post("/api/batches/encounters/replay", json={"business_date": "2026-10-13"},
                   headers={"Authorization": _token("viewer@x.com")})
    assert r.status_code == 403 and not pub.sent


def test_replay_unknown_dataset(make_client):
    c, _ = make_client()
    with c:
        assert c.post("/api/batches/nope/replay", json={"business_date": "2026-10-13"}).status_code == 404


def test_catalog_matches_ingest_registry():
    ingest = pytest.importorskip("reconcile_ingest.datasets")
    from app.services import catalog
    assert {n: d.source_system for n, d in ingest.DATASETS.items()} == \
        {n: s for n, (s, _) in catalog.DATASETS.items()}
    assert {k: v["label"] for k, v in ingest.SOURCE_SYSTEMS.items()} == \
        {k: v["label"] for k, v in catalog.SOURCE_SYSTEMS.items()}


def test_identity_matrix_reads_real_acls(monkeypatch):
    from types import SimpleNamespace as NS

    from app.services import environment as env
    s = Settings(gcp_project="proj")
    sa = s.component_accounts()
    acl = {
        "raw": [NS(entity_id=sa["ingest"], role="roles/bigquery.dataEditor"),
                NS(entity_id=sa["dbt"], role="roles/bigquery.dataViewer")],
        "audit": [NS(entity_id=sa["dbt"], role="WRITER"), NS(entity_id=sa["publish"], role="READER")],
        "analytics": [NS(entity_id=sa["publish"], role="roles/bigquery.dataEditor"),
                      NS(entity_id=sa["api"], role="roles/bigquery.dataViewer")],
    }
    monkeypatch.setattr(env, "_ops_table_writers", lambda settings: {sa["ingest"]})
    rows = {r.component: r.access for r in env.identity_matrix(s, acl)}
    assert rows["Ingest"] == {"raw": "Write", "audit": "—", "analytics": "—", "quarantine": "—",
                              "ops": "Write", "evidence_restricted": "—"}
    assert rows["Publish"]["analytics"] == "Write" and rows["Publish"]["audit"] == "Read"
    assert rows["Console API"]["analytics"] == "Read"
    assert set(rows["AI agents"].values()) == {"—"}       # no data access until Phase 5 masked views
    # nobody but publish can write analytics
    assert [c for c, a in rows.items() if a["analytics"] == "Write"] == ["Publish"]
