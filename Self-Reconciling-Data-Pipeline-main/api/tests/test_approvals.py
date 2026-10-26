"""Phase 5 stage 1: who may decide, and when. The console records decisions; it never acts."""
from __future__ import annotations

import base64
import json
from datetime import date, datetime, timedelta, timezone

import pytest
from app.core.config import Settings, get_settings
from app.deps import get_repo
from app.main import create_app
from app.routers.approvals import get_approvals_repo
from fastapi.testclient import TestClient

NOW = datetime.now(timezone.utc)
OWNER, VIEWER, ENGINE = "owner@example.com", "viewer@example.com", "sa-engine@proj.iam.gserviceaccount.com"


def card(aid: str, status: str = "PENDING", expires: datetime | None = None) -> dict:
    return {"approval_id": aid, "incident_id": f"INC-{aid}", "incident_key": "missing_receipt:encounters:2026-10-14",
            "business_date": date(2026, 10, 14), "dataset": "encounters", "action": "reextract_from_raw",
            "params": json.dumps({"dataset": "encounters", "business_date": "2026-10-14", "dag_id": "ingest_ehr"}),
            "status": status, "fingerprint": "fp-1", "policy_version": "1.7.0-poc.2",
            "rule_id": "control_totals_missing", "severity": "P2", "route": "L2", "attempt": 1,
            "card": json.dumps({"title": "Re-extract encounters", "evidence": {"raw_rows": 10012},
                                "guardrails": ["attempt 1 of 2"]}),
            "requested_by": ENGINE, "requested_at": NOW - timedelta(minutes=5),
            "expires_at": expires or NOW + timedelta(minutes=115), "note": "waiting", "at": NOW}


class FakeApprovals:
    def __init__(self):
        self.rows = {"A1": card("A1"), "A2": card("A2", expires=NOW - timedelta(minutes=1)),
                     "A3": card("A3", status="REJECTED")}
        self.decs: dict[str, dict] = {}

    def approvals(self, limit=100):
        return list(self.rows.values())

    def approval(self, approval_id):
        return self.rows.get(approval_id)

    def decisions(self, ids):
        return {i: self.decs[i] for i in ids if i in self.decs}

    def decision(self, approval_id):
        return self.decs.get(approval_id)

    def insert_decision(self, row):
        self.decs[row["approval_id"]] = {**row, "decided_at": datetime.fromisoformat(row["decided_at"])}


class FakeOps:
    def available_dates(self, limit=14):
        return []


def bearer(email: str) -> dict:
    payload = base64.urlsafe_b64encode(json.dumps({"email": email}).encode()).decode().rstrip("=")
    return {"Authorization": f"Bearer x.{payload}.sig"}


@pytest.fixture
def setup(tmp_path, monkeypatch):
    settings = Settings(env="cloud", gcp_project="proj", database_url="", static_dir=str(tmp_path / "x"),
                        role_map=f"{OWNER}=viewer,data_owner;{ENGINE}=viewer,data_owner")
    get_settings.cache_clear()
    import app.db.session as sess
    sess._engine = None
    sess._sessions = None
    monkeypatch.delenv("DATABASE_URL", raising=False)
    fake = FakeApprovals()
    app = create_app()
    app.dependency_overrides[get_settings] = lambda: settings
    app.dependency_overrides[get_repo] = lambda: FakeOps()
    app.dependency_overrides[get_approvals_repo] = lambda: fake
    with TestClient(app) as c:
        yield c, fake
    get_settings.cache_clear()


def post(c, aid, who, decision="APPROVE", fp="fp-1", reason=""):
    return c.post(f"/api/approvals/{aid}/decision", headers=bearer(who),
                  json={"decision": decision, "fingerprint": fp, "reason": reason})


def test_list_shows_card_and_who_can_decide(setup):
    c, _ = setup
    by = {a["approval_id"]: a for a in c.get("/api/approvals", headers=bearer(OWNER)).json()}
    assert by["A1"]["can_decide"] and by["A1"]["card"]["evidence"] == {"raw_rows": 10012}
    assert by["A2"]["shown_status"] == "EXPIRING" and not by["A2"]["can_decide"]
    assert by["A3"]["why_not"] == "card is REJECTED"
    viewer = {a["approval_id"]: a for a in c.get("/api/approvals", headers=bearer(VIEWER)).json()}
    assert viewer["A1"]["why_not"] == "only a data owner can approve or reject"


def test_owner_approves_once(setup):
    c, fake = setup
    r = post(c, "A1", OWNER)
    assert r.status_code == 202 and r.json()["decided_by"] == OWNER
    assert fake.decs["A1"]["decision"] == "APPROVE"
    assert post(c, "A1", OWNER).status_code == 409                       # first decision wins
    shown = {a["approval_id"]: a for a in c.get("/api/approvals", headers=bearer(OWNER)).json()}
    assert shown["A1"]["shown_status"] == "DECIDED"


def test_who_and_when_rules(setup):
    c, fake = setup
    assert post(c, "A1", VIEWER).status_code == 403                      # not a data owner
    assert post(c, "A1", ENGINE).status_code == 409                      # requester / service account
    assert post(c, "A2", OWNER).status_code == 409                       # expired
    assert post(c, "A1", OWNER, fp="old").status_code == 409             # saw a different card
    assert post(c, "A1", OWNER, decision="REJECT").status_code == 422    # rejection needs a reason
    assert post(c, "NOPE", OWNER).status_code == 404
    assert fake.decs == {}
    assert post(c, "A1", OWNER, decision="REJECT", reason="source re-sent the file").status_code == 202
