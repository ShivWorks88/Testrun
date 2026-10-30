from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone

import pytest
from app.core.config import Settings, get_settings
from app.deps import get_repo
from app.main import create_app
from app.routers.recovery import get_recovery_repo
from fastapi.testclient import TestClient

D = date(2026, 10, 13)
T0 = datetime(2026, 10, 23, 11, 37, tzinfo=timezone.utc)


class FakeOps:
    def available_dates(self, limit=14):
        return [date(2026, 10, 14), D]


GATES = [{"gate": i, "question": f"q{i}", "answer": "ok", "pass": True} for i in range(1, 6)]


class FakeRecovery:
    def recon_batches(self, d):
        base = {"business_date": d, "status": "LOADED", "mode": "extract", "injections": [], "finished_at": T0,
                "receipt_amount": None}
        return [
            {**base, "dataset": "encounters", "source_system": "EHR", "batch_id": "b-e", "rows_loaded": 9895,
             "receipt_rows": 10012, "receipt_keys": 10012, "injections": ["drop_hour"]},
            {**base, "dataset": "claims", "source_system": "BILLING", "batch_id": "b-c", "rows_loaded": 10012,
             "receipt_rows": 10000, "receipt_keys": 10000, "receipt_amount": "48123330.50"},
            {**base, "dataset": "patients", "source_system": "EHR", "batch_id": "b-p", "rows_loaded": 1500,
             "receipt_rows": 1500, "receipt_keys": 1500},
        ]

    def recon_checks(self, d):
        row = {"gate_run_id": "g1", "business_date": date(2026, 10, 14), "recorded_at": T0, "resource_type": "test",
               "failures": 0, "policy_outcome": "blocking"}
        return [
            {**row, "unique_id": "test.reconcile.reconcile_to_control_totals_stg_encounters_enc.47aa6bfe09",
             "status": "fail", "failures": 1, "attached_to": "stg_encounters,control_totals"},
            {**row, "unique_id": "test.reconcile.raw_claims_no_duplicates", "status": "warn", "failures": 12,
             "attached_to": "claims"},
            {**row, "unique_id": "test.reconcile.unique_stg_claims_claim_id.1a2b", "status": "pass",
             "attached_to": "stg_claims"},
            {**row, "unique_id": "test.reconcile.fct_daily_claims_matches_staging", "status": "pass",
             "attached_to": "fct_daily_claims"},
            {**row, "unique_id": "model.reconcile.stg_claims", "resource_type": "model", "status": "success"},
        ]

    def publication_cells(self, n_dates=14):
        def mk(ds, d, st):
            return {"dataset": ds, "business_date": d, "state": st, "reasons": "r", "decided_at": T0,
                    "snapshot": None}
        return [mk("fct_daily_claims", date(2026, 10, 12), "PUBLISHED"), mk("fct_daily_claims", D, "HELD"),
                mk("dim_encounters", D, "PUBLISHED")]

    def publication_log(self, limit=40):
        return [{"dataset": "fct_daily_claims", "business_date": D, "state": "HELD", "reasons": "claims FAIL (1)",
                 "decided_at": T0, "snapshot": None, "gate_run_id": "g1", "actor": "sa-publish@x"}]

    def incident_events(self, incident_id=None, limit=2000):
        def ev(i, m, st, att, gates=None):
            return {
                "incident_id": "INC-1", "incident_key": "partial_extract:encounters:2026-10-13", "business_date": D,
                "dataset": "encounters", "failure_type": "partial_extract", "status": st, "attempt": att,
                "action": "reextract_from_source", "mode": "auto", "note": st.lower(), "gate_run_id": "g1",
                "evidence": json.dumps({"missing_rows": 117}), "gates": json.dumps(gates) if gates else None,
                "at": T0 + timedelta(minutes=m)}
        rows = [ev(3, 2, "RESOLVED", 1), ev(2, 0.3, "ACTING", 1), ev(1, 0, "ACTING", 1, GATES)]
        return [r for r in rows if incident_id in (None, r["incident_id"])]


@pytest.fixture
def client(tmp_path, monkeypatch):
    settings = Settings(env="local", gcp_project="proj", database_url="", static_dir=str(tmp_path / "nostatic"))
    get_settings.cache_clear()
    import app.db.session as sess
    sess._engine = None
    sess._sessions = None
    monkeypatch.delenv("DATABASE_URL", raising=False)
    app = create_app()
    app.dependency_overrides[get_settings] = lambda: settings
    app.dependency_overrides[get_repo] = lambda: FakeOps()
    app.dependency_overrides[get_recovery_repo] = lambda: FakeRecovery()
    with TestClient(app) as c:
        yield c
    get_settings.cache_clear()


def test_reconciliation_maps_checks_to_datasets(client):
    body = client.get("/api/reconciliation", params={"date": "2026-10-13"}).json()
    by = {d["dataset"]: d for d in body["datasets"]}
    assert by["encounters"]["result"] == "fail" and by["encounters"]["counts_match"] is False
    assert by["encounters"]["checks"][0]["name"] == "reconcile_to_control_totals_stg_encounters_enc"  # hash dropped
    assert by["claims"]["result"] == "warn" and len(by["claims"]["checks"]) == 2
    assert by["patients"]["result"] == "not_checked" and by["patients"]["counts_match"] is True
    assert [c["name"] for c in body["other_checks"]] == ["fct_daily_claims_matches_staging"]
    assert body["gate_run_id"] == "g1" and body["checked_business_date"] == "2026-10-14"
    assert body["summary"] == {"fail": 1, "warn": 1, "not_checked": 1}


def test_publication_grid_and_last_verified(client):
    body = client.get("/api/publication").json()
    assert body["datasets"] == ["dim_encounters", "fct_daily_claims"]
    assert body["dates"] == ["2026-10-12", "2026-10-13"]
    assert body["last_verified"]["fct_daily_claims"] == "2026-10-12"   # 13 Oct is held
    assert body["log"][0]["state"] == "HELD"


def test_incidents_list_and_detail(client):
    [s] = client.get("/api/incidents").json()
    assert s["status"] == "RESOLVED" and s["attempt"] == 1 and s["minutes_open"] == 2.0 and s["events"] == 3
    d = client.get("/api/incidents/INC-1").json()
    assert [e["status"] for e in d["events"]] == ["ACTING", "ACTING", "RESOLVED"]   # oldest first
    assert len(d["events"][0]["gates"]) == 5 and d["events"][0]["evidence"] == {"missing_rows": 117}
    assert client.get("/api/incidents/NOPE").status_code == 404
