"""Phase 5 stage 1 — engine side of approvals, without BigQuery (all reads are faked).

Run:  cd ingest && python -m pytest -q tests/test_approvals_engine.py
"""
from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path

import pytest

pytest.importorskip("google.cloud.bigquery")
yaml = pytest.importorskip("yaml")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from reconcile_ingest import engine  # noqa: E402

POLICY_FILE = Path(__file__).resolve().parents[2] / "policy" / "policy.yml"
POLICY_VERSION = yaml.safe_load(POLICY_FILE.read_text())["version"]   # the card is written under the live policy
NOW = dt.datetime.now(dt.timezone.utc)
EVIDENCE = {"business_date": "2026-10-14", "diagnosis": "missing_receipt", "batch_id": "b-1", "receipt_rows": None,
            "raw_rows": 10012, "receipt_keys": None, "raw_keys": 10012, "missing_rows": None, "missing_keys": None}
PARAMS = {"dataset": "encounters", "business_date": "2026-10-14", "dag_id": "ingest_ehr"}


@pytest.fixture
def policy():
    return yaml.safe_load(POLICY_FILE.read_text())


def test_missing_receipt_waits_for_approval(policy):
    d = engine.gates({"type": "missing_receipt"}, policy, checks_ran=True, attempts=0)
    assert (d["mode"], d["action"], d["route"], d["rule_id"]) == \
        ("approve", "reextract_from_raw", "L2", "control_totals_missing")
    assert engine.gates({"type": "missing_receipt"}, {**policy, "kill_switch": True}, True, 0)["mode"] == "recommend"
    assert engine.gates({"type": "missing_receipt"}, policy, True, attempts=2)["mode"] == "escalate"
    # auto rules are unchanged
    assert engine.gates({"type": "partial_extract"}, policy, True, 0)["mode"] == "auto"
    assert engine.gates({"type": "unit_or_range"}, policy, True, 0)["mode"] == "escalate"


def test_fingerprint_changes_with_any_fact():
    fp = engine.fingerprint("INC-1", "reextract_from_raw", PARAMS, EVIDENCE, "1.7.0-poc.2")
    assert fp == engine.fingerprint("INC-1", "reextract_from_raw", dict(PARAMS), dict(EVIDENCE), "1.7.0-poc.2")
    assert fp != engine.fingerprint("INC-1", "reextract_from_raw", PARAMS, {**EVIDENCE, "batch_id": "b-2"},
                                    "1.7.0-poc.2")
    assert fp != engine.fingerprint("INC-1", "reextract_from_raw", PARAMS, EVIDENCE, "1.7.0-poc.3")


def _approval(**kw):
    a = {"approval_id": "APR-1", "incident_id": "INC-1", "incident_key": "missing_receipt:encounters:2026-10-14",
         "business_date": dt.date(2026, 10, 14), "dataset": "encounters", "action": "reextract_from_raw",
         "params": engine.json.dumps(PARAMS), "status": "PENDING", "policy_version": POLICY_VERSION,
         "rule_id": "control_totals_missing", "severity": "P2", "route": "L2", "attempt": 1, "card": "{}",
         "gate_run_id": "g1", "requested_by": "sa-engine@p.iam.gserviceaccount.com",
         "requested_at": NOW - dt.timedelta(minutes=5), "expires_at": NOW + dt.timedelta(minutes=115),
         "decided_by": None, "decision_reason": None, "note": "waiting", "at": NOW}
    a["fingerprint"] = engine.fingerprint("INC-1", a["action"], PARAMS, EVIDENCE, a["policy_version"])
    return {**a, **kw}


def _decision(a, **kw):
    return {"approval_id": a["approval_id"], "decision": "APPROVE", "fingerprint": a["fingerprint"],
            "decided_by": "owner@example.com", "decided_at": NOW, "reason": None, **kw}


@pytest.fixture
def fake_reads(monkeypatch):
    state = {"evidence": EVIDENCE, "attempts": 0}
    monkeypatch.setattr(engine, "attempts_so_far", lambda bq, i: state["attempts"])
    monkeypatch.setattr(engine, "diagnose", lambda bq, ds, days: [state["evidence"]])
    return state


def test_recheck_passes_then_catches_each_change(policy, fake_reads):
    a = _approval()
    assert engine.recheck(None, a, _decision(a), policy) == []
    assert "kill switch" in engine.recheck(None, a, _decision(a), {**policy, "kill_switch": True})[0]
    assert "different card" in engine.recheck(None, a, _decision(a, fingerprint="x"), policy)[0]
    assert "named person" in engine.recheck(None, a, _decision(a, decided_by=a["requested_by"]), policy)[0]
    late = _decision(a, decided_at=a["expires_at"] + dt.timedelta(seconds=1))
    assert "expired" in engine.recheck(None, a, late, policy)[0]
    assert "policy changed" in engine.recheck(None, a, _decision(a), {**policy, "version": "9.9.9-test"})[0]
    fake_reads["evidence"] = {**EVIDENCE, "batch_id": "b-2"}  # someone re-ingested meanwhile
    assert "evidence changed" in engine.recheck(None, a, _decision(a), policy)[-1]
    fake_reads["evidence"], fake_reads["attempts"] = EVIDENCE, 2
    assert any("attempt limit" in p for p in engine.recheck(None, a, _decision(a), policy))


@pytest.fixture
def worker(monkeypatch, policy, fake_reads):
    out = {"cards": [], "incs": []}
    monkeypatch.setattr(engine, "client_as", lambda name: None)
    monkeypatch.setattr(engine, "ensure_table", lambda bq: None)
    monkeypatch.setattr(engine, "service_account", lambda name: "sa-engine@p.iam.gserviceaccount.com")
    monkeypatch.setattr(engine, "record_approvals", lambda bq, rows: out["cards"].extend(rows))
    monkeypatch.setattr(engine, "record", lambda bq, rows: out["incs"].extend(rows))
    monkeypatch.setattr(engine, "_latest_incident", lambda bq, i: {"status": "AWAITING_APPROVAL", "attempt": 0})

    def run(approval, decision=None, later=0):
        out["cards"].clear()
        out["incs"].clear()
        monkeypatch.setattr(engine, "_latest_approvals", lambda bq: [approval])
        found = {approval["approval_id"]: decision} if decision else {}
        monkeypatch.setattr(engine, "_decisions", lambda bq, ids: found)
        return engine.process_approvals(later, policy=policy)
    out["run"] = run
    return out


def test_worker_approve_reject_expire(worker):
    a = _approval()
    acts = worker["run"](a, _decision(a))
    assert [c["status"] for c in worker["cards"]] == ["APPROVED"]
    assert worker["incs"][0]["status"] == "ACTING" and acts[0]["dataset"] == "encounters" and acts[0]["attempt"] == 1

    assert worker["run"](a, _decision(a, decision="REJECT", reason="source re-sends")) == []
    assert worker["cards"][0]["status"] == "REJECTED" and worker["incs"][0]["status"] == "ESCALATED"

    assert worker["run"](a) == [] and worker["cards"] == []              # pending, still time left
    assert worker["run"](a, later=180) == []                              # nobody answered in time
    assert worker["cards"][0]["status"] == "EXPIRED" and worker["incs"][0]["status"] == "ESCALATED"

    assert worker["run"](a, _decision(a, fingerprint="old")) == []        # approved the wrong card
    assert worker["cards"][0]["status"] == "STALE" and worker["incs"][0]["status"] == "ESCALATED"
