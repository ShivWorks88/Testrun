"""Phase 6 package 1 — outage breaker, freshness and the gate's source holds, without BigQuery.

Run:  cd ingest && python -m pytest -q tests/test_breaker.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

pytest.importorskip("google.cloud.bigquery")
pytest.importorskip("pyarrow")
yaml = pytest.importorskip("yaml")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from reconcile_ingest import engine, publish  # noqa: E402
from reconcile_ingest import source_state as ss  # noqa: E402

POLICY = yaml.safe_load((Path(__file__).resolve().parents[2] / "policy" / "policy.yml").read_text())
RULE = ss.open_rule(POLICY, "BILLING")


# ── opening and closing (T19, T21) ──────────────────────────────────────────
def test_breaker_opens_at_three_datasets_or_half_the_inventory():
    assert ss.should_open(["claims", "payments", "adjustments"], 3, RULE)[0]
    assert ss.should_open(["claims", "payments"], 3, RULE)[0]            # 67 % ≥ 50 %
    assert not ss.should_open(["claims"], 3, RULE)[0]                     # one dataset = not an outage
    assert not ss.should_open([], 3, RULE)[0]
    assert ss.should_open([], 3, RULE, probe_down=True)[0]               # unreachable source


def test_probe_alone_never_closes_the_breaker():
    assert not ss.should_close(probe_ok_after_open=False, not_reloaded=[])[0]
    closed, why = ss.should_close(probe_ok_after_open=True, not_reloaded=["payments"])
    assert not closed and "freshness fails" in why                       # T21
    assert ss.should_close(probe_ok_after_open=True, not_reloaded=[])[0]


def test_policy_defines_breaker_and_freshness():
    assert RULE == {"failing_datasets": 3, "or_share_of_inventory": 0.5, "within_minutes": 30}
    assert ss.global_threshold(POLICY) == 2
    assert ss.freshness(POLICY)["max_replica_lag_hours"] == 6


# ── late data (late documentation) ──────────────────────────────────────────
def test_low_volume_needs_a_baseline():
    assert ss.is_low_volume(3500, [10000, 10100, 9900], 0.5) == (True, 0.35)
    assert ss.is_low_volume(9000, [10000, 10100, 9900], 0.5)[0] is False
    assert ss.is_low_volume(10, [10000], 0.5) == (False, None)            # one earlier day is not enough
    assert ss.is_low_volume(None, [1, 2, 3], 0.5) == (False, None)


# ── engine decisions ────────────────────────────────────────────────────────
def test_open_breaker_holds_every_finding_on_that_source():
    for kind in ("extract_failed", "partial_extract", "missing_receipt", "check_failed"):
        d = engine.gates({"type": kind, "breaker": "INC-1"}, POLICY, checks_ran=True, attempts=0)
        assert (d["mode"], d["rule_id"]) == ("hold", "outage_breaker_open"), kind
        assert d["trace"][1]["pass"] is False                              # gate 2: evidence not trustworthy


def test_without_breaker_sources_follow_their_rules():
    d = engine.gates({"type": "extract_failed"}, POLICY, True, 0)
    assert (d["mode"], d["rule_id"]) == ("escalate", "extract_failed")
    for kind, rule in (("stale_feed", "stale_feed"), ("late_data", "late_documentation")):
        d = engine.gates({"type": kind}, POLICY, True, 0)
        assert (d["mode"], d["action"], d["rule_id"]) == ("wait", "wait_and_recheck", rule)
        assert engine.gates({"type": kind}, POLICY, True, attempts=2)["mode"] == "escalate"   # cap reached
        assert engine.gates({"type": kind}, {**POLICY, "kill_switch": True}, True, 0)["mode"] == "recommend"


def test_mark_breakers_by_source_and_global():
    found = [{"dataset": "claims"}, {"dataset": "encounters"}, {"dataset": None, "source": "BILLING"}]
    engine.mark_breakers(found, {"BILLING": {"source_system": "BILLING", "parent_incident_id": "INC-B"}})
    assert [f.get("breaker") for f in found] == ["INC-B", None, "INC-B"]
    found = [{"dataset": "encounters"}, {"dataset": None}]
    engine.mark_breakers(found, {"GLOBAL": {"source_system": "GLOBAL", "parent_incident_id": "INC-G"}})
    assert [f.get("breaker") for f in found] == ["INC-G", "INC-G"]


# ── publication gate (T20) ──────────────────────────────────────────────────
def _manifest():
    def model(name, path, deps):
        return {"resource_type": "model", "name": name, "original_file_path": path,
                "depends_on": {"nodes": deps}, "config": {}}
    nodes = {
        "model.r.stg_claims": model("stg_claims", "models/staging/stg_claims.sql", ["source.r.raw.claims"]),
        "model.r.stg_encounters": model("stg_encounters", "models/staging/stg_encounters.sql",
                                        ["source.r.raw.encounters"]),
        "model.r.dim_encounters": model("dim_encounters", "models/marts/dim_encounters.sql",
                                        ["model.r.stg_encounters"]),
        "model.r.fct_daily_claims": model("fct_daily_claims", "models/marts/fct_daily_claims.sql",
                                          ["model.r.stg_claims", "model.r.dim_encounters"]),
    }
    return {"nodes": nodes, "sources": {"source.r.raw.claims": {}, "source.r.raw.encounters": {}}}


def _results(bd):
    return {"args": {"vars": {"business_date": str(bd)}}, "metadata": {},
            "results": [{"unique_id": u, "status": "success"} for u in _manifest()["nodes"]]}


def test_gate_holds_reports_built_from_a_down_source():
    import datetime as dt
    bd = dt.date(2026, 10, 14)
    m = _manifest()
    assert publish.mart_upstream(m) == {"dim_encounters": ({"EHR"}, {"encounters"}),
                                        "fct_daily_claims": ({"BILLING", "EHR"}, {"claims", "encounters"})}
    _, d = publish.decide(m, _results(bd), bd)
    assert {k: v[0] for k, v in d.items()} == {"dim_encounters": "PENDING", "fct_daily_claims": "PENDING"}
    _, d = publish.decide(m, _results(bd), bd, blocked={"BILLING": "outage breaker OPEN for BILLING"})
    assert d["fct_daily_claims"][0] == "HELD" and "outage breaker" in d["fct_daily_claims"][1][0]
    assert d["dim_encounters"][0] == "PENDING"                            # EHR keeps publishing
    _, d = publish.decide(m, _results(bd), bd, blocked={"GLOBAL": "global outage"})
    assert all(v[0] == "HELD" for v in d.values())                         # global: hold everything
    _, d = publish.decide(m, _results(bd), bd, low={"claims": "late data: claims"})
    assert d["fct_daily_claims"][0] == "HELD" and d["dim_encounters"][0] == "PENDING"
