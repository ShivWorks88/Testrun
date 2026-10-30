"""Phase 6 package 2 — wrong values (T6, G4) and bad releases (T14, G6), without BigQuery.

    value-level check fails → value_mismatch → value_corruption: report held, L3, never automatic
    value-level check missing for the day → reports built from lab data held (zero bad publishes)
    a grain/model test fails on a model a recent release changed → suspected_regression, release attached
"""
from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path

import pytest

pytest.importorskip("google.cloud.bigquery")
yaml = pytest.importorskip("yaml")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from reconcile_ingest import engine, publish, releases  # noqa: E402

POLICY = yaml.safe_load((Path(__file__).resolve().parents[2] / "policy" / "policy.yml").read_text())
BD = dt.date(2026, 10, 14)
DAYS = [BD - dt.timedelta(days=i) for i in (2, 1, 0)]


def failure(name, attached, rtype="test", failures=3):
    return {"unique_id": f"{rtype}.reconcile.{name}.abc", "name": name, "resource_type": rtype, "status": "fail",
            "failures": failures, "policy_outcome": "blocking", "attached_to": attached}


# ── value-level mismatch (T6, G4) ───────────────────────────────────────────
def test_value_check_failure_becomes_value_corruption():
    out = engine.findings(None, [failure("value_recon_lab_results_clean", "value_recon_lab_results", failures=14)],
                          DAYS)
    assert [(f["type"], f["dataset"], f["key"]) for f in out] == \
        [("value_mismatch", "lab_results", "value_mismatch:lab_results:2026-10-14")]
    d = engine.gates(out[0], POLICY, checks_ran=True, attempts=0)
    assert (d["rule_id"], d["route"], d["mode"]) == ("value_corruption", "L3", "escalate")   # never automatic


def test_evidence_guard_is_not_a_value_mismatch():
    out = engine.findings(None, [failure("evidence_has_no_values", "value_recon_lab_results")], DAYS)
    assert out[0]["type"] == "check_failed"


def _vresults(status, bd=BD):
    return {"args": {"vars": {"business_date": str(bd)}}, "metadata": {},
            "results": [{"unique_id": "test.reconcile.value_recon_lab_results_clean", "status": status,
                         "failures": 14 if status == "fail" else 0}]}


VMANIFEST = {"nodes": {"test.reconcile.value_recon_lab_results_clean": {
    "config": {"meta": {"policy_outcome": "blocking"}},
    "depends_on": {"nodes": ["model.reconcile.value_recon_lab_results"]}}}}


def test_gate_holds_lab_reports_when_values_differ_or_were_not_checked():
    assert publish.value_check_reasons(VMANIFEST, _vresults("pass"), BD) == {}
    bad = publish.value_check_reasons(VMANIFEST, _vresults("fail"), BD)
    assert list(bad) == ["lab_results"] and "FAIL (14)" in bad["lab_results"]
    assert "did not run" in publish.value_check_reasons(None, None, BD)["lab_results"]
    assert "did not run" in publish.value_check_reasons(VMANIFEST, _vresults("pass", BD - dt.timedelta(1)),
                                                        BD)["lab_results"]       # results of another day


def test_lab_report_held_other_reports_publish():
    def model(name, path, deps):
        return {"resource_type": "model", "name": name, "original_file_path": path,
                "depends_on": {"nodes": deps}, "config": {}}
    m = {"nodes": {
        "model.r.stg_lab_results": model("stg_lab_results", "models/staging/s.sql", ["source.r.raw.lab_results"]),
        "model.r.fct_lab_results_daily": model("fct_lab_results_daily", "models/marts/l.sql",
                                               ["model.r.stg_lab_results"]),
        "model.r.stg_encounters": model("stg_encounters", "models/staging/e.sql", ["source.r.raw.encounters"]),
        "model.r.dim_encounters": model("dim_encounters", "models/marts/d.sql", ["model.r.stg_encounters"])},
        "sources": {}}
    res = {"args": {"vars": {"business_date": str(BD)}}, "metadata": {},
           "results": [{"unique_id": u, "status": "success"} for u in m["nodes"]]}
    _, d = publish.decide(m, res, BD, low=publish.value_check_reasons(VMANIFEST, _vresults("fail"), BD))
    assert d["fct_lab_results_daily"][0] == "HELD" and "value-level" in d["fct_lab_results_daily"][1][0]
    assert d["dim_encounters"][0] == "PENDING"


# ── bad release (T14, G6) ───────────────────────────────────────────────────
REL = [{"release_id": "REL-1", "released_at": "2026-10-27T09:00:00", "git_sha": "1a2b3c",
        "changed_models": '["fct_daily_claims"]', "note": "PR 42"},
       {"release_id": "REL-0", "released_at": "2026-10-20T09:00:00", "git_sha": "0f0f0f",
        "changed_models": '["stg_encounters"]', "note": None}]


def test_release_attaches_only_to_checks_on_models_it_changed():
    found = engine.findings(None, [failure("fct_daily_claims_matches_staging", "fct_daily_claims,stg_claims"),
                                   failure("not_null_stg_lab_results_lab_result_id", "stg_lab_results")], DAYS)
    releases.mark(found, REL)
    assert found[0]["release"]["release_id"] == "REL-1" and found[0]["evidence"]["release"]["git_sha"] == "1a2b3c"
    assert "release" not in found[1]
    d = engine.gates(found[0], POLICY, checks_ran=True, attempts=0)
    assert (d["rule_id"], d["route"], d["mode"]) == ("suspected_regression", "L3", "escalate")
    d = engine.gates(found[1], POLICY, checks_ran=True, attempts=0)
    assert d["rule_id"] == "fallback_unknown"                       # no release → unknown, as before


def test_newest_release_wins_and_build_errors_count():
    assert releases.touching(REL, {"stg_encounters", "fct_daily_claims"})["release_id"] == "REL-1"
    assert releases.touching(REL, {"stg_claims"}) is None
    f = {"type": "build_error", "dataset": None, "models": ["dim_encounters"], "evidence": {"model": "dim_encounters"}}
    assert releases.scope_models(f) == {"dim_encounters"}
    assert releases.window_days(POLICY) == 7
