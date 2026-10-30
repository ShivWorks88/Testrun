"""Phase 6 hardening pass and the Phase 0 rebuild proof — the pure parts, without BigQuery or the source."""
from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path

import pytest

pytest.importorskip("google.cloud.bigquery")
yaml = pytest.importorskip("yaml")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from reconcile_ingest import hardening, rebuild_proof  # noqa: E402

POLICY = yaml.safe_load((Path(__file__).resolve().parents[2] / "policy" / "policy.yml").read_text())


def test_the_committed_policy_passes_every_guardrail_check():
    checks = hardening.policy_checks(POLICY)
    assert {c.status for c in checks} == {"PASS"}, [(c.name, c.detail) for c in checks if c.status != "PASS"]


@pytest.mark.parametrize("change, name, status", [
    ({"kill_switch": True}, "kill switch", "WARN"),
    ({"agents": {"enabled": True}}, "agents for real data", "FAIL"),
    ({"never_auto": ["full_refresh"]}, "never automatic", "FAIL"),
    ({"limits": {"attempt_cap": 9}}, "attempt cap", "FAIL"),
])
def test_a_weakened_policy_is_flagged(change, name, status):
    p = {**POLICY, **change}
    if "agents" in change:
        p["agents"] = {**POLICY["agents"], **change["agents"]}
    got = {c.name: c.status for c in hardening.policy_checks(p)}
    assert got[name] == status
    assert hardening.exit_code(hardening.policy_checks(p)) == (1 if status == "FAIL" else 0)


def test_model_armor_must_be_required_and_in_mumbai():
    p = {**POLICY, "agents": {**POLICY["agents"], "model_armor": {"template": "t", "location": "us-central1"}}}
    assert {c.name: c.status for c in hardening.policy_checks(p)}["Model Armor"] == "FAIL"


def test_paused_check_and_counts():
    assert hardening.paused_check([{"dag_id": "ingest_ehr", "is_paused": True},
                                   {"dag_id": "dbt_build", "is_paused": False}]).status == "PASS"
    assert hardening.paused_check([{"dag_id": "ingest_lis", "is_paused": "False"}]).status == "WARN"
    assert hardening.count_check("a", "b", None, "", "").status == "SKIP"
    assert hardening.count_check("a", "b", 2, "", "{n} open", "FAIL").detail == "2 open"


def test_rebuild_compare_finds_changed_and_missing_fingerprints():
    before = {"raw.claims@2026-10-14": {"rows": 10, "hash": "1"}, "analytics.fct_daily_claims@2026-10-14":
              {"rows": 3, "hash": "9"}, "raw.encounters@2026-10-14": {"rows": 5, "hash": "2"}}
    after = {"raw.claims@2026-10-14": {"rows": 10, "hash": "1"},
             "analytics.fct_daily_claims@2026-10-14": {"rows": 3, "hash": "8"}}
    diff, missing = rebuild_proof.compare(before, after)
    assert missing == ["raw.encounters@2026-10-14"]
    assert diff == ["analytics.fct_daily_claims@2026-10-14: rows 3 → 3 (same rows, different values)"]
    assert rebuild_proof.compare(before, before) == ([], [])


def test_days_between_is_inclusive_and_order_free():
    assert rebuild_proof.days_between("2026-10-14", "2026-10-12") == \
        [dt.date(2026, 10, 12), dt.date(2026, 10, 13), dt.date(2026, 10, 14)]
