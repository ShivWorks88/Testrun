"""The policy checker must reject unsafe policies (it guards every policy pull request)."""
from __future__ import annotations

import copy
import importlib.util
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("check_policy", ROOT / "scripts" / "check_policy.py")
cp = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cp)

POLICY = yaml.safe_load((ROOT / "policy" / "policy.yml").read_text())
REGISTRY = yaml.safe_load((ROOT / "agents" / "registry.yml").read_text())


def errors(mutate):
    p = copy.deepcopy(POLICY)
    mutate(p)
    return cp.check(p, REGISTRY)


def test_committed_policy_is_valid():
    assert cp.check(POLICY, REGISTRY) == []


def test_never_auto_action_cannot_be_used():
    def m(p):
        p["rules"]["ordinary"][0]["action"] = "full_refresh"
    assert any("never-auto" in e for e in errors(m))


def test_unknown_action_rejected():
    def m(p):
        p["rules"]["ordinary"][1]["action"] = "drop_table"
    assert any("unknown action" in e for e in errors(m))


def test_fallback_must_be_last():
    def m(p):
        p["rules"]["ordinary"].insert(0, p["rules"]["ordinary"].pop())
    assert any("fallback" in e for e in errors(m))


def test_approval_actions_cannot_auto_run():
    def m(p):
        rule = next(r for r in p["rules"]["ordinary"] if r["action"] == "remap_merged_patient")
        rule["route"] = "auto"
    assert any("needs approval" in e for e in errors(m))


def test_agents_need_residency_confirmation():
    def m(p):
        p["agents"]["enabled"] = True
    assert any("residency" in e for e in errors(m))
