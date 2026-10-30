"""Phase 5 rest — Model Armor screening, the Communication agent and the ADK provider. No network.

    Model Armor  injected text blocked before the model is called; blocked answers rejected; a screening
                 error fails closed when required (T16 hardening)
    ADK          provider "adk" uses the Cloud Run path, the same validation and recommend_only (stage 3b)
    Comms        a summary is kept only if it adds no number, identifier, link or e-mail; quota → fallback model
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import pytest

yaml = pytest.importorskip("yaml")
pytest.importorskip("google.cloud.bigquery")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from reconcile_ingest import agent_gateway as ag  # noqa: E402
from reconcile_ingest import engine, model_armor  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
INCIDENT = {"id": "INC-20261014-abc123", "key": "staging_mismatch:claims:2026-10-14", "type": "staging_mismatch",
            "dataset": "claims", "business_date": "2026-10-14"}
EVIDENCE = {"raw_rows": 10012, "receipt_rows": 10012}
REG = {"model": "gemini-3.5-flash", "region": "asia-south1", "prompt_version": "diag-v2", "timeout_seconds": 20,
       "service_url": "https://reconcile-diagnosis-123.asia-south1.run.app"}
ESCALATE = json.dumps({"confirmed_facts": [{"statement": "raw equals receipt", "evidence_refs": ["ev-1"]}],
                       "hypotheses": [], "proposed_action": {"action": "escalate", "dataset": None,
                                                            "business_date": None}, "confidence": 0.9})


@pytest.fixture
def policy():
    return yaml.safe_load((ROOT / "policy" / "policy.yml").read_text())


@pytest.fixture(autouse=True)
def setup(monkeypatch):
    monkeypatch.setenv("ALLOW_INJECTIONS", "true")
    monkeypatch.setattr(ag, "load_registry", lambda path=None, agent_id="diagnosis": dict(REG))


def consult(policy, provider="vertex"):
    return ag.consult(incident=INCIDENT, evidence=EVIDENCE, check_names=["x"], policy=policy, provider=provider,
                      mode=None, attempts=0, implemented={"reextract_from_raw"}, actor="t", gate_run="g")


# ── the ADK package matches the gateway (one prompt, one action list) ───────
def test_adk_package_uses_the_same_prompt_and_actions():
    pkg = ROOT / "agents" / "reconcile_agents"
    assert (pkg / "prompts" / "diag-v2.txt").read_text(encoding="utf-8") == ag.DIAG_INSTRUCTION
    src = (pkg / "agent.py").read_text(encoding="utf-8")
    literal = re.search(r"Action = Literal\[(.*?)\]", src).group(1)
    assert tuple(re.findall(r'"(\w+)"', literal)) == ag.MAY_PROPOSE
    assert "tools=" not in src                              # no tools: the agent sees only the payload


# ── Model Armor ──────────────────────────────────────────────────────────────
def test_policy_screens_every_real_call_in_mumbai(policy):
    cfg = model_armor.config(policy)
    assert cfg == {"template": "reconcile-agents", "location": "asia-south1", "required": True}


def test_injected_text_is_blocked_before_the_model(policy, monkeypatch):
    called = []
    monkeypatch.setattr(model_armor, "screen", lambda c, kind, t, timeout_s=8.0:
                        ("blocked", ["pi_and_jailbreak"]) if kind == "prompt" else ("clean", []))
    monkeypatch.setattr(ag, "vertex_agent", lambda p, r, t: called.append(1) or (ESCALATE, "m"))
    r = consult(policy)
    assert not called                                         # the model never saw the text
    assert r["call"]["reason"] == "model_armor_prompt" and r["decision"]["mode"] == "escalate"
    assert "pi_and_jailbreak" in r["call"]["output"]


def test_blocked_answer_and_screening_errors_fail_closed(policy, monkeypatch):
    monkeypatch.setattr(ag, "vertex_agent", lambda p, r, t: (ESCALATE, "m"))
    monkeypatch.setattr(model_armor, "screen", lambda c, kind, t, timeout_s=8.0:
                        ("blocked", ["sdp"]) if kind == "response" else ("clean", []))
    assert consult(policy)["call"]["reason"] == "model_armor_response"
    monkeypatch.setattr(model_armor, "screen", lambda c, kind, t, timeout_s=8.0: ("error", ["http_403"]))
    assert consult(policy)["call"]["reason"] == "model_armor_error"
    lax = {**policy, "agents": {**policy["agents"], "model_armor": {**policy["agents"]["model_armor"],
                                                                    "required": False}}}
    assert consult(lax)["call"]["validation"] == "accepted"    # not required: an outage does not block


def test_fake_agent_is_never_screened(policy, monkeypatch):
    monkeypatch.setattr(model_armor, "screen", lambda *a, **k: pytest.fail("fake agent must not be screened"))
    r = ag.consult(incident=INCIDENT, evidence=EVIDENCE, check_names=["x"], policy=policy, provider="fake",
                   mode="escalate", attempts=0, implemented=set(), actor="t", gate_run="g")
    assert r["call"]["provider"] == "fake"


def test_matched_filters_names_only():
    res = {"sanitizationResult": {"filterMatchState": "MATCH_FOUND", "filterResults": {
        "pi_and_jailbreak": {"piAndJailbreakFilterResult": {"matchState": "MATCH_FOUND"}},
        "sdp": {"sdpFilterResult": {"inspectResult": {"matchState": "NO_MATCH_FOUND"}}}}}}
    assert model_armor.matched_filters(res) == ["pi_and_jailbreak"]
    assert model_armor.matched_filters({"sanitizationResult": {"filterMatchState": "NO_MATCH_FOUND"}}) == []


# ── ADK provider (stage 3b) ─────────────────────────────────────────────────
def test_adk_provider_same_gate_and_validation(policy, monkeypatch):
    monkeypatch.setattr(model_armor, "screen", lambda *a, **k: ("clean", []))
    assert ag.agents_allowed(policy, None, "adk")[0] == "adk"           # synthetic trial, test project
    monkeypatch.setattr(ag, "vertex_agent", lambda *a: pytest.fail("adk must not call Vertex directly"))
    monkeypatch.setattr(ag, "adk_agent", lambda p, r, t: (ag.fake_agent(p, "valid"), "adk:gemini-3.5-flash"))
    r = consult(policy, "adk")
    assert r["call"]["provider"] == "adk" and r["call"]["model"] == "adk:gemini-3.5-flash"
    assert r["decision"]["mode"] == "recommend"                          # recommend_only covers adk too
    monkeypatch.setenv("ALLOW_INJECTIONS", "false")
    assert ag.agents_allowed(policy, None, "adk")[0] is None             # never outside the test project


def test_adk_without_service_url_fails_closed():
    raw, _ = ag.adk_agent({"x": 1}, {**REG, "service_url": None}, 5)
    assert "_platform_error" in raw


# ── Communication agent ─────────────────────────────────────────────────────
CARD = {"title": "Re-extract encounters for 2026-10-14 from the source",
        "what_happened": "The batch now in raw has no source receipt.",
        "evidence": {"raw_rows": 10012, "receipt_rows": None},
        "impact": "Publication for 2026-10-14 is held for: dim_encounters.",
        "if_approved": "The engine re-extracts encounters 2026-10-14.", "if_rejected": "Nothing runs."}


def test_summary_may_only_restate_the_card():
    ok = "Encounters for 2026-10-14 loaded 10,012 rows without a receipt. Approving re-extracts that day."
    assert ag.check_summary(ok, CARD) is None
    assert ag.check_summary("About 12,000 rows are missing.", CARD) == "new_number"
    assert ag.check_summary("Patient P-000123 is affected.", CARD) == "identifier"
    assert ag.check_summary("See https://example.com for more.", CARD) == "link_or_email"
    assert ag.check_summary("", CARD) == "length"


def test_communicate_falls_back_on_quota_and_drops_bad_text(policy, monkeypatch):
    monkeypatch.setattr(ag, "load_registry", lambda path=None, agent_id="diagnosis": {
        "model": "gemini-3.5-flash-lite", "fallback_model": "gemini-3.5-flash", "region": "asia-south1",
        "prompt_version": "comms-v1", "timeout_seconds": 10})
    tried = []

    def gen(model, region, system, text, cfg, timeout_s, principal="agents"):
        tried.append(model)
        if model.endswith("lite"):
            return 429, {}
        return 200, {"candidates": [{"content": {"parts": [{"text": "Raw has 10012 rows and no receipt."}]}}]}
    monkeypatch.setattr(ag, "generate", gen)
    inc = {"id": "INC-1", "key": "k", "business_date": "2026-10-14", "dataset": "encounters"}
    summary, call = ag.communicate(CARD, policy, "vertex", inc, "t", "g")
    assert tried == ["gemini-3.5-flash-lite", "gemini-3.5-flash"] and summary.startswith("Raw has 10012")
    assert call["agent_id"] == "communication" and call["model"] == "gemini-3.5-flash"
    monkeypatch.setattr(ag, "generate", lambda *a, **k: (200, {"candidates": [{"content": {"parts": [
        {"text": "Roughly 9,999 rows are wrong."}]}}]}))
    summary, call = ag.communicate(CARD, policy, "vertex", inc, "t", "g")
    assert summary is None and call["reason"] == "summary_new_number"


def test_card_gets_summary_only_with_a_real_agent(policy, monkeypatch):
    monkeypatch.setattr(ag, "communicate", lambda card, pol, prov, inc, actor, gr: (
        "Approving re-extracts encounters 2026-10-14.", {"model": "gemini-3.5-flash", "prompt_version": "comms-v1"}))
    card, calls = dict(CARD), []
    engine.add_summary(card, policy, "vertex", {"id": "INC-1"}, "t", "g", calls)
    assert card["summary"].startswith("Approving") and "wording only" in card["summary_by"] and calls
    card = dict(CARD)
    engine.add_summary(card, policy, "fake-anything", {"id": "INC-1"}, "t", "g", [])
    assert "summary" not in card
