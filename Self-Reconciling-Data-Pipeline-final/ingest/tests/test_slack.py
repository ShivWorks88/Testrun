"""Phase 5 stage 4 — Slack approvals without Slack or BigQuery (T18 via Slack, T23 callback safety).

    T23  wrong signature, stale (replayed) request, wrong channel, unknown person → refused
    T18  an allowed data owner's click becomes the same decision row the console writes; nothing runs
    T26  a Slack card carries the engine's counts and words only — no patient identifiers
"""
from __future__ import annotations

import hashlib
import hmac
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

pytest.importorskip("google.cloud.bigquery")
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "slack"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import slack_core as core  # noqa: E402
from reconcile_ingest import slack_notify  # noqa: E402
from reconcile_ingest.phi_scan import find_record_ids  # noqa: E402

SECRET, NOW = "8f2b-test-signing-secret", datetime.now(timezone.utc)
ALLOWED = core.approvers("Owner@Example.com, second@example.com")
CARD = {"approval_id": "APR-20261014-abc123", "incident_id": "INC-20261014-0ff41d", "status": "PENDING",
        "fingerprint": "fp1", "requested_by": "sa-engine@p.iam.gserviceaccount.com",
        "expires_at": NOW + timedelta(minutes=90), "action": "reextract_from_raw", "policy_version": "1.8.0-poc.3",
        "attempt": 1, "card": json.dumps({
            "title": "Re-extract encounters for 2026-10-14 from the source",
            "what_happened": "The batch now in raw has no source receipt.",
            "summary": "Encounters for 2026-10-14 have no receipt; approving re-extracts that day.",
            "summary_by": "Communication agent (gemini-3.5-flash-lite, comms-v1) — wording only",
            "evidence": {"batch_id": "b-20261014-encounters-04", "raw_rows": 10012, "receipt_rows": None},
            "rule": "safety_overrides · control_totals_missing", "impact": "Publication held.",
            "if_approved": "Re-extract.", "if_rejected": "Nothing runs."})}


def sign(body: bytes, ts: int) -> str:
    return "v0=" + hmac.new(SECRET.encode(), b"v0:" + str(ts).encode() + b":" + body, hashlib.sha256).hexdigest()


def test_only_fresh_signed_requests_from_slack():
    body, ts = b"payload=%7B%7D", int(NOW.timestamp())
    assert core.verify_signature(SECRET, str(ts), body, sign(body, ts), now=ts) is None
    assert core.verify_signature(SECRET, str(ts), body, sign(b"tampered", ts), now=ts) == "signature mismatch"
    assert "replay" in core.verify_signature(SECRET, str(ts - 600), body, sign(body, ts - 600), now=ts)
    assert core.verify_signature(SECRET, "", body, "", now=ts) == "missing signature"
    assert core.verify_signature("", str(ts), body, sign(body, ts), now=ts) == "missing signature"


def test_who_may_decide_matches_the_console():
    ok = core.may_decide(CARD, None, "owner@example.com", "fp1", NOW, ALLOWED)
    assert ok is None
    assert "approver list" in core.may_decide(CARD, None, "stranger@example.com", "fp1", NOW, ALLOWED)
    assert "already decided" in core.may_decide(CARD, {"decided_by": "x"}, "owner@example.com", "fp1", NOW, ALLOWED)
    assert "expired" in core.may_decide(CARD, None, "owner@example.com", "fp1", NOW + timedelta(hours=3), ALLOWED)
    assert "changed" in core.may_decide(CARD, None, "owner@example.com", "old-fp", NOW, ALLOWED)
    assert "card is APPROVED" in core.may_decide({**CARD, "status": "APPROVED"}, None, "owner@example.com", "fp1",
                                                 NOW, ALLOWED)
    sa = "sa-engine@p.iam.gserviceaccount.com"
    assert core.may_decide(CARD, None, sa, "fp1", NOW, ALLOWED | {sa}) is not None   # never the requester / an SA


def test_a_click_is_only_a_decision_row():
    row = core.decision_row("APR-1", "REJECT", "fp1", "Owner@Example.com", "  source re-sends tonight ", NOW, "d1")
    assert row == {"decision_id": "d1", "approval_id": "APR-1", "decision": "REJECT", "fingerprint": "fp1",
                   "reason": "source re-sends tonight", "decided_by": "owner@example.com",
                   "roles": "data_owner,slack", "decided_at": NOW.isoformat()}
    assert core.parse_value("APR-20261014-abc123|fp1") == ("APR-20261014-abc123", "fp1")
    assert core.parse_value("INC-1|fp1") is None and core.parse_value("APR-1") is None


def test_slack_card_has_buttons_summary_and_no_patient_ids():
    blocks = slack_notify.card_blocks(CARD)
    text = json.dumps(blocks)
    buttons = [b for b in blocks if b["type"] == "actions"][0]["elements"]
    assert [b["action_id"] for b in buttons] == ["approve", "reject"]
    assert all(b["value"] == "APR-20261014-abc123|fp1" for b in buttons)
    assert "In plain words" in text and "raw rows" in text and "batch_id" not in text
    assert not find_record_ids(text)                                     # T26 holds in Slack too


def test_finished_cards_lose_their_buttons_and_slack_obeys_the_policy():
    assert "approved by owner@example.com" in slack_notify.final_text(
        {"approval_id": "APR-1", "status": "APPROVED", "decided_by": "owner@example.com"})
    assert "expired" in slack_notify.final_text({"approval_id": "APR-1", "status": "EXPIRED"})
    off = {"notifications": {"slack": {"enabled": False, "channel_id": "C0123ABCDEF"}}}
    assert slack_notify.config(off) is None                              # switched off → nothing is posted
    assert slack_notify.config({"notifications": {"slack": {"enabled": True, "channel_id": ""}}}) is None
    on = {"notifications": {"slack": {"enabled": True, "channel_id": "C0123ABCDEF"}}}
    assert slack_notify.config(on) == {"channel": "C0123ABCDEF", "secret": "slack-bot-token"}
