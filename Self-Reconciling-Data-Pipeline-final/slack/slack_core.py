"""Phase 5 stage 4 — the Slack rules, as pure functions (no network), shared by the callback service and CI.

A Slack click is only ever turned into the SAME decision row the console writes (recovery.approval_decisions).
Nothing runs from Slack: the approval_worker re-checks the card (fingerprint, policy, kill switch, attempts,
evidence) before anything happens. The callback refuses, in this order:
  1. a request whose Slack signature is wrong                      (someone pretending to be Slack)
  2. a request older than 5 minutes                                (a replayed request)
  3. a click from another channel                                  (cards live in one channel only)
  4. a person who is not on the approver list                      (A10: named data owners only)
  5. a card that is no longer PENDING, has expired, already has a decision, or changed since it was posted
  6. a rejection without a reason
"""
from __future__ import annotations

import hashlib
import hmac
import time
from datetime import datetime

MAX_AGE_S = 300


def verify_signature(secret: str, timestamp: str, body: bytes, signature: str, now: float | None = None) -> str | None:
    """None when the request really comes from Slack and is fresh; otherwise why not (Slack's v0 scheme)."""
    if not secret or not timestamp or not signature:
        return "missing signature"
    try:
        ts = int(timestamp)
    except ValueError:
        return "bad timestamp"
    if abs((now if now is not None else time.time()) - ts) > MAX_AGE_S:
        return "stale request (older than 5 minutes) — possible replay"
    base = b"v0:" + timestamp.encode() + b":" + body
    expected = "v0=" + hmac.new(secret.encode(), base, hashlib.sha256).hexdigest()
    return None if hmac.compare_digest(expected, signature) else "signature mismatch"


def approvers(raw: str) -> set[str]:
    return {e.strip().lower() for e in (raw or "").split(",") if e.strip()}


def parse_value(value: str) -> tuple[str, str] | None:
    """Button value = '<approval_id>|<fingerprint>' (both written by the engine)."""
    parts = (value or "").split("|")
    if len(parts) != 2 or not parts[0].startswith("APR-") or not parts[1]:
        return None
    return parts[0], parts[1]


def may_decide(card: dict | None, decision: dict | None, email: str, fingerprint: str, now: datetime,
               allowed: set[str]) -> str | None:
    """None when this person may decide this card now; otherwise the reason (same rules as the console)."""
    e = (email or "").lower()
    if not e or e not in allowed:
        return "you are not on the approver list for these cards"
    if e.endswith(".gserviceaccount.com"):
        return "service accounts and agents cannot approve"
    if card is None:
        return "unknown card"
    if e == (card.get("requested_by") or "").lower():
        return "the requester cannot approve its own card"
    if card.get("status") != "PENDING":
        return f"card is {card.get('status')}"
    if decision is not None:
        return f"already decided by {decision.get('decided_by')}"
    if now >= card["expires_at"]:
        return "card has expired"
    if fingerprint != card.get("fingerprint"):
        return "the card changed since it was posted — open the new card"
    return None


def decision_row(approval_id: str, decision: str, fingerprint: str, email: str, reason: str | None,
                 now: datetime, decision_id: str) -> dict:
    return {"decision_id": decision_id, "approval_id": approval_id, "decision": decision, "fingerprint": fingerprint,
            "reason": (reason or "").strip() or None, "decided_by": email.lower(), "roles": "data_owner,slack",
            "decided_at": now.isoformat()}


def outcome_text(decision: str, email: str, approval_id: str, reason: str | None = None) -> str:
    if decision == "APPROVE":
        return (f":white_check_mark: *Approved* by {email} — {approval_id}. The approval worker re-checks the facts "
                "and then re-extracts and rebuilds (within 5 minutes). Nothing runs if anything changed.")
    return (f":x: *Rejected* by {email} — {approval_id}. Reason: {reason or '-'}. "
            "The incident is escalated; publication stays held.")
