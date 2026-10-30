"""Phase 6 — "Ask the pipeline": a read-only assistant on the Operations screen.

How it stays safe (same rules as the Diagnosis and Communication agents):
  * READ-ONLY by construction. The API gathers a small context itself (report states, incidents, cards,
    breakers, policy — counts and the engine's own words only, no patient data) and sends it with the question.
    The agent has NO tools: it cannot query, approve, re-run or change anything.
  * GROUNDED. It must answer from that context and list the incident / card IDs it used. An answer that names
    an ID that is not in the context is rejected.
  * SCREENED. Model Armor (policy agents.model_armor, asia-south1) screens the question and the answer; when the
    policy says `required`, a screening error fails closed.
  * RECORDED. Every question gets one row in recovery.agent_calls (agent_id "assistant"), answered or not —
    the question's length is stored, not its text.
  * Runs as the `reconcile_assistant` app of the private ADK Cloud Run service (asia-south1), called with an
    ID token as sa-api. Switched on by `enabled: true` on the assistant entry in agents/registry.yml.
"""
from __future__ import annotations

import json
import re
import time
import uuid
from datetime import datetime, timezone

import yaml

from ..core.config import Settings

AGENT_ID = "assistant"
ADK_APP = "reconcile_assistant"
ID_PATTERN = re.compile(r"\b(?:INC|APR|REL)-\d{8}-[0-9a-f]{6}\b")
MAX_ANSWER = 1500
SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]


# ── configuration ───────────────────────────────────────────────────────────
def registry_entry(settings: Settings) -> dict | None:
    try:
        with open(settings.registry_file) as f:
            reg = yaml.safe_load(f) or {}
    except OSError:
        return None
    return next((a for a in reg.get("agents") or [] if a.get("id") == AGENT_ID), None)


def available(settings: Settings) -> tuple[bool, str]:
    e = registry_entry(settings)
    if not e:
        return False, "no assistant entry in agents/registry.yml"
    if not e.get("enabled"):
        return False, "the assistant is switched off (agents/registry.yml enabled: false)"
    if not str(e.get("service_url") or "").startswith("https://"):
        return False, "no service_url for the assistant in agents/registry.yml"
    if e.get("region", "asia-south1") != "asia-south1":
        return False, "the assistant runs in asia-south1 only"
    return True, "ok"


def armor_config(settings: Settings) -> dict | None:
    try:
        with open(settings.policy_file) as f:
            c = ((yaml.safe_load(f) or {}).get("agents") or {}).get("model_armor") or {}
    except OSError:
        return None
    if not c.get("template"):
        return None
    return {"template": c["template"], "location": c.get("location", "asia-south1"),
            "required": c.get("required", True) is not False}


# ── context: what the agent may know (counts and engine notes only) ─────────
def build_context(ops: dict) -> dict:
    """`ops` is the Operations payload as a dict. Keeps the newest 40 incidents and 15 cards."""
    return {
        "now_utc": datetime.now(timezone.utc).isoformat(timespec="minutes"),
        "policy": ops["shell"]["policy"],
        "outage_breakers_open": ops["shell"]["open_breakers"],
        "cards_waiting_for_a_person": ops["shell"]["pending_cards"],
        "latest_business_date": ops.get("latest_business_date"),
        "reports_latest": ops.get("today", []),
        "kpis_last_days": {"window_days": ops.get("window_days"), **ops.get("kpis", {})},
        "incidents_newest_first": [
            {k: i.get(k) for k in ("incident_id", "business_date", "dataset", "failure_type", "status", "kind",
                                   "attempt", "minutes_open", "rule_id", "severity", "note", "opened_at")}
            for i in ops.get("incidents", [])[:40]],
        "approval_cards_newest_first": ops.get("cards", [])[:15],
        "kinds": {"auto": "fixed automatically", "hold": "held and escalated to a person",
                  "quiet": "expected or waiting, no alarm", "approve": "needs a data owner's approval"},
    }


def known_ids(ctx: dict) -> set[str]:
    ids = {i["incident_id"] for i in ctx.get("incidents_newest_first", []) if i.get("incident_id")}
    ids |= {c["approval_id"] for c in ctx.get("approval_cards_newest_first", []) if c.get("approval_id")}
    ids |= {c.get("incident_id") for c in ctx.get("approval_cards_newest_first", []) if c.get("incident_id")}
    ids |= {b.get("parent_incident_id") for b in ctx.get("outage_breakers_open", []) if b.get("parent_incident_id")}
    return {i for i in ids if i}


# ── validation (pure; unit-tested) ──────────────────────────────────────────
def validate(raw: str | None, ids: set[str]) -> tuple[str | None, list[str], str | None]:
    """Returns (answer, cited, reason_rejected)."""
    if not raw:
        return None, [], "no_answer"
    text = raw.strip()
    if text.startswith("```"):
        text = text.strip("`").removeprefix("json").strip()
    try:
        obj = json.loads(text)
    except ValueError:
        return None, [], "not_json"
    if not isinstance(obj, dict) or not isinstance(obj.get("answer"), str) or not obj["answer"].strip():
        return None, [], "bad_shape"
    answer = obj["answer"].strip()
    cited = [c for c in obj.get("cited") or [] if isinstance(c, str)]
    if len(answer) > MAX_ANSWER:
        return None, [], "too_long"
    unknown = sorted((set(ID_PATTERN.findall(answer)) | set(cited)) - ids)
    if unknown:
        return None, [], "unknown_reference"   # names something that is not in the data it was given
    return answer, sorted(set(cited) | set(ID_PATTERN.findall(answer))), None


# ── calls ───────────────────────────────────────────────────────────────────
def _credentials():
    from ..core import gcp
    return gcp.credentials()


def _id_token_session(audience: str):
    from google.auth import impersonated_credentials
    from google.auth.transport.requests import AuthorizedSession, Request
    from google.oauth2 import id_token

    from ..core.config import get_settings
    s = get_settings()
    if s.impersonate_service_account:   # local dev: act as sa-api
        return AuthorizedSession(impersonated_credentials.IDTokenCredentials(
            _credentials(), target_audience=audience, include_email=True))
    import requests  # Cloud Run: the metadata server signs an ID token for sa-api
    sess = requests.Session()
    sess.headers["Authorization"] = f"Bearer {id_token.fetch_id_token(Request(), audience)}"
    return sess


def screen(settings: Settings, armor: dict, kind: str, text: str) -> tuple[str, list[str]]:
    """('clean' | 'blocked' | 'error', [filter names]) — mirrors ingest/reconcile_ingest/model_armor.py."""
    from google.auth.transport.requests import AuthorizedSession
    loc = armor["location"]
    base = f"https://modelarmor.{loc}.rep.googleapis.com/v1/projects/{settings.gcp_project}/locations/{loc}"
    method, body = (("sanitizeUserPrompt", {"userPromptData": {"text": text[:20000]}}) if kind == "prompt"
                    else ("sanitizeModelResponse", {"modelResponseData": {"text": text[:20000]}}))
    try:
        r = AuthorizedSession(_credentials()).post(f"{base}/templates/{armor['template']}:{method}", json=body,
                                                   timeout=8)
        if r.status_code != 200:
            return "error", [f"http_{r.status_code}"]
        sr = r.json().get("sanitizationResult") or {}
        if sr.get("filterMatchState") != "MATCH_FOUND":
            return "clean", []
        return "blocked", sorted((sr.get("filterResults") or {}).keys()) or ["match_found"]
    except Exception as exc:  # noqa: BLE001 — the caller fails closed when screening is required
        return "error", [type(exc).__name__]


def call_adk(entry: dict, payload: dict, timeout_s: float) -> str | None:
    url = str(entry["service_url"]).rstrip("/")
    http = _id_token_session(url)
    session_id = f"s-{uuid.uuid4().hex[:12]}"
    r = http.post(f"{url}/apps/{ADK_APP}/users/console/sessions/{session_id}", json={}, timeout=timeout_s)
    r.raise_for_status()
    r = http.post(f"{url}/run", timeout=timeout_s, json={
        "appName": ADK_APP, "userId": "console", "sessionId": session_id,
        "newMessage": {"role": "user", "parts": [{"text": json.dumps(payload, default=str, sort_keys=True)}]}})
    r.raise_for_status()
    events = r.json() if isinstance(r.json(), list) else []
    for ev in reversed(events):   # the final answer is the last model event with text
        parts = ((ev or {}).get("content") or {}).get("parts") or []
        text = "".join(p.get("text", "") for p in parts if isinstance(p, dict) and not p.get("thought"))
        if text:
            return text
    return None


def ask(settings: Settings, question: str, ops: dict, asked_by: str, policy_version: str | None,
        caller=call_adk, screener=screen) -> tuple[dict, dict]:
    """Returns (AskOut as dict, agent_calls row). Never raises for model or screening problems."""
    entry = registry_entry(settings) or {}
    ctx = build_context(ops)
    ids = known_ids(ctx)
    armor = armor_config(settings)
    t0 = time.monotonic()
    answer, cited, reason, raw = None, [], None, None
    try:
        if armor:
            verdict, hits = screener(settings, armor, "prompt", question)
            if verdict == "blocked" or (verdict == "error" and armor["required"]):
                reason = f"model_armor_{verdict}_prompt:{','.join(hits)}"
        if not reason:
            raw = caller(entry, {"question": question, "context": ctx}, float(entry.get("timeout_seconds", 25)))
            if armor and raw:
                verdict, hits = screener(settings, armor, "response", raw)
                if verdict == "blocked" or (verdict == "error" and armor["required"]):
                    reason, raw = f"model_armor_{verdict}_response:{','.join(hits)}", None
            if not reason:
                answer, cited, reason = validate(raw, ids)
    except Exception as exc:  # noqa: BLE001 — a platform error is an unanswered question, never a crash
        reason = f"platform_error:{type(exc).__name__}"
    latency = int((time.monotonic() - t0) * 1000)
    call_id = f"AC-{datetime.now(timezone.utc):%Y%m%d}-{uuid.uuid4().hex[:8]}"
    row = {
        "call_id": call_id, "incident_id": None, "incident_key": None, "business_date": None, "dataset": None,
        "agent_id": AGENT_ID, "provider": "adk", "model": entry.get("model"),
        "prompt_version": entry.get("prompt_version"), "fake_mode": None, "policy_version": policy_version,
        "gate_run_id": None,
        "request_refs": json.dumps({"question_chars": len(question), "asked_by": asked_by,
                                    "context": {"incidents": len(ctx["incidents_newest_first"]),
                                                "cards": len(ctx["approval_cards_newest_first"])}}),
        "output": answer, "validation": "accepted" if answer else "rejected", "reason": reason,
        "proposed_action": None, "confidence": None, "outcome": "answered" if answer else "refused",
        "latency_ms": latency, "actor": "sa-api", "at": datetime.now(timezone.utc).isoformat(),
    }
    if answer:
        out = {"status": "answered", "answer": answer, "cited": cited, "reason": None, "call_id": call_id}
    else:
        out = {"status": "refused", "call_id": call_id, "cited": [], "reason": reason,
               "answer": "I can't answer that safely from the pipeline's records. Try asking about a report, "
                         "an incident ID or an approval card — or open the incident for the full journal."}
    return out, row
