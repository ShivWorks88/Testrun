"""Phase 5 stage 3b — the Diagnosis agent as an ADK agent, served from a PRIVATE Cloud Run service.

ADK replaces only the model call. The engine still builds the masked payload, validates every answer
(same codes, same provenance in recovery.agent_calls) and decides what happens next.

  * same model, region and prompt as the REST path (agents/registry.yml: gemini-3.5-flash, asia-south1, diag-v2)
  * NO tools: the agent sees only the masked payload the engine sends (T26) and can call nothing
  * output_schema: the exact proposal shape agent_gateway.validate() checks
  * runs as sa-agents on Cloud Run; Vertex AI in asia-south1 only (India residency)
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Literal

from google.adk.agents import LlmAgent
from google.genai import types
from pydantic import BaseModel

os.environ.setdefault("GOOGLE_GENAI_USE_VERTEXAI", "TRUE")
os.environ.setdefault("GOOGLE_CLOUD_LOCATION", "asia-south1")
if os.environ["GOOGLE_CLOUD_LOCATION"] != "asia-south1":
    raise RuntimeError("the Diagnosis agent runs in asia-south1 only (India residency)")

MODEL = os.environ.get("DIAGNOSIS_MODEL", "gemini-3.5-flash")        # keep equal to agents/registry.yml
PROMPT_VERSION = "diag-v2"
INSTRUCTION = (Path(__file__).parent / "prompts" / f"{PROMPT_VERSION}.txt").read_text(encoding="utf-8")
# the engine filters again against the live catalog and never_auto; this list only narrows the model
Action = Literal["reextract_from_raw", "rebuild_partition", "link_unknown_member", "wait_and_recheck", "escalate"]


class Fact(BaseModel):
    statement: str
    evidence_refs: list[str]


class Hypothesis(BaseModel):
    statement: str
    confidence: float


class ProposedAction(BaseModel):
    action: Action
    dataset: str | None = None
    business_date: str | None = None


class Proposal(BaseModel):
    confirmed_facts: list[Fact]
    hypotheses: list[Hypothesis]
    proposed_action: ProposedAction
    confidence: float


root_agent = LlmAgent(
    name="diagnosis",
    model=MODEL,
    description="Advises on ONE unknown data-reconciliation incident; never acts.",
    instruction=INSTRUCTION,
    output_schema=Proposal,            # with an output schema the agent cannot use tools
    disallow_transfer_to_parent=True,
    disallow_transfer_to_peers=True,
    generate_content_config=types.GenerateContentConfig(
        temperature=0, thinking_config=types.ThinkingConfig(thinking_budget=512)),
)
