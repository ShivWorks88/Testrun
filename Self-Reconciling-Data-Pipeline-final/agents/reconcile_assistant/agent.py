"""Phase 6 — the "Ask the pipeline" assistant as an ADK agent, served from the SAME private Cloud Run service as
the Diagnosis agent (app name `reconcile_assistant`).

  * answers questions about the pipeline from a CONTEXT the console API sends with each question: report states,
    incidents, approval cards, outage breakers and policy — counts and the engine's own words only (T26)
  * NO tools: it cannot query, approve, re-run or change anything (read-only by construction)
  * output_schema: {answer, cited}; the API rejects an answer that names an ID not in the context
  * Gemini in asia-south1 only (India residency); Model Armor screens question and answer in the API
"""
from __future__ import annotations

import os
from pathlib import Path

from google.adk.agents import LlmAgent
from google.genai import types
from pydantic import BaseModel

os.environ.setdefault("GOOGLE_GENAI_USE_VERTEXAI", "TRUE")
os.environ.setdefault("GOOGLE_CLOUD_LOCATION", "asia-south1")
if os.environ["GOOGLE_CLOUD_LOCATION"] != "asia-south1":
    raise RuntimeError("the assistant runs in asia-south1 only (India residency)")

MODEL = os.environ.get("ASSISTANT_MODEL", "gemini-3.5-flash")      # keep equal to agents/registry.yml
PROMPT_VERSION = "assist-v1"
INSTRUCTION = (Path(__file__).parent / "prompts" / f"{PROMPT_VERSION}.txt").read_text(encoding="utf-8")


class Answer(BaseModel):
    answer: str
    cited: list[str]


root_agent = LlmAgent(
    name="assistant",
    model=MODEL,
    description="Answers questions about the reconcile pipeline from the records it is given; never acts.",
    instruction=INSTRUCTION,
    output_schema=Answer,              # with an output schema the agent cannot use tools
    disallow_transfer_to_parent=True,
    disallow_transfer_to_peers=True,
    generate_content_config=types.GenerateContentConfig(
        temperature=0, thinking_config=types.ThinkingConfig(thinking_budget=512)),
)
