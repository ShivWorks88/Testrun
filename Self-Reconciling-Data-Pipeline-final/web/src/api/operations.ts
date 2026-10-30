// Phase 6 Operations screen + read-only assistant. Shapes mirror api/app/schemas/operations.py.
import { useMutation, useQuery } from "@tanstack/react-query";
import { ApiError } from "./client";
import type { IncidentSummary } from "./recovery";

export type Kind = "auto" | "hold" | "quiet" | "approve";
export type Breaker = {
  source_system: string; opened_at: string | null; reason: string | null;
  failing_datasets: string[]; parent_incident_id: string | null;
};
export type PolicyBrief = { version: string; kill_switch: boolean; agents_enabled: boolean; attempt_cap: number | null };
export type Shell = { open_breakers: Breaker[]; pending_cards: number; next_expiry: string | null; policy: PolicyBrief };
export type ReportState = {
  dataset: string; business_date: string | null; state: string; reasons: string | null; decided_at: string | null;
};
export type OpsIncident = IncidentSummary & { kind: Kind };
export type AgentCall = {
  call_id: string | null; incident_id: string | null; agent_id: string | null; provider: string | null;
  model: string | null; prompt_version: string | null; validation: string | null; reason: string | null;
  proposed_action: string | null; outcome: string | null; latency_ms: number | null; at: string;
};
export type Operations = {
  generated_at: string; window_days: number; latest_business_date: string | null;
  today: ReportState[]; board_dates: string[]; board: { dataset: string; cells: ReportState[] }[];
  freshness: { last_batch_at: string | null; last_gate_at: string | null; last_engine_at: string | null };
  kpis: {
    incidents: number; open_incidents: number; resolved: number; median_minutes_to_resolve: number | null;
    without_a_person: number; held_report_days: number;
  };
  mix: { kind: Kind; count: number; failure_types: string[] }[];
  per_day: { day: string; counts: Partial<Record<Kind, number>> }[];
  resolve_times: { failure_type: string; kind: Kind; median_minutes: number; incidents: number }[];
  incidents: OpsIncident[];
  agent_calls: AgentCall[];
  shell: Shell;
  assistant_available: boolean;
};
export type AskOut = {
  status: "answered" | "refused" | "unavailable"; answer: string; cited: string[];
  reason: string | null; call_id: string | null;
};

async function call<T>(path: string, init?: RequestInit): Promise<T> {
  const r = await fetch(`/api${path}`, { ...init, headers: { "Content-Type": "application/json", ...(init?.headers ?? {}) } });
  if (!r.ok) {
    let detail = r.statusText;
    try { detail = (await r.json()).detail ?? detail; } catch { /* not JSON */ }
    throw new ApiError(r.status, typeof detail === "string" ? detail : JSON.stringify(detail));
  }
  return r.json() as Promise<T>;
}

export const useShell = () =>
  useQuery({ queryKey: ["ops-shell"], queryFn: () => call<Shell>("/operations/shell"), refetchInterval: 30_000 });

export const useOperations = (days = 7) =>
  useQuery({ queryKey: ["operations", days], queryFn: () => call<Operations>(`/operations?days=${days}`), refetchInterval: 30_000 });

export const useAsk = () =>
  useMutation({
    mutationFn: (question: string) => call<AskOut>("/assistant", { method: "POST", body: JSON.stringify({ question }) }),
  });

// Plain words next to the technical names (same words as the Pipeline Story demo).
export const KIND: Record<Kind, { label: string; color: string; what: string }> = {
  auto: { label: "Fixes itself", color: "var(--s1)", what: "the engine fixed it, no person needed" },
  hold: { label: "Holds and escalates", color: "var(--s2)", what: "reports held; an engineer gets the evidence" },
  quiet: { label: "Expected or waits", color: "var(--s3)", what: "recorded as normal or re-checked later; no alarm" },
  approve: { label: "A person approves", color: "var(--s4)", what: "an approval card waits for the data owner" },
};
export const KINDS: Kind[] = ["auto", "hold", "quiet", "approve"];

export const REPORT: Record<string, string> = {
  dim_encounters: "Encounters", fct_bed_occupancy: "Bed occupancy", fct_daily_claims: "Daily claims",
  fct_lab_results_daily: "Lab results",
};

export const PLAIN: Record<string, string> = {
  partial_extract: "fewer rows than the source receipt",
  missing_load: "nothing landed for the day",
  offsetting_errors: "lost and doubled rows that cancel out in the count",
  amount_mismatch: "the amount does not match the receipt",
  raw_duplicates: "a retried batch loaded rows twice",
  missing_receipt: "no source receipt, so completeness can't be proven",
  checks_incomplete: "some checks did not run",
  unit_or_range: "values in the wrong unit or range",
  staging_mismatch: "a report does not match staging",
  build_error: "the dbt build failed",
  check_failed: "a data test failed",
  extract_failed: "a dataset failed to load",
  stale_feed: "the source replica is behind",
  late_data: "the source has only part of the day yet",
  value_mismatch: "critical lab values differ from raw",
  archived_history: "the hospital archived old records (expected)",
  count_excess: "raw has rows the source no longer has",
  late_change: "the source changed a day already loaded",
  late_change_old: "the source changed a day older than the look-back",
  young_orphan: "a late result whose encounter comes tomorrow",
  archived_parent: "a result whose encounter was archived (expected)",
  true_orphan: "a result whose encounter exists nowhere",
  merged_patient: "rows still use a merged patient's old ID",
  source_outage: "a whole source system is down",
};
export const plain = (t: string) => PLAIN[t] ?? "";
