// Phase 2–4 screens: reconciliation, publication ledger, incidents (read-only).
// Shapes mirror api/app/schemas/recovery.py.
import { useQuery } from "@tanstack/react-query";
import { ApiError } from "./client";

export type ReconCheck = {
  name: string; resource_type: string; status: string; failures: number | null;
  policy_outcome: string | null; attached_to: string | null;
};
export type ReconDataset = {
  dataset: string; source_system: string; batch_id: string; status: string; mode: string;
  rows_loaded: number | null; receipt_rows: number | null; receipt_keys: number | null;
  receipt_amount: string | number | null; injections: string[]; finished_at: string | null;
  counts_match: boolean | null; result: "pass" | "warn" | "fail" | "not_checked"; checks: ReconCheck[];
};
export type Reconciliation = {
  business_date: string; available_dates: string[]; gate_run_id: string | null;
  checked_business_date: string | null; checked_at: string | null;
  datasets: ReconDataset[]; other_checks: ReconCheck[]; summary: Record<string, number>;
};
export type PublicationCell = {
  dataset: string; business_date: string; state: string; reasons: string | null;
  decided_at: string; snapshot: string | null;
};
export type PublicationEvent = PublicationCell & { gate_run_id: string | null; actor: string | null };
export type Publication = {
  dates: string[]; datasets: string[]; cells: PublicationCell[]; log: PublicationEvent[];
  last_verified: Record<string, string | null>;
};
export type Gate = { gate: number; question: string; answer: string; pass: boolean };
export type IncidentSummary = {
  incident_id: string; business_date: string; dataset: string | null; failure_type: string; status: string;
  attempt: number; action: string | null; mode: string | null; note: string | null;
  opened_at: string; updated_at: string; minutes_open: number; events: number;
  severity: string | null; rule_id: string | null; policy_version: string | null;
};
export type IncidentEvent = {
  at: string; status: string; attempt: number | null; action: string | null; mode: string | null;
  note: string | null; evidence: unknown; gates: Gate[] | null; gate_run_id: string | null;
  rule_id: string | null; severity: string | null; route: string | null; policy_version: string | null;
};
export type IncidentDetail = { summary: IncidentSummary; events: IncidentEvent[] };

async function get<T>(path: string): Promise<T> {
  const r = await fetch(`/api${path}`, { headers: { "Content-Type": "application/json" } });
  if (!r.ok) {
    let detail = r.statusText;
    try { detail = (await r.json()).detail ?? detail; } catch { /* not JSON */ }
    throw new ApiError(r.status, typeof detail === "string" ? detail : JSON.stringify(detail));
  }
  return r.json() as Promise<T>;
}

export const useReconciliation = (date: string | null) =>
  useQuery({
    queryKey: ["reconciliation", date],
    queryFn: () => get<Reconciliation>(`/reconciliation${date ? `?date=${date}` : ""}`),
    refetchInterval: 30_000,
  });

export const usePublication = () =>
  useQuery({ queryKey: ["publication"], queryFn: () => get<Publication>("/publication"), refetchInterval: 30_000 });

export const useIncidents = () =>
  useQuery({ queryKey: ["incidents"], queryFn: () => get<IncidentSummary[]>("/incidents"), refetchInterval: 20_000 });

export const useIncident = (id: string | undefined) =>
  useQuery({
    queryKey: ["incident", id],
    queryFn: () => get<IncidentDetail>(`/incidents/${encodeURIComponent(id ?? "")}`),
    enabled: !!id,
    refetchInterval: 20_000,
  });
