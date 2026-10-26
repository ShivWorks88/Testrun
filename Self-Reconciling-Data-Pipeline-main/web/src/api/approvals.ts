// Phase 5 stage 1: approval cards (engine-written) and data-owner decisions.
// Shapes mirror api/app/schemas/approvals.py.
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { ApiError } from "./client";

export type ApprovalCard = {
  title: string; what_happened: string; evidence: Record<string, unknown>; rule: string; impact: string;
  if_approved: string; if_rejected: string; guardrails: string[];
};
export type Decision = { decision: "APPROVE" | "REJECT"; decided_by: string; decided_at: string; reason: string | null };
export type Approval = {
  approval_id: string; incident_id: string; business_date: string; dataset: string | null; action: string;
  params: Record<string, unknown>; status: string; shown_status: string; fingerprint: string;
  policy_version: string | null; rule_id: string | null; severity: string | null; route: string | null;
  attempt: number | null; card: ApprovalCard; requested_by: string | null; requested_at: string; expires_at: string;
  note: string | null; decided_by: string | null; decision: Decision | null; can_decide: boolean; why_not: string | null;
};
export type DecisionAccepted = {
  approval_id: string; decision: "APPROVE" | "REJECT"; decided_by: string; decided_at: string; next: string;
};

async function call<T>(path: string, init?: RequestInit): Promise<T> {
  const r = await fetch(`/api${path}`, { ...init, headers: { "Content-Type": "application/json" } });
  if (!r.ok) {
    let detail = r.statusText;
    try { detail = (await r.json()).detail ?? detail; } catch { /* not JSON */ }
    throw new ApiError(r.status, typeof detail === "string" ? detail : JSON.stringify(detail));
  }
  return r.json() as Promise<T>;
}

export const useApprovals = () =>
  useQuery({ queryKey: ["approvals"], queryFn: () => call<Approval[]>("/approvals"), refetchInterval: 15_000 });

export const useDecide = () => {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (v: { approval_id: string; decision: "APPROVE" | "REJECT"; fingerprint: string; reason: string }) =>
      call<DecisionAccepted>(`/approvals/${encodeURIComponent(v.approval_id)}/decision`, {
        method: "POST",
        body: JSON.stringify({ decision: v.decision, fingerprint: v.fingerprint, reason: v.reason }),
      }),
    onSettled: () => {
      qc.invalidateQueries({ queryKey: ["approvals"] });
      qc.invalidateQueries({ queryKey: ["incidents"] });
    },
  });
};
