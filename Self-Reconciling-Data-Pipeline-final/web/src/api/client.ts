// Typed API client. Types come from the FastAPI OpenAPI contract (npm run gen:api).
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import type { components } from "./schema";

export type Environment = components["schemas"]["Environment"];
export type Resource = components["schemas"]["Resource"];
export type SourceHealth = components["schemas"]["SourceHealth"];
export type Batch = components["schemas"]["Batch"];
export type Batches = components["schemas"]["Batches"];
export type ReplayAccepted = components["schemas"]["ReplayAccepted"];
export type Me = { email: string; roles: string[] };

export class ApiError extends Error {
  constructor(public status: number, message: string) {
    super(message);
  }
}

async function call<T>(path: string, init?: RequestInit): Promise<T> {
  const r = await fetch(`/api${path}`, {
    ...init,
    headers: { "Content-Type": "application/json", ...(init?.headers ?? {}) },
  });
  if (!r.ok) {
    let detail = r.statusText;
    try {
      detail = (await r.json()).detail ?? detail;
    } catch {
      /* not JSON */
    }
    throw new ApiError(r.status, typeof detail === "string" ? detail : JSON.stringify(detail));
  }
  return r.json() as Promise<T>;
}

export const useMe = () => useQuery({ queryKey: ["me"], queryFn: () => call<Me>("/me"), staleTime: 300_000 });

export const useEnvironment = () =>
  useQuery({ queryKey: ["environment"], queryFn: () => call<Environment>("/environment"), staleTime: 60_000 });

export const useSources = () =>
  useQuery({ queryKey: ["sources"], queryFn: () => call<SourceHealth[]>("/sources"), refetchInterval: 60_000 });

export const useBatches = (date: string | null) =>
  useQuery({
    queryKey: ["batches", date],
    queryFn: () => call<Batches>(`/batches${date ? `?date=${date}` : ""}`),
    refetchInterval: 30_000,
  });

export const useReplay = () => {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (v: { dataset: string; business_date: string; batch_id?: string | null; reason?: string }) =>
      call<ReplayAccepted>(`/batches/${v.dataset}/replay`, {
        method: "POST",
        body: JSON.stringify({ business_date: v.business_date, batch_id: v.batch_id ?? null, reason: v.reason ?? "" }),
      }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["batches"] }),
  });
};
