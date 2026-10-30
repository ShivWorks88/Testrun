import { render, screen, within } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { MemoryRouter } from "react-router-dom";
import { BusinessDateProvider } from "../../app/BusinessDate";
import SourcesPage from "./SourcesPage";

const data: Record<string, unknown> = {
  "/api/me": { email: "eng@x.com", roles: ["engineer", "viewer"] },
  "/api/sources": [
    { system: "EHR", label: "EHR", description: "Encounters, patients, diagnoses", access: "Read replica", reachable: true,
      last_probe_at: "2026-10-13T19:40:00Z", latency_ms: 12, replica_lag_seconds: 12, archive_cutoff_date: "2026-10-06",
      error: null, datasets: 3, datasets_loaded: 3, last_batch_at: "2026-10-13T19:44:00Z", last_business_date: "2026-10-13" },
  ],
  "/api/batches": {
    business_date: "2026-10-13", available_dates: ["2026-10-13"],
    batches: [
      { batch_id: "b-20261013-encounters-01", dataset: "encounters", source_system: "EHR", business_date: "2026-10-13",
        mode: "extract", status: "LOADED", rows_extracted: 10000, rows_loaded: 9200, control_total_rows: 10000,
        distinct_keys: 10000, amount_total: null, totals_rows: 1, archive_uri: "gs://b/raw/encounters/load_date=2026-10-13/b.parquet",
        injections: ["drop_hour"], finished_at: "2026-10-13T19:39:00Z", error: null },
      { batch_id: "b-20261013-lab_orders-01", dataset: "lab_orders", source_system: "LIS", business_date: "2026-10-13",
        mode: "extract", status: "LOADED", rows_extracted: 7904, rows_loaded: 7904, control_total_rows: null,
        distinct_keys: null, amount_total: null, totals_rows: 0, archive_uri: "gs://b/x.parquet",
        injections: ["missing_control_totals"], finished_at: "2026-10-13T19:46:00Z", error: null },
    ],
  },
};

beforeEach(() => {
  vi.stubGlobal("fetch", vi.fn(async (url: string) => {
    const key = url.split("?")[0];
    return new Response(JSON.stringify(data[key]), { status: 200, headers: { "Content-Type": "application/json" } });
  }));
});

test("shows source health, flags a short load and a missing control-totals row", async () => {
  render(
    <QueryClientProvider client={new QueryClient()}>
      <BusinessDateProvider><MemoryRouter><SourcesPage /></MemoryRouter></BusinessDateProvider>
    </QueryClientProvider>,
  );
  expect(await screen.findByText("Connected")).toBeTruthy();
  const enc = (await screen.findByText("b-20261013-encounters-01")).closest("tr")!;
  expect(within(enc).getByText("9,200").className).toContain("bad");      // landed < control total
  const lab = screen.getByText("b-20261013-lab_orders-01").closest("tr")!;
  expect(within(lab).getByText("Missing")).toBeTruthy();
  expect(within(lab).getByRole("button", { name: "Replay" })).toHaveProperty("disabled", false);
});
