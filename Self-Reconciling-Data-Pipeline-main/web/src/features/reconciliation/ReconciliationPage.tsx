import { useReconciliation, type ReconCheck, type ReconDataset } from "../../api/recovery";
import { useBusinessDate } from "../../app/BusinessDate";
import { ErrorBox, Loading, PageHead, Panel, StatusPill, fmt, type Tone } from "../../components/ui";

const RESULT: Record<ReconDataset["result"], [Tone, string]> = {
  pass: ["ok", "Pass"], warn: ["held", "Warning"], fail: ["crit", "Fail"], not_checked: ["pend", "Not checked"],
};
const checkTone = (s: string): Tone => (s === "pass" || s === "success" ? "ok" : s === "warn" ? "held" : s === "skipped" ? "pend" : "crit");

function Diff({ d }: { d: ReconDataset }) {
  if (d.receipt_rows == null || d.rows_loaded == null) return <span className="ink2">—</span>;
  const diff = d.rows_loaded - d.receipt_rows;
  return <span className={diff ? "bad" : undefined}>{diff === 0 ? "0" : `${diff > 0 ? "+" : ""}${fmt.n(diff)}`}</span>;
}

function Checks({ checks }: { checks: ReconCheck[] }) {
  if (!checks.length) return <span className="ink2">—</span>;
  const bad = checks.filter((c) => c.status !== "pass");
  return (
    <div>
      {checks.length - bad.length} of {checks.length} passed
      {bad.map((c) => (
        <div key={c.name} className={`note${c.status === "warn" ? "" : " bad"}`}>
          {c.name} · {c.status.toUpperCase()}{c.failures ? ` (${c.failures})` : ""}
        </div>
      ))}
    </div>
  );
}

export default function ReconciliationPage() {
  const { date } = useBusinessDate();
  const q = useReconciliation(date);
  const r = q.data;
  const tile = (label: string, n: number | undefined, tone: Tone) => (
    <div className="card">
      <div className="desc">{label}</div>
      <div style={{ fontSize: 28, fontWeight: 600, margin: "6px 0" }}>{n ?? 0}</div>
      <StatusPill tone={tone}>{label}</StatusPill>
    </div>
  );

  return (
    <main className="content">
      <PageHead
        title="Reconciliation"
        lede="Every day is checked against the source's receipt — rows, unique IDs and ₹ totals — plus unit, range and duplicate checks."
        phase="Phase 2 · Models and checks"
      />
      {q.error && <ErrorBox error={q.error} />}
      {q.isLoading && <Loading what="reconciliation" />}
      {r && (
        <>
          <div className="grid3">
            {tile("Fail", r.summary.fail, "crit")}
            {tile("Warning", r.summary.warn, "held")}
            {tile("Pass", r.summary.pass, "ok")}
          </div>

          <Panel
            title="Datasets against their receipt"
            sub={r.gate_run_id
              ? <>{fmt.day(r.business_date)} · checked by the dbt run for {fmt.day(r.checked_business_date)} at {fmt.dateTime(r.checked_at)}</>
              : <>{fmt.day(r.business_date)} · no gate run covers this day yet</>}
            foot="Receipt = control totals written by the source in the same snapshot as the rows. Value-level checks (KMS) keep their evidence in evidence_restricted and are not shown here."
          >
            {r.datasets.length === 0 ? <div className="empty">No batches for this date.</div> : (
              <table>
                <thead>
                  <tr>
                    <th>Dataset</th><th>Source</th><th>Batch</th><th className="n">Rows landed</th>
                    <th className="n">Receipt rows</th><th className="n">Difference</th><th className="n">Unique IDs (receipt)</th>
                    <th className="n">Amount (receipt)</th><th>Checks</th><th>Result</th>
                  </tr>
                </thead>
                <tbody>
                  {r.datasets.map((d) => (
                    <tr key={d.dataset}>
                      <td>{d.dataset}{d.injections.length > 0 && <div className="note">injected: {d.injections.join(", ")}</div>}</td>
                      <td className="ink2">{d.source_system}</td>
                      <td className="mono">{d.batch_id}</td>
                      <td className="n">{fmt.n(d.rows_loaded)}</td>
                      <td className="n">{d.receipt_rows == null ? <StatusPill tone="crit">Missing</StatusPill> : fmt.n(d.receipt_rows)}</td>
                      <td className="n"><Diff d={d} /></td>
                      <td className="n">{fmt.n(d.receipt_keys)}</td>
                      <td className="n">{d.receipt_amount == null ? "—" : fmt.inr(d.receipt_amount)}</td>
                      <td><Checks checks={d.checks} /></td>
                      <td><StatusPill tone={RESULT[d.result][0]}>{RESULT[d.result][1]}</StatusPill></td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </Panel>

          {r.other_checks.length > 0 && (
            <Panel title="Report and run checks" sub="tests on the report tables and the expected-test-count assertion">
              <table>
                <thead><tr><th>Check</th><th>Type</th><th className="n">Failures</th><th>Policy</th><th>Status</th></tr></thead>
                <tbody>
                  {r.other_checks.map((c) => (
                    <tr key={c.name}>
                      <td className="mono">{c.name}</td><td className="ink2">{c.resource_type}</td>
                      <td className="n">{fmt.n(c.failures)}</td><td className="ink2">{c.policy_outcome ?? "—"}</td>
                      <td><StatusPill tone={checkTone(c.status)}>{c.status}</StatusPill></td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </Panel>
          )}
        </>
      )}
    </main>
  );
}
