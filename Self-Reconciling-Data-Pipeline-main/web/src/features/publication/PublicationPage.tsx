import { useState } from "react";
import { usePublication, type PublicationCell } from "../../api/recovery";
import { ErrorBox, Loading, PageHead, Panel, StatusPill, fmt, type Tone } from "../../components/ui";

export const stateTone = (s: string): Tone =>
  s === "PUBLISHED" ? "ok" : s === "HELD" ? "held" : s === "ROLLED_BACK" ? "info" : s === "PENDING" ? "pend" : "cav";
const label = (s: string) => s.replace("_", " ").toLowerCase().replace(/^./, (c) => c.toUpperCase());

export default function PublicationPage() {
  const q = usePublication();
  const p = q.data;
  const [sel, setSel] = useState<PublicationCell | null>(null);
  const cell = (ds: string, d: string) => p?.cells.find((c) => c.dataset === ds && c.business_date === d);
  const latest = p?.dates[p.dates.length - 1];
  const delayed = p && latest ? p.datasets.filter((ds) => cell(ds, latest)?.state !== "PUBLISHED") : [];

  return (
    <main className="content">
      <PageHead
        title="Publication"
        lede="Users only see verified days. Each report and day is published, or held with its reason; reports keep the last verified figures."
        phase="Phase 3 · Publication gate"
      />
      {q.error && <ErrorBox error={q.error} />}
      {q.isLoading && <Loading what="the publication ledger" />}

      {p && delayed.map((ds) => (
        <div key={ds} className="banner held" role="status">
          <b>Data delayed: {ds}.</b>
          <span>Last verified {fmt.day(p.last_verified[ds])}. Users keep that version until the day passes its checks.</span>
        </div>
      ))}

      {p && (
        <Panel title="Verification ledger" sub="reports × business days · click a cell for the gate's reasons"
          foot="Only sa-publish can write analytics. A backup copy is taken before every publish, so any day can be rolled back.">
          {p.datasets.length === 0 ? <div className="empty">The gate has not decided anything yet. Run the dbt_build DAG.</div> : (
            <table>
              <thead>
                <tr><th>Report</th>{p.dates.map((d) => <th key={d}>{fmt.day(d)}</th>)}<th>Last verified</th></tr>
              </thead>
              <tbody>
                {p.datasets.map((ds) => (
                  <tr key={ds}>
                    <td className="mono">{ds}</td>
                    {p.dates.map((d) => {
                      const c = cell(ds, d);
                      return (
                        <td key={d} style={{ cursor: c ? "pointer" : undefined }} onClick={() => c && setSel(c)}
                          title={c?.reasons ?? ""}>
                          {c ? <StatusPill tone={stateTone(c.state)}>{label(c.state)}</StatusPill> : <span className="ink2">—</span>}
                        </td>
                      );
                    })}
                    <td>{fmt.day(p.last_verified[ds])}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </Panel>
      )}

      {sel && (
        <Panel title={`${sel.dataset} · ${fmt.day(sel.business_date)}`} sub="gate decision"
          right={<button className="btn small" onClick={() => setSel(null)}>Close</button>}>
          <dl className="kv">
            <dt>State</dt><dd><StatusPill tone={stateTone(sel.state)}>{label(sel.state)}</StatusPill></dd>
            <dt>Decided</dt><dd>{fmt.dateTime(sel.decided_at)}</dd>
            <dt>Backup</dt><dd className="mono">{sel.snapshot ?? "—"}</dd>
            <dt>Reasons</dt>
            <dd>{(sel.reasons || "—").split("; ").map((x) => <div key={x} className={sel.state === "HELD" ? "note bad" : "note"}>{x}</div>)}</dd>
          </dl>
        </Panel>
      )}

      {p && p.log.length > 0 && (
        <Panel title="Publish log" sub="latest decisions first · interim PENDING rows hidden">
          <table>
            <thead><tr><th>When</th><th>Report</th><th>Business day</th><th>State</th><th>Reason</th><th>By</th></tr></thead>
            <tbody>
              {p.log.filter((e) => e.state !== "PENDING").map((e, i) => (
                <tr key={i}>
                  <td>{fmt.dateTime(e.decided_at)}</td><td className="mono">{e.dataset}</td><td>{fmt.day(e.business_date)}</td>
                  <td><StatusPill tone={stateTone(e.state)}>{label(e.state)}</StatusPill></td>
                  <td className="ink2">{e.reasons ?? ""}</td><td className="ink2">{(e.actor ?? "").split("@")[0]}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </Panel>
      )}
    </main>
  );
}
