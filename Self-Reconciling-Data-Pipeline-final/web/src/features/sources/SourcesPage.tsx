import { useState } from "react";
import { useBatches, useMe, useReplay, useSources, type Batch, type SourceHealth } from "../../api/client";
import { useBusinessDate } from "../../app/BusinessDate";
import { ErrorBox, Loading, PageHead, Panel, StatusPill, fmt } from "../../components/ui";

function SourceCard({ s }: { s: SourceHealth }) {
  const tone = s.reachable == null ? "pend" : s.reachable ? "ok" : "crit";
  const label = s.reachable == null ? "No probe yet" : s.reachable ? "Connected" : "Unreachable";
  return (
    <div className="card">
      <div style={{ display: "flex", gap: 10, alignItems: "center" }}>
        <div>
          <h3>{s.label}</h3>
          <div className="desc">{s.description}</div>
        </div>
        <span className="sp" />
        <StatusPill tone={tone}>{label}</StatusPill>
      </div>
      <dl className="kv">
        <dt>Replica lag</dt><dd>{s.replica_lag_seconds == null ? "—" : `${Math.round(s.replica_lag_seconds)} s`}</dd>
        <dt>Last batch</dt><dd>{fmt.time(s.last_batch_at)}</dd>
        <dt>Datasets loaded</dt><dd>{s.datasets_loaded} of {s.datasets}</dd>
        <dt>{s.system === "EHR" ? "Archive cutoff" : "Last probe"}</dt>
        <dd>{s.system === "EHR" ? fmt.day(s.archive_cutoff_date) : fmt.time(s.last_probe_at)}</dd>
      </dl>
      {s.error && <div className="note bad">{s.error}</div>}
      <div className="note">{s.access}</div>
    </div>
  );
}

function totalsPill(b: Batch) {
  if (b.totals_rows === 1) return <StatusPill tone="ok">1 row</StatusPill>;
  if (b.totals_rows === 0) return <StatusPill tone="crit">Missing</StatusPill>;
  return <StatusPill tone="crit">{b.totals_rows} rows</StatusPill>;
}

function landedCell(b: Batch) {
  const diff = b.rows_loaded != null && b.control_total_rows != null && b.rows_loaded !== b.control_total_rows;
  return <span className={diff ? "bad" : undefined} title={diff ? "Differs from the control total: Phase 2 reconciliation will flag it" : undefined}>{fmt.n(b.rows_loaded)}</span>;
}

export default function SourcesPage() {
  const { date } = useBusinessDate();
  const sources = useSources();
  const batches = useBatches(date);
  const me = useMe();
  const replay = useReplay();
  const [confirm, setConfirm] = useState<Batch | null>(null);
  const canReplay = me.data?.roles.includes("engineer") ?? false;

  return (
    <main className="content">
      <PageHead
        title="Sources and ingestion"
        lede="Every extract lands with a batch ID, its own control totals and an immutable archive copy."
        phase="Phase 1 · Ingest and archive"
      />
      {sources.error && <ErrorBox error={sources.error} />}
      {sources.isLoading ? <Loading what="sources" /> : (
        <div className="grid3">{sources.data?.map((s) => <SourceCard key={s.system} s={s} />)}</div>
      )}

      {replay.data && (
        <div className="banner held" role="status">
          <b>Replay queued.</b>
          <span>Request <span className="mono">{replay.data.request_id.slice(0, 8)}</span> sent to <span className="mono">{replay.data.topic}</span>. {replay.data.note}</span>
        </div>
      )}
      {replay.error && <ErrorBox error={replay.error} />}

      <Panel
        title="Batches landed"
        sub={batches.data ? `${fmt.day(batches.data.business_date)} · control totals written in the same snapshot as the rows` : ""}
        foot="Archive: Cloud Storage, write-once with a retention policy. Missing control totals always block publication (Phase 3)."
      >
        {batches.error && <ErrorBox error={batches.error} />}
        {batches.isLoading && <Loading what="batches" />}
        {batches.data && batches.data.batches.length === 0 && <div className="empty">No batches for this date yet. Trigger an ingest DAG in Airflow.</div>}
        {batches.data && batches.data.batches.length > 0 && (
          <table>
            <thead>
              <tr>
                <th>Batch</th><th>Dataset</th><th>Source</th><th className="n">Rows landed</th><th className="n">Control total</th>
                <th className="n">Distinct keys</th><th className="n">Amount total</th><th>Totals row</th><th>Archive object</th>
                <th>Landed</th><th />
              </tr>
            </thead>
            <tbody>
              {batches.data.batches.map((b) => (
                <tr key={b.batch_id}>
                  <td className="mono">{b.batch_id}{b.mode === "replay" && <> <StatusPill tone="info">replay</StatusPill></>}
                    {b.injections.length > 0 && <div className="note">injected: {b.injections.join(", ")}</div>}
                    {b.status === "FAILED" && <div className="note bad">{b.error}</div>}
                  </td>
                  <td>{b.dataset}</td>
                  <td className="ink2">{b.source_system}</td>
                  <td className="n">{landedCell(b)}</td>
                  <td className="n">{fmt.n(b.control_total_rows)}</td>
                  <td className="n">{fmt.n(b.distinct_keys)}</td>
                  <td className="n">{b.amount_total == null ? "—" : fmt.inr(b.amount_total)}</td>
                  <td>{b.status === "FAILED" ? <StatusPill tone="crit">Failed</StatusPill> : totalsPill(b)}</td>
                  <td className="mono" title={b.archive_uri ?? ""}>{fmt.shortUri(b.archive_uri)}</td>
                  <td>{fmt.time(b.finished_at)}</td>
                  <td>
                    <button className="btn small" disabled={!canReplay || replay.isPending}
                      title={canReplay ? "Queue a replay from the archive" : "Requires the engineer role"}
                      onClick={() => setConfirm(b)}>Replay</button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </Panel>

      {confirm && (
        <div className="banner held" role="dialog" aria-label="Confirm replay">
          <span>Queue a replay of <b>{confirm.dataset}</b> for {fmt.day(confirm.business_date)} from archived batch <span className="mono">{confirm.batch_id}</span>? The engine decides; nothing runs directly from the console.</span>
          <span className="sp" />
          <button className="btn" onClick={() => setConfirm(null)}>Cancel</button>
          <button className="btn pri" onClick={() => {
            replay.mutate({ dataset: confirm.dataset, business_date: confirm.business_date, batch_id: confirm.batch_id, reason: "console request" });
            setConfirm(null);
          }}>Queue replay</button>
        </div>
      )}
    </main>
  );
}
