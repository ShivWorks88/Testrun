import { Fragment } from "react";
import { Link, useParams } from "react-router-dom";
import { useIncident, useIncidents, type IncidentEvent } from "../../api/recovery";
import { ErrorBox, Loading, PageHead, Panel, StatusPill, fmt, type Tone } from "../../components/ui";

export const incidentTone = (s: string): Tone =>
  s === "RESOLVED" ? "ok" : s === "ACTING" ? "info" : s === "ESCALATED" ? "crit" : s === "RECOMMENDED" ? "held" : "pend";
const KILL = "Rules, attempt cap and kill switch come from policy/policy.yml (version and kill switch shown in the left rail). Kill switch ON = recommend only.";

export function IncidentsPage() {
  const q = useIncidents();
  const open = q.data?.filter((i) => i.status !== "RESOLVED").length ?? 0;
  return (
    <main className="content">
      <PageHead
        title="Incidents"
        lede="Every held day becomes one incident. The engine diagnoses it against the receipt, decides with five gates, fixes only what is safe, and closes it after the checks pass again."
        phase="Phase 4 · Decide and recover"
      />
      {q.error && <ErrorBox error={q.error} />}
      {q.isLoading && <Loading what="incidents" />}
      {q.data && (
        <Panel title="Incidents" sub={`${q.data.length} total · ${open} open`} foot={KILL}>
          {q.data.length === 0 ? <div className="empty">No incidents yet.</div> : (
            <table>
              <thead>
                <tr><th>Incident</th><th>Business day</th><th>Dataset</th><th>Problem</th><th>Severity</th><th>Status</th>
                  <th className="n">Attempt</th><th className="n">Minutes open</th><th>Opened</th><th>Latest note</th></tr>
              </thead>
              <tbody>
                {q.data.map((i) => (
                  <tr key={i.incident_id}>
                    <td className="mono"><Link to={`/incidents/${i.incident_id}`}>{i.incident_id}</Link></td>
                    <td>{fmt.day(i.business_date)}</td><td>{i.dataset ?? "—"}</td><td className="mono">{i.failure_type}</td>
                    <td>{i.severity ?? "—"}</td>
                    <td><StatusPill tone={incidentTone(i.status)}>{i.status}</StatusPill></td>
                    <td className="n">{i.attempt}</td><td className="n">{i.minutes_open}</td>
                    <td>{fmt.dateTime(i.opened_at)}</td><td className="ink2">{i.note ?? ""}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </Panel>
      )}
    </main>
  );
}

function Evidence({ value }: { value: unknown }) {
  if (value == null) return <span className="ink2">—</span>;
  if (typeof value !== "object") return <span>{String(value)}</span>;
  return (
    <dl className="kv">
      {Object.entries(value as Record<string, unknown>).map(([k, v]) => (
        <Fragment key={k}><dt>{k}</dt><dd className="mono">{typeof v === "object" ? JSON.stringify(v) : String(v ?? "—")}</dd></Fragment>
      ))}
    </dl>
  );
}

export function IncidentDetailPage() {
  const { id } = useParams();
  const q = useIncident(id);
  const d = q.data;
  const decision: IncidentEvent | undefined = d ? [...d.events].reverse().find((e) => e.gates) : undefined;
  // the diagnosis (counts) recorded with the decision; fall back to the latest evidence
  const latestEvidence = decision?.evidence ?? (d ? [...d.events].reverse().find((e) => e.evidence != null)?.evidence : undefined);
  return (
    <main className="content">
      <PageHead title={id ?? "Incident"} lede="Five-gate decision, evidence (counts only) and the action journal." phase="Phase 4 · Decide and recover"
        actions={<Link className="btn" to="/incidents">All incidents</Link>} />
      {q.error && <ErrorBox error={q.error} />}
      {q.isLoading && <Loading what="the incident" />}
      {d && (
        <>
          <div className="grid3">
            <div className="card">
              <h3>{d.summary.failure_type}</h3>
              <div className="desc">{d.summary.dataset ?? "—"} · {fmt.day(d.summary.business_date)}</div>
              <dl className="kv">
                <dt>Status</dt><dd><StatusPill tone={incidentTone(d.summary.status)}>{d.summary.status}</StatusPill></dd>
                <dt>Attempt</dt><dd>{d.summary.attempt} of 2</dd>
                <dt>Open for</dt><dd>{d.summary.minutes_open} min</dd>
                <dt>Opened</dt><dd>{fmt.dateTime(d.summary.opened_at)}</dd>
              </dl>
            </div>
            <div className="card">
              <h3>Decision</h3>
              <dl className="kv">
                <dt>Action</dt><dd className="mono">{decision?.action ?? d.summary.action ?? "—"}</dd>
                <dt>Mode</dt><dd>{decision?.mode ?? d.summary.mode ?? "—"}</dd>
                <dt>Rule</dt><dd className="mono">{decision?.rule_id ?? d.summary.rule_id ?? "— (before v5.2)"}</dd>
                <dt>Severity · route</dt><dd>{decision?.severity ?? d.summary.severity ?? "—"} · {decision?.route ?? "—"}</dd>
                <dt>Policy</dt><dd className="mono">{decision?.policy_version ?? d.summary.policy_version ?? "—"}</dd>
                <dt>Latest note</dt><dd>{d.summary.note ?? "—"}</dd>
              </dl>
            </div>
            <div className="card">
              <h3>Evidence</h3>
              <Evidence value={latestEvidence} />
            </div>
          </div>

          <Panel title="Five gates" sub={decision ? `decided ${fmt.dateTime(decision.at)}` : "no decision recorded"}>
            {decision?.gates ? (
              <table>
                <thead><tr><th className="n">Gate</th><th>Question</th><th>Answer</th><th>Result</th></tr></thead>
                <tbody>
                  {decision.gates.map((g) => (
                    <tr key={g.gate}>
                      <td className="n">{g.gate}</td><td>{g.question}</td><td>{g.answer}</td>
                      <td><StatusPill tone={g.pass ? "ok" : "crit"}>{g.pass ? "pass" : "stop"}</StatusPill></td>
                    </tr>
                  ))}
                </tbody>
              </table>
            ) : <div className="empty">—</div>}
          </Panel>

          <Panel title="Action journal" sub="written before and after every action; append-only" foot={KILL}>
            <table>
              <thead><tr><th>When</th><th>Status</th><th className="n">Attempt</th><th>Action</th><th>Mode</th><th>Note</th></tr></thead>
              <tbody>
                {d.events.map((e, i) => (
                  <tr key={i}>
                    <td>{fmt.dateTime(e.at)}</td>
                    <td><StatusPill tone={incidentTone(e.status)}>{e.status}</StatusPill></td>
                    <td className="n">{e.attempt ?? "—"}</td><td className="mono">{e.action ?? "—"}</td>
                    <td>{e.mode ?? "—"}</td><td className="ink2">{e.note ?? ""}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </Panel>
        </>
      )}
    </main>
  );
}
