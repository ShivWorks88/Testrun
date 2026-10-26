import { Fragment, useState } from "react";
import { Link } from "react-router-dom";
import { useApprovals, useDecide, type Approval } from "../../api/approvals";
import { ErrorBox, Loading, PageHead, Panel, StatusPill, fmt, type Tone } from "../../components/ui";

// PENDING waits for a person; DECIDED = recorded, the worker has not picked it up yet.
export const approvalTone = (s: string): Tone =>
  s === "APPROVED" ? "ok" : s === "PENDING" ? "held" : s === "DECIDED" ? "info"
    : s === "REJECTED" || s === "EXPIRED" || s === "STALE" || s === "EXPIRING" ? "crit" : "pend";

const FOOT =
  "The console only records your decision. Within 5 minutes the approval worker re-checks the card " +
  "(same evidence, incident still waiting, same policy version, kill switch off, attempt cap) and only then acts.";

function minutesLeft(iso: string): number {
  return Math.max(0, Math.round((new Date(iso).getTime() - Date.now()) / 60_000));
}

function Facts({ value }: { value: Record<string, unknown> }) {
  return (
    <dl className="kv">
      {Object.entries(value).map(([k, v]) => (
        <Fragment key={k}><dt>{k}</dt><dd className="mono">{v == null ? "—" : String(v)}</dd></Fragment>
      ))}
    </dl>
  );
}

function DecisionBox({ a }: { a: Approval }) {
  const decide = useDecide();
  const [reason, setReason] = useState("");
  if (a.decision) {
    return (
      <p>
        <b>{a.decision.decision === "APPROVE" ? "Approved" : "Rejected"}</b> by {a.decision.decided_by} at{" "}
        {fmt.dateTime(a.decision.decided_at)}{a.decision.reason ? ` — “${a.decision.reason}”` : ""}
      </p>
    );
  }
  if (!a.can_decide) return <p className="ink2">You cannot decide this card: {a.why_not}.</p>;
  const send = (decision: "APPROVE" | "REJECT") =>
    decide.mutate({ approval_id: a.approval_id, decision, fingerprint: a.fingerprint, reason });
  return (
    <div>
      <label className="ink2" htmlFor={`r-${a.approval_id}`}>Reason (needed to reject; optional to approve)</label>
      <textarea id={`r-${a.approval_id}`} rows={2} style={{ width: "100%", margin: "6px 0" }} value={reason}
        maxLength={500} onChange={(e) => setReason(e.target.value)} />
      <div style={{ display: "flex", gap: 8 }}>
        <button className="btn" disabled={decide.isPending} onClick={() => send("APPROVE")}>Approve re-extract</button>
        <button className="btn" disabled={decide.isPending || reason.trim().length < 3} onClick={() => send("REJECT")}>
          Reject
        </button>
      </div>
      {decide.error && <ErrorBox error={decide.error} />}
      {decide.data && <p className="ink2">Recorded. Next: {decide.data.next}.</p>}
    </div>
  );
}

function CardView({ a }: { a: Approval }) {
  const c = a.card;
  return (
    <Panel
      title={c.title || `${a.action} · ${a.dataset ?? ""}`}
      sub={<>{a.approval_id} · incident <Link to={`/incidents/${a.incident_id}`}>{a.incident_id}</Link></>}
      right={<StatusPill tone={approvalTone(a.shown_status)}>{a.shown_status}</StatusPill>}
      foot={FOOT}
    >
      <div className="grid3">
        <div className="card">
          <h3>What happened</h3>
          <div className="desc">{c.what_happened}</div>
          <dl className="kv">
            <dt>Rule</dt><dd className="mono">{c.rule}</dd>
            <dt>Policy</dt><dd className="mono">{a.policy_version ?? "—"}</dd>
            <dt>Severity</dt><dd>{a.severity ?? "—"}</dd>
            <dt>Impact</dt><dd>{c.impact}</dd>
          </dl>
        </div>
        <div className="card">
          <h3>Evidence (counts only)</h3>
          <Facts value={c.evidence ?? {}} />
        </div>
        <div className="card">
          <h3>If you decide</h3>
          <dl className="kv">
            <dt>Approve</dt><dd>{c.if_approved}</dd>
            <dt>Reject</dt><dd>{c.if_rejected}</dd>
            <dt>Requested</dt><dd>{fmt.dateTime(a.requested_at)} by {a.requested_by ?? "—"}</dd>
            <dt>Expires</dt>
            <dd>{fmt.dateTime(a.expires_at)}{a.status === "PENDING" ? ` (${minutesLeft(a.expires_at)} min left)` : ""}</dd>
            <dt>Fingerprint</dt><dd className="mono">{a.fingerprint}</dd>
          </dl>
        </div>
      </div>
      <div style={{ padding: "8px 16px 14px" }}>
        <ul className="ink2">{c.guardrails.map((g) => <li key={g}>{g}</li>)}</ul>
        {a.status === "PENDING" ? <DecisionBox a={a} /> : <p className="ink2">{a.note}</p>}
      </div>
    </Panel>
  );
}

export default function ApprovalsPage() {
  const q = useApprovals();
  const waiting = q.data?.filter((a) => a.status === "PENDING") ?? [];
  const history = q.data?.filter((a) => a.status !== "PENDING") ?? [];
  return (
    <main className="content">
      <PageHead
        title="Agents and approvals"
        lede="Route L2 rules wait for a data owner. The engine writes the card from its own evidence; you approve or reject; the engine re-checks before it acts. Agents (advisory) arrive in stage 2."
        phase="Phase 5 · Stage 1 approvals"
      />
      {q.error && <ErrorBox error={q.error} />}
      {q.isLoading && <Loading what="approval cards" />}
      {q.data && waiting.length === 0 && (
        <Panel title="Waiting for a decision" foot={FOOT}><div className="empty">No card is waiting.</div></Panel>
      )}
      {waiting.map((a) => <CardView key={a.approval_id} a={a} />)}
      {q.data && (
        <Panel title="History" sub={`${history.length} closed card(s)`}>
          {history.length === 0 ? <div className="empty">None yet.</div> : (
            <table>
              <thead>
                <tr><th>Card</th><th>Incident</th><th>Day</th><th>Dataset</th><th>Status</th><th>Decided by</th>
                  <th>Requested</th><th>Note</th></tr>
              </thead>
              <tbody>
                {history.map((a) => (
                  <tr key={a.approval_id}>
                    <td className="mono">{a.approval_id}</td>
                    <td className="mono"><Link to={`/incidents/${a.incident_id}`}>{a.incident_id}</Link></td>
                    <td>{fmt.day(a.business_date)}</td><td>{a.dataset ?? "—"}</td>
                    <td><StatusPill tone={approvalTone(a.status)}>{a.status}</StatusPill></td>
                    <td>{a.decided_by ?? "—"}</td><td>{fmt.dateTime(a.requested_at)}</td>
                    <td className="ink2">{a.note ?? ""}</td>
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
