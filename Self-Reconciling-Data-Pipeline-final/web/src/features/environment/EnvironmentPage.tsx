import { useEnvironment } from "../../api/client";
import { ErrorBox, Loading, PageHead, Panel, StatusPill, fmt, stateTone } from "../../components/ui";

const accessTone = { Write: "ok", Read: "info", Masked: "cav", "—": "pend" } as const;

export default function EnvironmentPage() {
  const q = useEnvironment();
  return (
    <main className="content">
      <PageHead
        title="Environment"
        lede="The POC test project, built from Terraform, with one identity per component and synthetic patients only."
        phase="Phase 0 · Foundations"
      />
      {q.isLoading && <Loading what="environment checks" />}
      {q.error && <ErrorBox error={q.error} />}
      {q.data && (
        <>
          <div className="grid2">
            <Panel title="Resources" sub={`${q.data.resources.filter((r) => r.state === "ready").length} of ${q.data.resources.length} ready`}
              foot="Checked live against the project each time this page loads.">
              <table>
                <thead><tr><th>Resource</th><th>Service</th><th className="n">State</th></tr></thead>
                <tbody>
                  {q.data.resources.map((r) => (
                    <tr key={r.name} title={r.detail}>
                      <td>{r.name}<div className="note">{r.detail}</div></td>
                      <td className="ink2">{r.service}</td>
                      <td className="n"><StatusPill tone={stateTone[r.state] ?? "pend"}>{r.label}</StatusPill></td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </Panel>
            <Panel title="Policy and AI" sub="policy/policy.yml · agents/registry.yml">
              <dl className="kv" style={{ padding: 16 }}>
                <dt>Policy version</dt><dd className="mono">{q.data.policy.version}</dd>
                <dt>Kill switch</dt><dd>{q.data.policy.kill_switch ? <StatusPill tone="held">ON: recommend only</StatusPill> : <StatusPill tone="ok">off</StatusPill>}</dd>
                <dt>Catalog actions</dt><dd>{q.data.policy.actions}</dd>
                <dt>Rules</dt><dd>{q.data.policy.rules}</dd>
                <dt>AI agents</dt><dd>{q.data.policy.agents_enabled ? <StatusPill tone="ok">advisory</StatusPill> : <StatusPill tone="pend">off</StatusPill>}</dd>
                <dt>Residency confirmed</dt><dd>{q.data.policy.residency_confirmed ? <StatusPill tone="ok">yes</StatusPill> : <StatusPill tone="held">not yet</StatusPill>}</dd>
                <dt>API runs as</dt><dd className="mono" style={{ wordBreak: "break-all" }}>{q.data.runs_as}</dd>
                <dt>Build</dt><dd className="mono">{q.data.build.git_sha}</dd>
              </dl>
            </Panel>
          </div>

          <Panel title="Identities and access" sub="one service account per component; read from the live dataset ACLs"
            foot="Only Publish can write analytics, and only Ingest can write raw. Permissions enforce the design even if code is wrong.">
            <table>
              <thead>
                <tr><th>Service account</th>{q.data.identity_columns.map((c) => <th key={c}>{c}</th>)}</tr>
              </thead>
              <tbody>
                {q.data.identities.map((i) => (
                  <tr key={i.component}>
                    <td>{i.component}<div className="note mono">{i.email}</div></td>
                    {q.data.identity_columns.map((c) => {
                      const a = (i.access[c] ?? "—") as keyof typeof accessTone;
                      return <td key={c}>{a === "—" ? <span className="muted">—</span> : <StatusPill tone={accessTone[a]}>{a}</StatusPill>}</td>;
                    })}
                  </tr>
                ))}
              </tbody>
            </table>
          </Panel>

          <Panel title="Synthetic data" sub="data-gen · seed 2026 · latest loaded batch per dataset"
            foot={<>Generate a business date locally: <span className="mono">make generate DATE=2026-10-13</span>, then trigger the ingest DAGs.</>}>
            <table>
              <thead><tr><th>Dataset</th><th>Source</th><th>Business date</th><th className="n">Rows</th><th>Edge cases built in</th></tr></thead>
              <tbody>
                {q.data.synthetic.map((s) => (
                  <tr key={s.dataset}>
                    <td className="mono">{s.dataset}</td>
                    <td className="ink2">{s.source_system}</td>
                    <td>{fmt.day(s.business_date)}</td>
                    <td className="n">{fmt.n(s.rows)}</td>
                    <td className="ink2">{s.edge_cases}</td>
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
