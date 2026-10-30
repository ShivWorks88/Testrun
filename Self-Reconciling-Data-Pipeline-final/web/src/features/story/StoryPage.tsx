import { useEffect, useMemo, useRef, useState } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { KIND, KINDS, plain, useOperations, type Kind, type OpsIncident } from "../../api/operations";
import { useIncident, type IncidentEvent } from "../../api/recovery";
import { ErrorBox, Loading, PageHead, StatusPill, fmt } from "../../components/ui";
import { incidentTone } from "../incidents/IncidentsPage";

// Phase 6 · Pipeline story: replays a REAL incident from the journal on a map of the pipeline.
// Every step is one journal row (what the engine wrote, when); the map only colours what that row says.

type NodeState = "ok" | "fail" | "held" | "act" | "exp" | "wait";
const NODES: Record<string, [number, number, number, number, string, string]> = {
  ehr: [30, 40, 140, 64, "EHR", "encounters · patients"],
  lis: [30, 140, 140, 64, "LIS (lab)", "results · orders"],
  billing: [30, 240, 140, 64, "Billing", "claims · payments"],
  raw: [240, 130, 160, 84, "Raw + receipt", "BigQuery · asia-south1"],
  dbt: [450, 130, 160, 84, "dbt build + checks", "audit · reconcile"],
  gate: [660, 130, 130, 84, "Publication gate", "copies only on pass"],
  r_enc: [838, 16, 146, 62, "Encounters", "dim_encounters"],
  r_bed: [838, 86, 146, 62, "Bed occupancy", "fct_bed_occupancy"],
  r_clm: [838, 156, 146, 62, "Daily claims", "fct_daily_claims"],
  r_lab: [838, 226, 146, 62, "Lab results", "fct_lab_results_daily"],
  breaker: [30, 350, 140, 60, "Outage breaker", "one incident per outage"],
  agent: [240, 340, 160, 70, "AI agent", "advises · Gemini, Mumbai"],
  engine: [450, 330, 160, 90, "Recovery engine", "policy.yml"],
  slack: [660, 340, 130, 70, "Data owner", "approves in Slack"],
};
const EDGES: Record<string, string> = {
  e1: "M170 72 C205 72 205 150 240 150", e2: "M170 172 L240 172", e3: "M170 272 C205 272 205 194 240 194",
  e4: "M400 172 L450 172", e5: "M610 172 L660 172",
  g1: "M790 160 C814 160 814 47 838 47", g2: "M790 166 C814 166 814 117 838 117",
  g3: "M790 178 C814 178 814 187 838 187", g4: "M790 186 C814 186 814 257 838 257",
  d1: "M530 214 L530 330", x1: "M450 360 C330 360 320 300 320 214", x2: "M450 375 L400 375",
  x3: "M610 375 L660 375", b1: "M100 350 L100 304", b2: "M450 400 C300 430 200 430 170 395",
};
const WORD: Record<NodeState, string> = { ok: "PASSING", fail: "FAILING", held: "HELD", act: "WORKING", exp: "EXPECTED", wait: "WAITING" };

const SOURCE: Record<string, string> = {
  encounters: "ehr", patients: "ehr", diagnoses: "ehr", archive_index: "ehr",
  lab_orders: "lis", lab_panels: "lis", lab_results: "lis",
  claims: "billing", payments: "billing", adjustments: "billing",
};
const REPORTS: Record<string, string[]> = {
  encounters: ["r_enc", "r_bed"], patients: ["r_enc"], diagnoses: ["r_enc"],
  lab_results: ["r_lab"], lab_orders: ["r_lab"], lab_panels: ["r_lab"],
  claims: ["r_clm"], payments: ["r_clm"], adjustments: ["r_clm"],
};
const REPORT_EDGE: Record<string, string> = { r_enc: "g1", r_bed: "g2", r_clm: "g3", r_lab: "g4" };
const SOURCE_EDGE: Record<string, string> = { ehr: "e1", lis: "e2", billing: "e3" };
const SOURCE_SIDE = new Set(["late_change", "late_change_old", "archived_history", "count_excess", "young_orphan",
  "archived_parent", "true_orphan", "merged_patient", "stale_feed", "late_data", "extract_failed", "source_outage",
  "global_outage"]);
const OUTAGE = new Set(["source_outage", "global_outage"]);

type Frame = { nodes: Record<string, NodeState>; edges: string[]; tone: "bad" | "fix" | "" };

// What one journal row means on the map (cumulative: earlier rows stay coloured unless a later row changes them).
function frames(inc: OpsIncident, events: IncidentEvent[]): Frame[] {
  const ds = inc.dataset ?? "";
  const src = SOURCE[ds] ?? (OUTAGE.has(inc.failure_type) ? "billing" : "");
  const reps = REPORTS[ds] ?? (OUTAGE.has(inc.failure_type) ? ["r_clm"] : []);
  const sourceSide = SOURCE_SIDE.has(inc.failure_type);
  const allOk: Record<string, NodeState> = Object.fromEntries(
    ["ehr", "lis", "billing", "raw", "dbt", "gate", "r_enc", "r_bed", "r_clm", "r_lab"].map((n) => [n, "ok" as NodeState]));
  let cur: Record<string, NodeState> = { ...allOk };
  const out: Frame[] = [];
  // step 0: where it starts
  if (OUTAGE.has(inc.failure_type)) cur = { ...cur, [src]: "fail", raw: "fail" };
  else if (sourceSide) cur = { ...cur, ...(src ? { [src]: "fail" } : {}) };
  else cur = { ...cur, dbt: "fail" };
  out.push({ nodes: { ...cur }, edges: src && !sourceSide ? [SOURCE_EDGE[src], "e4"] : [], tone: "bad" });
  for (const e of events) {
    const n: Record<string, NodeState> = { ...cur };
    let edges: string[] = [];
    let tone: Frame["tone"] = "";
    switch (e.status) {
      case "OPEN":
        n.engine = "act"; edges = sourceSide ? ["x1"] : ["d1"]; tone = "bad"; break;
      case "HELD":
        n.engine = "held"; n.gate = "held"; reps.forEach((r) => (n[r] = "held"));
        if (OUTAGE.has(inc.failure_type)) { n.breaker = "fail"; edges = ["b2", "b1"]; } else edges = ["e5"];
        break;
      case "ESCALATED": case "RECOMMENDED":
        n.engine = "held"; if (!sourceSide || inc.failure_type === "true_orphan") { n.gate = "held"; reps.forEach((r) => (n[r] = "held")); }
        edges = ["d1"]; break;
      case "AWAITING_APPROVAL":
        n.engine = "wait"; n.slack = "wait"; n.gate = "held"; reps.forEach((r) => (n[r] = "held")); edges = ["x3"]; break;
      case "WAITING":
        n.engine = "wait"; if (src) n[src] = "wait"; edges = ["x1"]; break;
      case "EXPECTED":
        n.engine = "exp"; if (src) n[src] = "exp"; n.gate = "ok"; reps.forEach((r) => (n[r] = "ok"));
        edges = ["e5", ...reps.map((r) => REPORT_EDGE[r])]; break;
      case "ACTING":
        n.engine = "act"; n.raw = "act"; n.dbt = "act"; if (src) n[src] = "act"; if (cur.slack) n.slack = "ok";
        edges = ["x1", ...(src ? [SOURCE_EDGE[src]] : []), "e4"]; tone = "fix"; break;
      case "RESOLVED":
        Object.assign(n, allOk, { engine: "ok" }, cur.slack ? { slack: "ok" } : {}, cur.breaker ? { breaker: "ok" } : {});
        edges = ["e4", "e5", ...reps.map((r) => REPORT_EDGE[r])]; tone = "fix"; break;
      default:
        n.engine = "act";
    }
    if (e.mode === "approve" && e.status !== "RESOLVED") { n.slack = n.slack ?? "wait"; }
    cur = n;
    out.push({ nodes: { ...n }, edges, tone });
  }
  return out;
}

export default function StoryPage() {
  const ops = useOperations(7);
  const [params, setParams] = useSearchParams();
  // one incident per failure type: the newest one that has finished (or the newest at all)
  const choices = useMemo(() => {
    const seen = new Map<string, OpsIncident>();
    for (const i of ops.data?.incidents ?? []) {
      const prev = seen.get(i.failure_type);
      if (!prev || (prev.status !== "RESOLVED" && i.status === "RESOLVED")) seen.set(i.failure_type, i);
    }
    return [...seen.values()];
  }, [ops.data]);
  const id = params.get("incident") ?? choices.find((c) => c.failure_type === "offsetting_errors")?.incident_id ?? choices[0]?.incident_id;
  const chosen = ops.data?.incidents.find((i) => i.incident_id === id);
  return (
    <main className="content">
      <PageHead title="Pipeline story"
        lede="Replays a real incident, one journal row at a time: what broke, what the engine decided at each of the five gates, and how it ended."
        phase="Phase 6 · Demo" actions={<Link className="btn" to="/operations">Operations</Link>} />
      {ops.error && <ErrorBox error={ops.error} />}
      {ops.isLoading && <Loading what="incidents" />}
      {ops.data && (
        <nav className="srail" aria-label="Incidents to replay">
          {KINDS.map((k) => {
            const list = choices.filter((c) => c.kind === k);
            if (!list.length) return null;
            return (
              <div key={k} className="sgroup">
                <span className="sglabel">{KIND[k].label}</span>
                <div className="schips">
                  {list.map((c) => (
                    <button key={c.incident_id} className="schip" aria-pressed={c.incident_id === id}
                      onClick={() => setParams({ incident: c.incident_id })} title={plain(c.failure_type)}>
                      <i style={{ background: KIND[k].color }} />{c.failure_type}
                    </button>
                  ))}
                </div>
              </div>
            );
          })}
        </nav>
      )}
      {chosen ? <Replay key={chosen.incident_id} inc={chosen} /> : ops.data && <div className="panel"><div className="empty">No incidents in the last 7 days to replay.</div></div>}
    </main>
  );
}

function Replay({ inc }: { inc: OpsIncident }) {
  const q = useIncident(inc.incident_id);
  const events = q.data?.events ?? [];
  const fr = useMemo(() => frames(inc, events), [inc, events]);
  const [step, setStep] = useState(0);
  const [playing, setPlaying] = useState(false);
  const timer = useRef<number | null>(null);
  const last = fr.length - 1;
  useEffect(() => {
    if (!playing) return;
    if (step >= last) { setPlaying(false); return; }
    timer.current = window.setTimeout(() => setStep((s) => Math.min(last, s + 1)), 2600);
    return () => { if (timer.current) window.clearTimeout(timer.current); };
  }, [playing, step, last]);
  useEffect(() => {
    const k = (e: KeyboardEvent) => {
      if ((e.target as HTMLElement).closest("input,textarea,select")) return;
      if (e.key === "ArrowRight") setStep((s) => Math.min(last, s + 1));
      if (e.key === "ArrowLeft") setStep((s) => Math.max(0, s - 1));
    };
    window.addEventListener("keydown", k);
    return () => window.removeEventListener("keydown", k);
  }, [last]);
  if (q.error) return <ErrorBox error={q.error} />;
  if (q.isLoading || !q.data) return <Loading what="the incident journal" />;
  const f = fr[step];
  const ev: IncidentEvent | undefined = step > 0 ? events[step - 1] : undefined;
  const decision = [...events.slice(0, Math.max(0, step))].reverse().find((e) => e.gates);
  const kind: Kind = inc.kind;
  return (
    <>
      <section className="stage">
        <div className="stage-head">
          <div>
            <h2>{inc.failure_type} <span className="muted">· {plain(inc.failure_type)}</span></h2>
            <div className="ink2">{inc.dataset ?? "—"} · business day {fmt.day(inc.business_date)} · <span className="mono">{inc.incident_id}</span> · {KIND[kind].label}</div>
          </div>
          <span className="sp" />
          <div className="controls">
            <button className="btn small" onClick={() => { setPlaying(false); setStep(0); }}>Restart</button>
            <button className="btn small" disabled={step === 0} onClick={() => { setPlaying(false); setStep(step - 1); }}>◀ Back</button>
            <button className="btn small pri" onClick={() => { if (step >= last) setStep(0); setPlaying((p) => !p); }}>
              {playing ? "❚❚ Pause" : step >= last ? "↻ Replay" : "▶ Play"}</button>
            <button className="btn small" disabled={step >= last} onClick={() => { setPlaying(false); setStep(step + 1); }}>Next ▶</button>
            <div className="dots" aria-label={`Step ${step + 1} of ${fr.length}`}>
              {fr.map((_, i) => <button key={i} className={i < step ? "done" : i === step ? "now" : ""} onClick={() => { setPlaying(false); setStep(i); }} aria-label={`Step ${i + 1}`} />)}
            </div>
          </div>
        </div>
        <div className="mapscroll">
          <svg viewBox="0 0 1000 440" className="map" role="img" aria-label="Pipeline map for this step">
            <defs>
              <marker id="ar" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" className="arrow" /></marker>
              <marker id="aron" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" className="arrow on" /></marker>
            </defs>
            <rect x={14} y={14} width={172} height={306} rx={10} className="zone" />
            <text x={26} y={32} className="zl">HOSPITAL SOURCES</text>
            <rect x={826} y={4} width={170} height={296} rx={10} className="zone" />
            <text x={836} y={316} className="zl">REPORTS USERS SEE</text>
            <text x={450} y={316} className="zl">WHEN A CHECK FAILS</text>
            {Object.entries(EDGES).map(([eid, d]) => {
              const on = f.edges.includes(eid);
              return <path key={eid} d={d} className={`edge${eid === "x1" || eid === "b2" ? " loop" : ""}${on ? ` on ${f.tone}` : ""}`} markerEnd={on ? "url(#aron)" : "url(#ar)"} />;
            })}
            {Object.entries(NODES).map(([nid, [x, y, w, h, t, s]]) => {
              const st = f.nodes[nid];
              const small = h < 70;
              return (
                <g key={nid} className={`node ${st ?? "dim"}`}>
                  <rect x={x} y={y} width={w} height={h} rx={8} />
                  <text x={x + 12} y={y + (small ? 20 : 22)}>{t}</text>
                  <text x={x + 12} y={y + (small ? 36 : 39)} className="sub">{s}</text>
                  {st && <text x={x + 12} y={y + h - (small ? 9 : 10)} className="state">{WORD[st]}</text>}
                </g>
              );
            })}
          </svg>
        </div>
      </section>
      <div className="grid3">
        <section className="card" aria-live="polite">
          <div className="row"><span className="eyebrow">Step {step + 1} of {fr.length}</span><span className="sp" />
            <span className="mono muted">{ev ? fmt.time(ev.at) : fmt.time(inc.opened_at)}</span></div>
          {ev ? (
            <>
              <h3><StatusPill tone={incidentTone(ev.status)}>{ev.status}</StatusPill>{ev.action && <span className="mono"> {ev.action}</span>}</h3>
              <p className="say">{ev.note || "(no note on this row)"}</p>
            </>
          ) : (
            <>
              <h3>What broke</h3>
              <p className="say">{plain(inc.failure_type) ? `${plain(inc.failure_type).replace(/^./, (c) => c.toUpperCase())} — ` : ""}{inc.dataset ?? "a dataset"}, business day {fmt.day(inc.business_date)}. Press Play to follow what the pipeline did.</p>
            </>
          )}
          {ev?.evidence != null && typeof ev.evidence === "object" && (
            <dl className="kv">
              {Object.entries(ev.evidence as Record<string, unknown>).slice(0, 8).map(([k, v]) => (
                <div key={k} style={{ display: "contents" }}><dt>{k.replace(/_/g, " ")}</dt><dd className="mono">{typeof v === "object" ? JSON.stringify(v) : String(v ?? "—")}</dd></div>
              ))}
            </dl>
          )}
        </section>
        <section className="card">
          <div className="row"><span className="eyebrow">The five gates</span><span className="sp" />
            {decision?.rule_id && <span className="mono muted">{decision.rule_id}</span>}</div>
          {decision?.gates ? (
            <ol className="gates5">
              {decision.gates.map((g) => (
                <li key={g.gate} className={g.pass ? "pass" : "stop"}><span className="n">{g.gate}</span>
                  <div><b>{g.question}</b><div className="ink2">{g.answer}</div></div></li>
              ))}
            </ol>
          ) : <p className="muted">The gates light up when the engine records its decision.</p>}
        </section>
        <section className="card">
          <div className="row"><span className="eyebrow">Incident journal</span><span className="sp" />
            <Link className="mono" to={`/incidents/${inc.incident_id}`}>full page</Link></div>
          <ol className="journal">
            {events.slice(0, step).map((e, i) => (
              <li key={i} className={i === step - 1 ? "now" : ""}>
                <span className="mono muted">{fmt.time(e.at)}</span>
                <StatusPill tone={incidentTone(e.status)}>{e.status}</StatusPill>
                <span>{e.note ?? ""}</span>
              </li>
            ))}
            {step === 0 && <li><span /><span /><span className="muted">Nothing recorded yet.</span></li>}
          </ol>
        </section>
      </div>
      <div className="legend">
        {(["ok", "fail", "held", "act", "exp", "wait"] as NodeState[]).map((s) => <span key={s} className={`lg ${s}`}><i />{WORD[s].toLowerCase()}</span>)}
        <span className="sp" /><span className="muted">Map colours come only from the journal rows; the words are the engine's own.</span>
      </div>
    </>
  );
}
