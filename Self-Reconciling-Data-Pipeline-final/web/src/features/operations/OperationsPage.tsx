import { Fragment, useEffect, useMemo, useRef, useState, type FocusEvent, type PointerEvent, type ReactNode } from "react";
import { Link } from "react-router-dom";
import {
  KIND, KINDS, REPORT, plain, useAsk, useOperations,
  type AgentCall, type Kind, type Operations, type OpsIncident,
} from "../../api/operations";
import { useIncident, type IncidentEvent } from "../../api/recovery";
import { ErrorBox, Loading, PageHead, Panel, StatusPill, fmt, type Tone } from "../../components/ui";
import { incidentTone } from "../incidents/IncidentsPage";
import { stateTone } from "../publication/PublicationPage";

// Phase 6 · Operations: is everything OK, what happened, and why — from the pipeline's own records.
export default function OperationsPage() {
  const q = useOperations(7);
  const [open, setOpen] = useState<string | null>(null);
  const o = q.data;
  return (
    <main className="content">
      <PageHead
        title="Operations"
        lede="One view of the whole pipeline: today's reports, how every failure was handled, and how fast. Read-only — fixes still go through the engine and approval cards."
        phase="Phase 6 · Operate"
      />
      {q.error && <ErrorBox error={q.error} />}
      {q.isLoading && <Loading what="the operations summary" />}
      {o && (
        <>
          <Glance o={o} />
          <Kpis o={o} />
          <Board o={o} />
          <Assistant available={o.assistant_available} />
          <div className="grid2">
            <MixChart o={o} />
            <PerDayChart o={o} />
          </div>
          <div className="grid2">
            <ResolveChart o={o} />
            <PolicyCard o={o} />
          </div>
          <IncidentList items={o.incidents} onOpen={setOpen} />
          <AgentCalls calls={o.agent_calls} />
        </>
      )}
      {open && <Drawer id={open} onClose={() => setOpen(null)} />}
    </main>
  );
}

// ── today at a glance ──────────────────────────────────────────────────────
function Glance({ o }: { o: Operations }) {
  const published = o.today.filter((r) => r.state === "PUBLISHED").length;
  const outage = o.shell.open_breakers;
  const allGood = published === o.today.length && o.kpis.open_incidents === 0 && o.shell.pending_cards === 0 && !outage.length;
  const sentence = allGood
    ? `All ${o.today.length} reports for ${fmt.day(o.latest_business_date)} are published and nothing needs attention.`
    : `${published} of ${o.today.length} reports for ${fmt.day(o.latest_business_date)} are published` +
      (outage.length ? `; ${outage.map((b) => b.source_system).join(" and ")} is down` : "") +
      (o.shell.pending_cards ? `; ${o.shell.pending_cards} card${o.shell.pending_cards > 1 ? "s" : ""} wait${o.shell.pending_cards > 1 ? "" : "s"} for a decision` : "") + ".";
  return (
    <section className={`glance ${allGood ? "good" : "attn"}`} aria-label="Today at a glance">
      <p className="glance-s">{sentence}</p>
      <div className="glance-chips">
        <span className="chip"><b>{published} of {o.today.length}</b> reports published</span>
        <span className="chip"><b>{o.kpis.open_incidents}</b> open incident{o.kpis.open_incidents === 1 ? "" : "s"}</span>
        <Link className="chip" to="/agents"><b>{o.shell.pending_cards}</b> card{o.shell.pending_cards === 1 ? "" : "s"} waiting</Link>
        <span className="chip">{outage.length ? <b className="bad">Outage: {outage.map((b) => b.source_system).join(", ")}</b> : <>no outage</>}</span>
        <span className="sp" />
        <span className="fresh" title="When the pipeline last wrote something">
          Last load {fmt.time(o.freshness.last_batch_at)} · gate {fmt.time(o.freshness.last_gate_at)} · engine {fmt.time(o.freshness.last_engine_at)} · updated {fmt.time(o.generated_at)}
        </span>
      </div>
    </section>
  );
}

function Kpis({ o }: { o: Operations }) {
  const k = o.kpis;
  const tiles: [string, ReactNode, string][] = [
    ["Incidents", <>{k.incidents}</>, `in the last ${o.window_days} days · ${k.resolved} resolved`],
    ["Handled with no person", <>{k.without_a_person}<small> of {k.incidents}</small></>, "fixed automatically or recorded as expected"],
    ["Median time to resolve", <>{k.median_minutes_to_resolve ?? "—"}<small> min</small></>, "from the first record to RESOLVED"],
    ["Report-days held", <>{k.held_report_days}</>, "users kept the last verified figures instead"],
  ];
  return (
    <div className="kpis">
      {tiles.map(([t, v, d]) => (
        <div className="kpi" key={t}><span className="kt">{t}</span><span className="kv2">{v}</span><span className="kd">{d}</span></div>
      ))}
    </div>
  );
}

// ── report status board ────────────────────────────────────────────────────
function Board({ o }: { o: Operations }) {
  return (
    <Panel title="Is today's report trustworthy?" sub="each report × the last 7 business days · hover a cell for the gate's reasons"
      foot={<>Held means users still see the last verified day. Full history on <Link to="/publication">Publication</Link>.</>}>
      {o.board.length === 0 ? <div className="empty">The gate has not decided anything yet.</div> : (
        <table>
          <thead><tr><th>Report</th>{o.board_dates.map((d) => <th key={d}>{fmt.day(d)}</th>)}</tr></thead>
          <tbody>
            {o.board.map((r) => (
              <tr key={r.dataset}>
                <td><b>{REPORT[r.dataset] ?? r.dataset}</b><div className="mono muted">{r.dataset}</div></td>
                {r.cells.map((c) => (
                  <td key={c.business_date ?? ""} title={c.reasons ?? ""}>
                    {c.state === "NONE" ? <span className="muted">—</span>
                      : <StatusPill tone={stateTone(c.state)}>{c.state.toLowerCase()}</StatusPill>}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </Panel>
  );
}

// ── charts (SVG, hover + keyboard tooltips; colour never the only cue) ─────
type Tip = { x: number; y: number; lines: [string, string?][] } | null;
function useTip() {
  const [tip, setTip] = useState<Tip>(null);
  const bind = (lines: [string, string?][]) => ({
    tabIndex: 0,
    onPointerMove: (e: PointerEvent) => setTip({ x: e.clientX, y: e.clientY, lines }),
    onPointerLeave: () => setTip(null),
    onFocus: (e: FocusEvent<SVGElement>) => {
      const r = e.currentTarget.getBoundingClientRect();
      setTip({ x: r.left + r.width / 2, y: r.top, lines });
    },
    onBlur: () => setTip(null),
  });
  const node = tip && (
    <div className="tip" role="tooltip" style={{ left: Math.min(window.innerWidth - 300, tip.x + 14), top: Math.max(8, tip.y - 70) }}>
      {tip.lines.map(([t, color], i) => (
        <div key={i} className={i === 0 ? "tv" : "tl"}>{color && <i style={{ background: color }} />}{t}</div>
      ))}
    </div>
  );
  return { bind, node };
}

function Keys({ kinds = KINDS }: { kinds?: Kind[] }) {
  return <div className="keys">{kinds.map((k) => <span key={k}><i style={{ background: KIND[k].color }} />{KIND[k].label}</span>)}</div>;
}

function MixChart({ o }: { o: Operations }) {
  const { bind, node } = useTip();
  const total = o.mix.reduce((s, m) => s + m.count, 0);
  let x = 0;
  return (
    <Panel title="How failures were handled" sub={`${total} incidents · last ${o.window_days} days`}>
      <div className="chartbox">
        <Keys />
        {total === 0 ? <div className="empty">No incidents in this window.</div> : (
          <svg viewBox="0 0 560 70" className="chart" role="img" aria-label="Incidents by how they were handled">
            {o.mix.filter((m) => m.count).map((m) => {
              const w = (540 * m.count) / total, x0 = 10 + x;
              x += w;
              return (
                <g key={m.kind} className="mark" {...bind([[`${m.count} of ${total}`], [KIND[m.kind].label, KIND[m.kind].color], [m.failure_types.join(", ")]])}>
                  <rect x={x0} y={8} width={Math.max(0, w - 2)} height={34} rx={4} fill={KIND[m.kind].color} className="paint" />
                  <text x={x0 + 6} y={60} className="ink">{m.count}</text>
                </g>
              );
            })}
          </svg>
        )}
        <ul className="kinds">
          {o.mix.map((m) => (
            <li key={m.kind}><i style={{ background: KIND[m.kind].color }} />
              <b>{KIND[m.kind].label} <span>{m.count}</span></b>
              <p>{KIND[m.kind].what}{m.failure_types.length ? `: ${m.failure_types.join(", ")}` : ""}.</p>
            </li>
          ))}
        </ul>
      </div>
      {node}
    </Panel>
  );
}

function PerDayChart({ o }: { o: Operations }) {
  const { bind, node } = useTip();
  const max = Math.max(1, ...o.per_day.map((d) => KINDS.reduce((s, k) => s + (d.counts[k] ?? 0), 0)));
  const step = niceStep(max);
  const top = Math.ceil(max / step) * step;
  const L = 34, R = 550, T = 8, B = 176, bw = (R - L) / o.per_day.length;
  const y = (v: number) => B - ((B - T) * v) / top;
  return (
    <Panel title="Incidents per day" sub="by the day they opened (India time)">
      <div className="chartbox">
        <Keys />
        <svg viewBox="0 0 560 200" className="chart" role="img" aria-label="Incidents opened per day, by how they were handled">
          {Array.from({ length: top / step + 1 }, (_, i) => i * step).map((v) => (
            <g key={v}><line x1={L} x2={R} y1={y(v)} y2={y(v)} className="grid" /><text x={L - 6} y={y(v) + 4} textAnchor="end">{v}</text></g>
          ))}
          {o.per_day.map((d, i) => {
            let acc = 0;
            const total = KINDS.reduce((s, k) => s + (d.counts[k] ?? 0), 0);
            return (
              <g key={d.day} className="mark" {...bind([[`${total} incident${total === 1 ? "" : "s"}`], [fmt.day(d.day)],
                ...KINDS.filter((k) => d.counts[k]).map((k) => [`${d.counts[k]} ${KIND[k].label.toLowerCase()}`, KIND[k].color] as [string, string])])}>
                <rect x={L + i * bw} y={T} width={bw} height={B - T} className="hit" />
                {KINDS.map((k) => {
                  const v = d.counts[k] ?? 0;
                  if (!v) return null;
                  const y0 = y(acc + v), h = y(acc) - y(acc + v);
                  acc += v;
                  return <rect key={k} x={L + i * bw + bw * 0.2} y={y0} width={bw * 0.6} height={Math.max(0, h - 2)} rx={3} fill={KIND[k].color} className="paint" />;
                })}
                <text x={L + i * bw + bw / 2} y={194} textAnchor="middle">{new Date(`${d.day}T00:00:00`).toLocaleDateString("en-IN", { day: "2-digit", month: "short" })}</text>
              </g>
            );
          })}
        </svg>
      </div>
      {node}
    </Panel>
  );
}

function ResolveChart({ o }: { o: Operations }) {
  const { bind, node } = useTip();
  const rows = o.resolve_times.slice(0, 12);
  const dup = new Set(rows.map((r) => r.failure_type).filter((t, i, a) => a.indexOf(t) !== i));
  const label = (r: (typeof rows)[number]) => (dup.has(r.failure_type) ? `${r.failure_type} (${KIND[r.kind].label.toLowerCase()})` : r.failure_type);
  const max = Math.max(1, ...rows.map((r) => r.median_minutes));
  const L = 230, R = 510, rowH = 24, H = rows.length * rowH + 30;
  const x = (v: number) => L + ((R - L) * v) / max;
  return (
    <Panel title="Minutes to resolve" sub="median per failure type · resolved incidents only">
      <div className="chartbox">
        <Keys />
        {rows.length === 0 ? <div className="empty">Nothing resolved in this window.</div> : (
          <svg viewBox={`0 0 560 ${H}`} className="chart" role="img" aria-label="Median minutes to resolve, by failure type">
            {rows.map((r, i) => (
              <g key={`${r.failure_type}-${r.kind}`} className="mark"
                {...bind([[`${r.median_minutes} min`], [r.failure_type], [plain(r.failure_type)], [`${KIND[r.kind].label} · ${r.incidents} incident${r.incidents === 1 ? "" : "s"}`, KIND[r.kind].color]])}>
                <rect x={0} y={i * rowH} width={560} height={rowH} className="hit" />
                <text x={L - 8} y={i * rowH + 16} textAnchor="end" className="ink">{label(r)}</text>
                <rect x={L} y={i * rowH + 5} width={Math.max(2, x(r.median_minutes) - L)} height={rowH - 10} rx={3} fill={KIND[r.kind].color} className="paint" />
                <text x={x(r.median_minutes) + 5} y={i * rowH + 16} className="val">{r.median_minutes}</text>
              </g>
            ))}
            <text x={R} y={H - 6} textAnchor="end">minutes</text>
          </svg>
        )}
      </div>
      {node}
    </Panel>
  );
}

function niceStep(max: number) {
  for (const s of [1, 2, 5, 10, 20, 50, 100]) if (max / s <= 5) return s;
  return 200;
}

function PolicyCard({ o }: { o: Operations }) {
  const p = o.shell.policy;
  return (
    <Panel title="What the engine may do right now" sub="from policy/policy.yml — changed only by a reviewed pull request">
      <dl className="kv pad">
        <dt>Policy version</dt><dd className="mono">{p.version}</dd>
        <dt>Kill switch</dt><dd>{p.kill_switch ? <StatusPill tone="crit">ON — recommend only</StatusPill> : <StatusPill tone="ok">off — safe fixes run</StatusPill>}</dd>
        <dt>Attempts per incident</dt><dd>{p.attempt_cap ?? "—"}, then a person</dd>
        <dt>AI agents in decisions</dt><dd>{p.agents_enabled ? "advisory (engine validates every answer)" : "off — rules only"}</dd>
        <dt>Never automatic</dt><dd>full refresh, deletes, wrong clinical values, patient identity changes</dd>
      </dl>
    </Panel>
  );
}

// ── incidents + drawer ─────────────────────────────────────────────────────
function IncidentList({ items, onOpen }: { items: OpsIncident[]; onOpen: (id: string) => void }) {
  return (
    <Panel title="Recent incidents" sub="click one to replay it" foot="Plain words come from the failure type; the full journal is on the incident page.">
      {items.length === 0 ? <div className="empty">No incidents in this window.</div> : (
        <table>
          <thead><tr><th>Incident</th><th>What happened</th><th>Handled</th><th>Status</th><th className="n">Minutes</th><th>Opened</th></tr></thead>
          <tbody>
            {items.slice(0, 25).map((i) => (
              <tr key={i.incident_id} className="rowlink" tabIndex={0} onClick={() => onOpen(i.incident_id)}
                onKeyDown={(e) => { if (e.key === "Enter") onOpen(i.incident_id); }}>
                <td className="mono">{i.incident_id}</td>
                <td><span className="mono">{i.failure_type}</span>{plain(i.failure_type) && <div className="muted">{plain(i.failure_type)} · {i.dataset ?? "—"} · {fmt.day(i.business_date)}</div>}</td>
                <td><span className="kdot" style={{ background: KIND[i.kind].color }} />{KIND[i.kind].label}</td>
                <td><StatusPill tone={incidentTone(i.status)}>{i.status}</StatusPill></td>
                <td className="n">{i.minutes_open}</td>
                <td>{fmt.dateTime(i.opened_at)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </Panel>
  );
}

function Drawer({ id, onClose }: { id: string; onClose: () => void }) {
  const q = useIncident(id);
  const d = q.data;
  const [shown, setShown] = useState<number>(Infinity);
  const timer = useRef<number | null>(null);
  const ref = useRef<HTMLDivElement>(null);
  useEffect(() => {
    const k = (e: KeyboardEvent) => { if (e.key === "Escape") onClose(); };
    window.addEventListener("keydown", k);
    ref.current?.focus();
    return () => { window.removeEventListener("keydown", k); if (timer.current) window.clearInterval(timer.current); };
  }, [onClose]);
  const replay = () => {
    if (!d) return;
    if (timer.current) window.clearInterval(timer.current);
    setShown(1);
    timer.current = window.setInterval(() => {
      setShown((n) => {
        if (n >= d.events.length) { if (timer.current) window.clearInterval(timer.current); return n; }
        return n + 1;
      });
    }, 1400);
  };
  const events = d ? d.events.slice(0, shown) : [];
  const decision: IncidentEvent | undefined = useMemo(() => [...events].reverse().find((e) => e.gates), [events]);
  const evidence = decision?.evidence ?? [...events].reverse().find((e) => e.evidence != null)?.evidence;
  return (
    <div className="scrim" onClick={onClose}>
      <aside className="drawer" role="dialog" aria-modal="true" aria-label={`Incident ${id}`} tabIndex={-1} ref={ref}
        onClick={(e) => e.stopPropagation()}>
        <div className="dh">
          <div><div className="mono muted">{id}</div><h2>{d?.summary.failure_type ?? "…"}</h2>
            {d && <div className="ink2">{plain(d.summary.failure_type)}{plain(d.summary.failure_type) ? " · " : ""}{d.summary.dataset ?? "—"} · {fmt.day(d.summary.business_date)}</div>}
          </div>
          <span className="sp" />
          <button className="btn small" onClick={onClose} aria-label="Close">Close</button>
        </div>
        {q.error && <ErrorBox error={q.error} />}
        {q.isLoading && <Loading what="the incident" />}
        {d && (
          <div className="db">
            <div className="row">
              <StatusPill tone={incidentTone(d.summary.status)}>{d.summary.status}</StatusPill>
              <span className="ink2">attempt {d.summary.attempt} · {d.summary.minutes_open} min · rule <span className="mono">{d.summary.rule_id ?? "—"}</span></span>
              <span className="sp" />
              <button className="btn small pri" onClick={replay}>▶ Replay</button>
              <Link className="btn small" to={`/story?incident=${id}`}>On the map</Link>
              <Link className="btn small" to={`/incidents/${id}`}>Full page</Link>
            </div>
            <h3>Journal</h3>
            <ol className="journal">
              {events.map((e, i) => (
                <li key={i} className={i === events.length - 1 && shown !== Infinity ? "now" : ""}>
                  <span className="mono muted">{fmt.time(e.at)}</span>
                  <StatusPill tone={incidentTone(e.status)}>{e.status}</StatusPill>
                  <span>{e.note ?? ""}{e.action ? <span className="mono muted"> · {e.action}</span> : null}</span>
                </li>
              ))}
            </ol>
            <h3>Five gates</h3>
            {decision?.gates ? (
              <ol className="gates5">
                {decision.gates.map((g) => (
                  <li key={g.gate} className={g.pass ? "pass" : "stop"}>
                    <span className="n">{g.gate}</span>
                    <div><b>{g.question}</b><div className="ink2">{g.answer}</div></div>
                  </li>
                ))}
              </ol>
            ) : <p className="muted">{shown === Infinity ? "No decision recorded." : "…"}</p>}
            <h3>Evidence <span className="muted">(counts only)</span></h3>
            <Evidence value={evidence} />
          </div>
        )}
      </aside>
    </div>
  );
}

function Evidence({ value }: { value: unknown }) {
  if (value == null) return <p className="muted">—</p>;
  if (typeof value !== "object") return <p>{String(value)}</p>;
  return (
    <dl className="kv">
      {Object.entries(value as Record<string, unknown>).map(([k, v]) => (
        <Fragment key={k}><dt>{k.replace(/_/g, " ")}</dt><dd className="mono">{typeof v === "object" ? JSON.stringify(v) : String(v ?? "—")}</dd></Fragment>
      ))}
    </dl>
  );
}

// ── agent calls ────────────────────────────────────────────────────────────
const callTone = (c: AgentCall): Tone =>
  c.validation === "accepted" ? "ok" : c.reason?.startsWith("timeout") || c.outcome === "timeout" ? "held" : "crit";

function AgentCalls({ calls: all }: { calls: AgentCall[] }) {
  const [evals, setEvals] = useState(false);
  const isEval = (c: AgentCall) => (c.incident_id ?? "").startsWith("EVAL-");
  const calls = evals ? all : all.filter((c) => !isEval(c));
  const hidden = all.length - calls.length;
  return (
    <Panel title="AI agent calls" sub="every call is recorded — accepted, rejected or timed out — with the reason"
      right={all.some(isEval) ? <button className="btn small" onClick={() => setEvals((v) => !v)}>
        {evals ? "Hide evaluation runs" : `Show evaluation runs (${hidden})`}</button> : undefined}
      foot="Agents advise; the engine validates every answer against the fixed action list and the policy. Request and answer texts are not shown here.">
      {calls.length === 0 ? <div className="empty">No agent calls yet.</div> : (
        <table>
          <thead><tr><th>When</th><th>Agent</th><th>Model</th><th>Result</th><th>Reason / proposal</th><th className="n">ms</th><th>Incident</th></tr></thead>
          <tbody>
            {calls.map((c, i) => (
              <tr key={c.call_id ?? i}>
                <td>{fmt.dateTime(c.at)}</td>
                <td>{c.agent_id ?? "—"} <span className="muted">· {c.provider ?? "—"}</span></td>
                <td className="mono">{c.model ?? "—"}</td>
                <td><StatusPill tone={callTone(c)}>{c.validation ?? c.outcome ?? "—"}</StatusPill></td>
                <td className="ink2">{c.reason ?? (c.proposed_action ? `proposed ${c.proposed_action}` : c.outcome ?? "")}</td>
                <td className="n">{c.latency_ms ?? "—"}</td>
                <td className="mono">{isEval(c) ? <span className="muted">eval · {c.incident_id}</span> : c.incident_id ?? "—"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </Panel>
  );
}

// ── Ask the pipeline (read-only assistant) ─────────────────────────────────
const SUGGEST = [
  "Is anything wrong with today's reports?",
  "Which incidents needed a person this week?",
  "Why was the claims report held most recently?",
  "What happened to the last approval card?",
];

function Assistant({ available }: { available: boolean }) {
  const ask = useAsk();
  const [q, setQ] = useState("");
  const [log, setLog] = useState<{ q: string; a: string; cited: string[]; status: string }[]>([]);
  const send = (text: string) => {
    const t = text.trim();
    if (t.length < 3 || ask.isPending) return;
    ask.mutate(t, {
      onSuccess: (r) => setLog((l) => [{ q: t, a: r.answer, cited: r.cited, status: r.status }, ...l].slice(0, 6)),
      onError: (e) => setLog((l) => [{ q: t, a: e instanceof Error ? e.message : String(e), cited: [], status: "error" }, ...l]),
    });
    setQ("");
  };
  return (
    <Panel title="Ask the pipeline" sub="read-only · answers from the pipeline's own records · every question is recorded"
      foot="The assistant cannot approve, re-run or change anything. It only sees report states, incidents, cards and counts — never patient data — and Model Armor screens each question and answer.">
      <div className="ask">
        {!available && <p className="muted">The assistant is switched off. Turn it on with <span className="mono">enabled: true</span> for the assistant in agents/registry.yml (see the runbook).</p>}
        <form onSubmit={(e) => { e.preventDefault(); send(q); }} className="askrow">
          <label htmlFor="ask-q" className="sr">Question</label>
          <input id="ask-q" value={q} onChange={(e) => setQ(e.target.value)} maxLength={500} disabled={!available}
            placeholder="e.g. Why is Daily claims held?" />
          <button className="btn pri" disabled={!available || ask.isPending || q.trim().length < 3}>{ask.isPending ? "Thinking…" : "Ask"}</button>
        </form>
        <div className="suggest">
          {SUGGEST.map((s) => <button key={s} className="btn small" disabled={!available || ask.isPending} onClick={() => send(s)}>{s}</button>)}
        </div>
        {log.map((m, i) => (
          <div key={i} className={`qa ${m.status}`}>
            <div className="qq">{m.q}</div>
            <div className="aa">{m.a}</div>
            {m.cited.length > 0 && <div className="cites">Sources: {m.cited.map((c) => c.startsWith("INC-")
              ? <Link key={c} to={`/incidents/${c}`} className="mono">{c}</Link> : <Link key={c} to="/agents" className="mono">{c}</Link>)}</div>}
            {m.status === "refused" && <div className="muted">Not answered — recorded with the reason.</div>}
          </div>
        ))}
      </div>
    </Panel>
  );
}
