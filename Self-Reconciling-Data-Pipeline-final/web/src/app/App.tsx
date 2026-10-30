import { NavLink, Navigate, Route, Routes } from "react-router-dom";
import { useBatches, useEnvironment, useMe } from "../api/client";
import EnvironmentPage from "../features/environment/EnvironmentPage";
import SourcesPage from "../features/sources/SourcesPage";
import ReconciliationPage from "../features/reconciliation/ReconciliationPage";
import PublicationPage from "../features/publication/PublicationPage";
import { IncidentDetailPage, IncidentsPage } from "../features/incidents/IncidentsPage";
import ApprovalsPage from "../features/agents/ApprovalsPage";
import OperationsPage from "../features/operations/OperationsPage";
import StoryPage from "../features/story/StoryPage";
import { useShell } from "../api/operations";
import { useBusinessDate } from "./BusinessDate";
import { fmt } from "../components/ui";

// One screen per POC phase. Later phases are visible but marked, so reviewers see the roadmap.
const NAV = [
  { group: "Pipeline", items: [
    { to: "/sources", label: "Sources and ingestion", phase: 1 },
    { to: "/reconciliation", label: "Reconciliation", phase: 2 },
    { to: "/publication", label: "Publication", phase: 3 },
  ] },
  { group: "Recovery", items: [
    { to: "/incidents", label: "Incidents", phase: 4 },
    { to: "/agents", label: "Agents and approvals", phase: 5 },
    { to: "/operations", label: "Operations", phase: 6 },
    { to: "/story", label: "Pipeline story", phase: 6 },
  ] },
  { group: "Platform", items: [{ to: "/environment", label: "Environment", phase: 0 }] },
];
const BUILT = new Set([0, 1, 2, 3, 4, 5, 6]); // Phase 6: Operations screen + read-only assistant

function Rail() {
  const env = useEnvironment();
  const p = env.data?.policy;
  return (
    <nav className="rail" aria-label="Console">
      <div className="brand">
        <div className="mark">
          <svg width="16" height="16" viewBox="0 0 16 16" aria-hidden="true">
            <path d="M3 8.5l3 3 7-7" stroke="#fff" strokeWidth="2.2" fill="none" strokeLinecap="round" strokeLinejoin="round" />
          </svg>
        </div>
        <div><b>Reconcile</b><span>Data trust console</span></div>
      </div>
      {NAV.map((g) => (
        <div key={g.group}>
          <div className="navgrp">{g.group}</div>
          {g.items.map((i) => (
            <NavLink key={i.to} to={i.to} className={({ isActive }) => `nav${isActive ? " active" : ""}${BUILT.has(i.phase) ? "" : " later"}`}>
              <i />{i.label}<span className="ph">P{i.phase}</span>
            </NavLink>
          ))}
        </div>
      ))}
      <div className="railfoot">
        Policy <b>{p?.version ?? "…"}</b><br />
        Kill switch <b>{p ? (p.kill_switch ? "ON" : "off") : "…"}</b> · agents <b>{p ? (p.agents_enabled ? "advisory" : "off") : "…"}</b>
      </div>
    </nav>
  );
}

function TopBar() {
  const me = useMe();
  const { date, setDate } = useBusinessDate();
  const batches = useBatches(date);
  const dates = batches.data?.available_dates ?? [];
  const env = useEnvironment();
  const initials = (me.data?.email ?? "?").slice(0, 2).toUpperCase();
  return (
    <header className="top">
      <span className="crumb">{env.data?.project ?? "…"} · <b>{env.data?.region ?? "asia-south1"}</b></span>
      <span className="sp" />
      <label className="chip">
        Business date
        <select aria-label="Business date" value={date ?? ""} onChange={(e) => setDate(e.target.value || null)}>
          <option value="">Latest{batches.data ? ` (${fmt.day(batches.data.business_date)})` : ""}</option>
          {dates.map((d) => <option key={d} value={d}>{fmt.day(d)}</option>)}
        </select>
      </label>
      <WaitingBadge />
      <span className="chip" title={me.data?.roles.join(", ")}>
        <b>{me.data?.email ?? "…"}</b> {me.data?.roles.includes("engineer") ? "engineer" : "viewer"}
      </span>
      <span className="av" aria-hidden="true">{initials}</span>
    </header>
  );
}

// Shown on every page while a decision is waiting: a card must never be missed because Slack was closed.
function WaitingBadge() {
  const s = useShell();
  const n = s.data?.pending_cards ?? 0;
  if (!n) return null;
  return (
    <NavLink to="/agents" className="chip waiting" title={s.data?.next_expiry ? `first one expires ${fmt.dateTime(s.data.next_expiry)}` : ""}>
      <b>{n}</b> card{n > 1 ? "s" : ""} waiting for you
    </NavLink>
  );
}

// Red bar across every page while an outage breaker is open (Phase 6 package 1).
function OutageBanner() {
  const s = useShell();
  const open = s.data?.open_breakers ?? [];
  if (!open.length) return null;
  return (
    <div className="outage" role="alert">
      {open.map((b) => (
        <div key={b.source_system}>
          <b>{b.source_system === "GLOBAL" ? "Several sources are down" : `${b.source_system} is down`}</b>
          {" "}since {fmt.dateTime(b.opened_at)}
          {b.failing_datasets.length ? ` · ${b.failing_datasets.join(", ")} held` : ""}
          {" · "}reports from it keep the last verified day; no fixes run until fresh data loads.
          {b.parent_incident_id && <> <NavLink to={`/incidents/${b.parent_incident_id}`}>Open the incident</NavLink></>}
        </div>
      ))}
    </div>
  );
}

export default function App() {
  return (
    <div className="app">
      <Rail />
      <div className="main">
        <TopBar />
        <OutageBanner />
        <Routes>
          <Route path="/" element={<Navigate to="/sources" replace />} />
          <Route path="/environment" element={<EnvironmentPage />} />
          <Route path="/sources" element={<SourcesPage />} />
          <Route path="/reconciliation" element={<ReconciliationPage />} />
          <Route path="/publication" element={<PublicationPage />} />
          <Route path="/incidents" element={<IncidentsPage />} />
          <Route path="/incidents/:id" element={<IncidentDetailPage />} />
          <Route path="/agents" element={<ApprovalsPage />} />
          <Route path="/operations" element={<OperationsPage />} />
          <Route path="/story" element={<StoryPage />} />
          <Route path="*" element={<Navigate to="/sources" replace />} />
        </Routes>
      </div>
    </div>
  );
}
