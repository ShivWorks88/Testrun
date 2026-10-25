import { NavLink, Navigate, Route, Routes } from "react-router-dom";
import { useBatches, useEnvironment, useMe } from "../api/client";
import EnvironmentPage from "../features/environment/EnvironmentPage";
import SourcesPage from "../features/sources/SourcesPage";
import ReconciliationPage from "../features/reconciliation/ReconciliationPage";
import PublicationPage from "../features/publication/PublicationPage";
import { IncidentDetailPage, IncidentsPage } from "../features/incidents/IncidentsPage";
import LaterPhase from "../features/LaterPhase";
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
  ] },
  { group: "Platform", items: [{ to: "/environment", label: "Environment", phase: 0 }] },
];
const BUILT = new Set([0, 1, 2, 3, 4]);

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
      <span className="chip" title={me.data?.roles.join(", ")}>
        <b>{me.data?.email ?? "…"}</b> {me.data?.roles.includes("engineer") ? "engineer" : "viewer"}
      </span>
      <span className="av" aria-hidden="true">{initials}</span>
    </header>
  );
}

export default function App() {
  return (
    <div className="app">
      <Rail />
      <div className="main">
        <TopBar />
        <Routes>
          <Route path="/" element={<Navigate to="/sources" replace />} />
          <Route path="/environment" element={<EnvironmentPage />} />
          <Route path="/sources" element={<SourcesPage />} />
          <Route path="/reconciliation" element={<ReconciliationPage />} />
          <Route path="/publication" element={<PublicationPage />} />
          <Route path="/incidents" element={<IncidentsPage />} />
          <Route path="/incidents/:id" element={<IncidentDetailPage />} />
          <Route path="/agents" element={<LaterPhase phase={5} title="Agents and approvals" what="Agent proposals with cited facts, engine validation and L2 approval cards." />} />
          <Route path="/operations" element={<LaterPhase phase={6} title="Operations" what="Outage breaker, KPI tiles and demo scenario results." />} />
          <Route path="*" element={<Navigate to="/sources" replace />} />
        </Routes>
      </div>
    </div>
  );
}
