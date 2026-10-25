import type { ReactNode } from "react";

// The same five status words and colours on every screen (UX rule from the plan).
export type Tone = "ok" | "held" | "crit" | "info" | "cav" | "pend";

export function StatusPill({ tone, children }: { tone: Tone; children: ReactNode }) {
  return <span className={`st ${tone}`}>{children}</span>;
}

export const stateTone: Record<string, Tone> = {
  ready: "ok", warning: "held", missing: "crit", error: "crit", off: "pend", pending: "pend",
};

export function Panel(props: { title: string; sub?: ReactNode; right?: ReactNode; foot?: ReactNode; children: ReactNode }) {
  return (
    <section className="panel">
      <div className="ph">
        <h3>{props.title}</h3>
        {props.sub && <span className="sub">{props.sub}</span>}
        {props.right && <><span className="sp" />{props.right}</>}
      </div>
      {props.children}
      {props.foot && <div className="foot">{props.foot}</div>}
    </section>
  );
}

export function PageHead(props: { title: string; lede: string; phase: string; actions?: ReactNode }) {
  return (
    <div className="head">
      <div>
        <h1>{props.title}</h1>
        <p>{props.lede}</p>
      </div>
      <span className="phase">{props.phase}</span>
      <span className="sp" />
      {props.actions}
    </div>
  );
}

export function Loading({ what }: { what: string }) {
  return <div className="empty">Loading {what}…</div>;
}

export function ErrorBox({ error }: { error: unknown }) {
  const msg = error instanceof Error ? error.message : String(error);
  return (
    <div className="banner crit" role="alert">
      <b>Could not load.</b> <span>{msg}</span>
    </div>
  );
}

const inr = new Intl.NumberFormat("en-IN", { style: "currency", currency: "INR", minimumFractionDigits: 2 });
const num = new Intl.NumberFormat("en-IN");

export const fmt = {
  n: (v: number | null | undefined) => (v == null ? "—" : num.format(v)),
  inr: (v: string | number | null | undefined) => (v == null ? "—" : inr.format(Number(v))),
  time: (v: string | null | undefined) =>
    v ? new Date(v).toLocaleTimeString("en-IN", { hour: "2-digit", minute: "2-digit", timeZone: "Asia/Kolkata" }) : "—",
  dateTime: (v: string | null | undefined) =>
    v
      ? new Date(v).toLocaleString("en-IN", { day: "2-digit", month: "short", hour: "2-digit", minute: "2-digit", timeZone: "Asia/Kolkata" })
      : "—",
  day: (v: string | null | undefined) =>
    v ? new Date(`${v}T00:00:00`).toLocaleDateString("en-IN", { day: "2-digit", month: "short", year: "numeric" }) : "—",
  shortUri: (v: string | null | undefined) => (v ? `…/${v.split("/").slice(-1)[0]}` : "—"),
};
