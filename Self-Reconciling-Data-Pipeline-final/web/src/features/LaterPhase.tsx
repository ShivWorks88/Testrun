import { PageHead } from "../components/ui";

export default function LaterPhase({ phase, title, what }: { phase: number; title: string; what: string }) {
  return (
    <main className="content">
      <PageHead title={title} lede={what} phase={`Phase ${phase}`} />
      <div className="panel"><div className="empty">This screen is built in Phase {phase}. Phases 0 and 1 are live.</div></div>
    </main>
  );
}
