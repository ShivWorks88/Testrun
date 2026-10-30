"""Draw the README architecture diagrams with a fixed, hand-placed layout.

Usage:  python3 docs/diagrams/draw.py          (writes docs/images/*.png)

To mark a phase as done, add its number to BUILT_PHASES below and re-run:
boxes tagged with that phase turn green.
"""

from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib import font_manager
from matplotlib.patches import FancyBboxPatch

plt.switch_backend("Agg")  # render to files only, never open a window

BUILT_PHASES = {0, 1}  # <-- update as phases are completed

OUT = Path(__file__).resolve().parents[1] / "images"
for f in font_manager.findSystemFonts():
    if "Inter" in f:
        font_manager.fontManager.addfont(f)
plt.rcParams["font.family"] = ["Inter", "DejaVu Sans"]

INK, MUTED, LINE = "#1f2933", "#52606d", "#5b6472"
BUILT = ("#dff3e6", "#2e8b57")
PLANNED = ("#fdf0d5", "#c58a00")
NEUTRAL = ("#eef1f5", "#7b8794")
WHITE = ("#ffffff", "#7b8794")


def colours(phase):
    if phase is None:
        return NEUTRAL
    phases = phase if isinstance(phase, (list, tuple, set)) else [phase]
    return BUILT if all(p in BUILT_PHASES for p in phases) else PLANNED


class Canvas:
    def __init__(self, w, h, scale=0.115):
        self.fig = plt.figure(figsize=(w * scale, h * scale))
        self.ax = self.fig.add_axes([0, 0, 1, 1])
        self.ax.set_xlim(0, w)
        self.ax.set_ylim(0, h)
        self.ax.axis("off")

    def text(
        self,
        x,
        y,
        s,
        size=9,
        weight="normal",
        color=INK,
        ha="left",
        va="center",
        style="normal",
    ):
        self.ax.text(
            x,
            y,
            s,
            fontsize=size,
            fontweight=weight,
            color=color,
            ha=ha,
            va=va,
            fontstyle=style,
            zorder=6,
        )

    def group(
        self,
        x0,
        y0,
        x1,
        y1,
        title,
        edge="#2b6cb0",
        fill="#f7faff",
        dashed=False,
        size=10.5,
    ):
        p = FancyBboxPatch(
            (x0, y0),
            x1 - x0,
            y1 - y0,
            boxstyle="round,pad=0,rounding_size=1.6",
            linewidth=1.3,
            edgecolor=edge,
            facecolor=fill,
            linestyle=(0, (5, 3)) if dashed else "solid",
            zorder=1,
        )
        self.ax.add_patch(p)
        self.text(x0 + 1.4, y1 - 1.8, title, size=size, weight="bold", color=edge)

    def box(
        self,
        x0,
        y0,
        x1,
        y1,
        title,
        lines=(),
        phase=None,
        pal=None,
        tsize=9.5,
        lsize=7.6,
        shape="round",
        tag=True,
    ):
        fill, edge = pal or colours(phase)
        style = "round,pad=0,rounding_size=1.2" if shape == "round" else "square,pad=0"
        self.ax.add_patch(
            FancyBboxPatch(
                (x0, y0),
                x1 - x0,
                y1 - y0,
                boxstyle=style,
                linewidth=1.2,
                edgecolor=edge,
                facecolor=fill,
                zorder=3,
            )
        )
        if shape == "db":  # cylinder hint: an ellipse-ish top line
            self.ax.plot([x0 + 0.6, x1 - 0.6], [y1 - 1.1, y1 - 1.1], color=edge, lw=0.9, zorder=4)
        n = len(lines)
        step = 2.05 * lsize / 7.6
        top = (y0 + y1) / 2 + (n * step) / 2
        label = title
        if tag and phase is not None:
            ps = phase if isinstance(phase, (list, tuple)) else [phase]
            label += "  [" + "/".join(f"P{p}" for p in ps) + "]"
        self.text((x0 + x1) / 2, top, label, size=tsize, weight="bold", ha="center")
        for i, line in enumerate(lines):
            colr, sty = MUTED, "normal"
            if line.startswith("!"):
                line, colr = line[1:], "#b03a48"
            if line.startswith("~"):
                line, sty = line[1:], "italic"
            self.text(
                (x0 + x1) / 2,
                top - step * (i + 1) - 0.25,
                line,
                size=lsize,
                color=colr,
                ha="center",
                style=sty,
            )

    def arrow(
        self,
        pts,
        label=None,
        lpos=None,
        style="solid",
        color=LINE,
        both=False,
        head=True,
        lsize=7.4,
        lha="left",
    ):
        ls = {"solid": "-", "dashed": (0, (5, 3)), "dotted": (0, (1.5, 2))}[style]
        xs, ys = zip(*pts, strict=True)
        self.ax.plot(
            xs,
            ys,
            color=color,
            lw=1.3,
            linestyle=ls,
            zorder=2,
            solid_capstyle="round",
        )
        if head:
            self.ax.annotate(
                "",
                xy=pts[-1],
                xytext=pts[-2],
                arrowprops={
                    "arrowstyle": "-|>",
                    "color": color,
                    "lw": 1.3,
                    "mutation_scale": 12,
                    "shrinkA": 0,
                    "shrinkB": 0,
                },
                zorder=5,
            )
        if both:
            self.ax.annotate(
                "",
                xy=pts[0],
                xytext=pts[1],
                arrowprops={
                    "arrowstyle": "-|>",
                    "color": color,
                    "lw": 1.3,
                    "mutation_scale": 12,
                    "shrinkA": 0,
                    "shrinkB": 0,
                },
                zorder=5,
            )
        if label:
            x, y = lpos
            self.ax.text(
                x,
                y,
                label,
                fontsize=lsize,
                color="#333333",
                ha=lha,
                va="center",
                zorder=7,
                bbox={
                    "boxstyle": "round,pad=0.18",
                    "fc": "white",
                    "ec": "none",
                    "alpha": 0.92,
                },
            )

    def legend(self, x, y):
        self.box(x, y, x + 15, y + 3.6, "Built", pal=BUILT, tsize=8, tag=False)
        self.box(x + 16.5, y, x + 31.5, y + 3.6, "Planned", pal=PLANNED, tsize=8, tag=False)
        self.text(
            x + 33,
            y + 1.8,
            "solid = flow    dashed = replay / hold    dotted = reads / state",
            size=7.6,
            color=MUTED,
        )

    def save(self, name):
        OUT.mkdir(parents=True, exist_ok=True)
        self.fig.savefig(OUT / name, dpi=170, facecolor="white")
        plt.close(self.fig)


# ---------------------------------------------------------------------------
# High-level architecture
# ---------------------------------------------------------------------------
def high_level():
    c = Canvas(170, 100)
    c.text(
        3,
        96.5,
        "Self-Reconciling Data Pipeline — high-level architecture",
        size=17,
        weight="bold",
    )
    c.text(
        3,
        93.0,
        "detect → hold → decide → fix → verify     ·     "
        "Google Cloud asia-south1 (Mumbai)     ·     synthetic healthcare data",
        size=9.5,
        color=MUTED,
    )
    c.legend(96, 91.2)

    # Sources
    c.group(2, 50, 25, 88, "Source systems", edge="#7b8794", fill="#fafbfc", dashed=True)
    c.text(3.4, 84.0, "synthetic Postgres (Docker)", size=7.4, color=MUTED)
    for i, (t, desc) in enumerate(
        [
            ("EHR", "encounters · patients · diagnoses"),
            ("LIS", "lab orders · panels · results"),
            ("Billing", "claims · payments · adjustments"),
            ("MPI", "patient merges"),
        ]
    ):
        y = 74.5 - i * 7.0
        c.box(4, y, 23, y + 5.6, t, [desc], pal=NEUTRAL, tsize=9, lsize=7)

    # Airflow
    c.box(
        30,
        61,
        50,
        74,
        "Airflow 3",
        [
            "Docker on the operator Mac",
            "probe → extract rows + totals",
            "(same snapshot)",
            "→ archive → load → announce",
        ],
        phase=1,
    )
    c.arrow([(25, 67.5), (30, 67.5)], "read-only", (25.4, 69.6), lsize=7)

    # Data plane
    c.group(55, 50, 148, 88, "Data plane  (BigQuery + Cloud Storage)")
    c.box(
        58,
        74,
        78,
        84,
        "Cloud Storage",
        ["immutable raw archive", "Parquet + manifest (SHA-256)"],
        phase=1,
    )
    c.box(
        58,
        56,
        78,
        67,
        "BigQuery raw",
        ["one partition per business date", "+ ops.control_totals (receipt)"],
        phase=1,
    )
    c.box(
        84,
        61,
        104,
        72,
        "BigQuery audit",
        ["dbt staging + marts", "reconciliation + value tests"],
        phase=2,
    )
    c.box(
        109,
        61,
        125,
        72,
        "Publication gate",
        ["Write-Audit-Publish", "verified days only"],
        phase=3,
    )
    c.box(
        130,
        61,
        146,
        72,
        "analytics",
        ["what reports read", "written by sa-publish only"],
        phase=3,
    )
    c.box(
        84,
        52,
        125,
        57.5,
        "quarantine · evidence_restricted",
        ["moved rows · mismatch keys and field names, never values"],
        phase=2,
        lsize=7,
    )

    c.arrow(
        [(50, 71), (54, 71), (54, 79), (58, 79)],
        "archive (write-once)",
        (40.5, 80.8),
        lsize=7,
    )
    c.arrow([(50, 64), (58, 64)], "load (idempotent)", (50.4, 61.8), lsize=7)
    c.arrow([(68, 74), (68, 67)], "replay", (68.8, 70.5), style="dashed")
    c.arrow([(78, 66.5), (84, 66.5)], "dbt", (78.8, 68.4))
    c.arrow([(104, 66.5), (109, 66.5)])
    c.arrow([(125, 66.5), (130, 66.5)], "clone", (125.6, 68.6), lsize=6.8)
    c.arrow([(94, 61), (94, 57.5)], style="dashed")

    # Reports
    c.box(
        150,
        61,
        167,
        72,
        "Reports",
        ["dashboards read", "analytics only"],
        pal=WHITE,
        tag=False,
    )
    c.arrow([(146, 66.5), (150, 66.5)])
    c.arrow(
        [(117, 72), (117, 78), (158.5, 78), (158.5, 72)],
        "HELD: reports keep the last verified day, marked “Data delayed”",
        (119, 79.8),
        style="dashed",
        lsize=7.2,
    )

    # Control plane
    c.group(
        55,
        21,
        148,
        46,
        "Control plane  (Cloud Run · Pub/Sub · Cloud SQL)",
        edge="#7b3fb3",
        fill="#fbf8ff",
    )
    c.box(
        58,
        30,
        78,
        40,
        "Pub/Sub",
        ["run-complete · engine-requests", "notifications (+ dead letters)"],
        phase=0,
    )
    c.box(
        84,
        30,
        104,
        40,
        "Decision engine",
        ["5 gates · policy.yml", "kill switch · severity"],
        phase=4,
    )
    c.box(109, 33.5, 125, 41.5, "Recovery executor", ["catalog actions only"], phase=4)
    c.box(109, 23, 125, 31, "Outage breaker", ["one incident per outage"], phase=6)
    c.box(
        130,
        30,
        146,
        40,
        "Cloud SQL",
        ["incidents · journals", "publication state · locks"],
        phase=0,
        shape="db",
    )

    c.arrow([(47, 61), (47, 35), (58, 35)], "run_complete", (47.6, 52), lsize=7)
    c.arrow(
        [(84, 63.5), (81, 63.5), (81, 46.5), (70, 46.5), (70, 40)],
        "test results",
        (71, 48.2),
        lsize=7,
    )
    c.arrow([(78, 35), (84, 35)])
    c.arrow([(104, 38), (109, 38)], "fix", (104.6, 39.6), lsize=6.8)
    c.arrow(
        [(109, 27), (106.5, 27), (106.5, 33), (104, 33)],
        "hold",
        (105.2, 25.2),
        lsize=6.8,
    )
    c.arrow(
        [(125, 39.5), (127.5, 39.5), (127.5, 59.2), (100, 59.2), (100, 61)],
        "replay · rebuild · re-verify",
        (113, 48.5),
        style="dashed",
        lsize=7,
    )
    c.arrow([(125, 37.5), (130, 37.5)], style="dotted", head=False)

    # People and advisors
    c.group(55, 2, 166, 17.5, "People and advisors", edge="#b03a48", fill="#fff8f8")
    c.box(
        58,
        4,
        78,
        13.5,
        "AI agents (advisory)",
        ["diagnosis · communication", "never act · masked data only"],
        phase=5,
        tsize=9,
    )
    c.box(84, 4, 104, 13.5, "Slack approvals", ["L2 data owner"], phase=5)
    c.box(
        109,
        4,
        125,
        13.5,
        "L3 engineers",
        ["code / schema fixes", "with evidence"],
        pal=WHITE,
        tag=False,
    )
    c.box(
        130,
        4,
        164,
        13.5,
        "Ops console (FastAPI + React)",
        ["Environment · Sources screens live", "one new screen per phase"],
        phase=0,
    )
    c.arrow(
        [(88, 30), (88, 24), (68, 24), (68, 13.5)],
        "unknown case",
        (69, 21.8),
        both=True,
        lsize=7,
    )
    c.arrow([(94, 30), (94, 13.5)], "approve", (94.8, 21.8), both=True, lsize=7)
    c.arrow(
        [(102, 30), (102, 19.5), (117, 19.5), (117, 13.5)],
        "escalate",
        (106.5, 21.2),
        lsize=7,
    )
    c.arrow(
        [(140, 30), (140, 13.5)],
        "state",
        (140.8, 21.8),
        style="dotted",
        both=True,
        lsize=7,
    )

    # Left-hand promise panel
    c.box(2, 4, 44, 44, "", [], pal=WHITE, tag=False)
    c.text(4.5, 41.2, "The promise", size=10.5, weight="bold")
    notes = [
        ("1  Detect", "every load is checked against a receipt"),
        ("", "counted from the same source snapshot"),
        ("2  Hold", "nothing reaches reports until checks pass"),
        ("3  Decide", "deterministic engine picks one safe action"),
        ("4  Fix and verify", "auto-fix (L1) · approve (L2) · engineer (L3)"),
        ("", "an incident closes only after re-verification"),
        ("5  Group", "a source outage is one incident, not twenty"),
    ]
    y = 37.3
    for head, body in notes:
        if head:
            c.text(4.5, y, head, size=8.6, weight="bold")
        c.text(17, y, body, size=7.6, color=MUTED)
        y -= 3.3
    c.text(
        4.5,
        11.5,
        "Users see slightly old figures, never wrong ones.",
        size=8.4,
        weight="bold",
        color="#2e8b57",
    )
    c.text(4.5, 8.0, "Delayed data is always marked as delayed.", size=8.4, color="#2e8b57")
    c.save("architecture-high-level.png")


# ---------------------------------------------------------------------------
# Low-level design
# ---------------------------------------------------------------------------
def low_level():
    c = Canvas(214, 170, scale=0.112)
    c.text(
        3,
        166.5,
        "Self-Reconciling Data Pipeline — low-level design",
        size=18,
        weight="bold",
    )
    c.text(
        3,
        162.8,
        "[Pn] = phase that builds it   ·   sa-* = the service account it runs as   ·   "
        "all GCP resources built by Terraform (infra/)",
        size=9.5,
        color=MUTED,
    )
    c.legend(132, 161.2)

    # ---- Lane A: operator Mac ------------------------------------------------
    c.group(
        2,
        124,
        211,
        158,
        "Operator Mac — Docker Compose",
        edge="#52606d",
        fill="#f6f7f9",
    )
    c.group(
        4,
        126,
        61,
        155.5,
        "sources-postgres  (synthetic, seed 2026)",
        edge="#7b8794",
        fill="#ffffff",
        dashed=True,
        size=9,
    )
    c.box(
        6,
        138,
        33,
        151.6,
        "Source schemas",
        [
            "ehr: encounters · patients",
            "diagnoses · archive_index",
            "lis: lab_orders · panels · results",
            "billing: claims · payments · adj.",
            "mpi: patient_merges",
        ],
        phase=0,
        lsize=7,
    )
    c.box(
        35,
        138,
        59,
        151.6,
        "data-gen/",
        [
            "deterministic generator",
            "edge cases built in",
            "UTC vs facility day",
            "late, reversed, merged",
        ],
        phase=0,
        lsize=7,
    )
    c.box(
        6,
        127.5,
        59,
        136.5,
        "Failure injectors",
        [
            "drop_hour · duplicate_rows · missing_control_totals",
            "~P2/P6: offsetting · unit_change · truncation · interface_down",
        ],
        phase=[1, 6],
        lsize=7,
    )

    c.group(
        64,
        126,
        209,
        155.5,
        "Airflow 3.3.2  (impersonates service accounts through ADC; no key files)",
        edge="#2e8b57",
        fill="#ffffff",
        dashed=True,
        size=9,
    )
    c.box(
        66,
        129,
        127,
        150,
        "ingest_ehr · ingest_lis · ingest_billing",
        [
            "daily 01:00 IST, loads yesterday's facility day  ·  sa-ingest",
            "1  probe_source → ops.source_health",
            "2  extract rows + totals in one repeatable-read txn",
            "3  archive Parquet + manifest (write-once)",
            "4  load: one BigQuery txn (delete day, insert,",
            "    write totals, log batch)",
            "5  announce ingest.run_complete",
        ],
        phase=1,
        lsize=7.4,
    )
    c.box(
        131,
        135,
        152,
        150,
        "replay_batch",
        [
            "archive object → raw",
            "same load transaction",
            "used by reextract_",
            "from_raw (P4)",
        ],
        phase=1,
        lsize=7,
    )
    c.box(
        157,
        129,
        207,
        150,
        "dbt build (Cosmos)",
        [
            "one Airflow task per model + its tests  ·  sa-dbt",
            "staging: incremental, de-dup, 3-day look-back",
            "recon_<dataset> vs control totals",
            "control_totals_present · expected test count",
            "value_recon_lab_results as sa-valuerecon",
            "meta.policy_outcome on every test",
        ],
        phase=2,
        lsize=7.2,
    )
    c.arrow([(61, 145.5), (66, 145.5)], "RR snapshot", (60.6, 147.6), lsize=6.8)

    # ---- Lane B: storage and warehouse ---------------------------------------
    c.group(64, 80, 211, 120, "Storage and warehouse")
    c.box(
        66,
        104,
        94,
        116,
        "GCS archive",
        [
            "raw/<dataset>/load_date=<d>/",
            "<batch>.parquet + manifest",
            "retention 7 d · Lock opt-in",
        ],
        phase=1,
        lsize=7,
    )
    c.box(
        100,
        104,
        126,
        116,
        "raw",
        ["exact source copy", "1 partition / business date", "writer: sa-ingest"],
        phase=1,
        shape="db",
        lsize=7,
    )
    c.box(
        132,
        104,
        152,
        116,
        "ops",
        ["control_totals · batches", "source_health", "writer: sa-ingest, engine"],
        phase=1,
        shape="db",
        lsize=7,
    )
    c.box(
        157,
        104,
        207,
        116,
        "audit",
        [
            "stg_encounters · stg_claims · stg_lab_results",
            "dim_encounters · fct_daily_claims · fct_bed_occupancy",
            "writer: sa-dbt, sa-executor",
        ],
        phase=2,
        shape="db",
        lsize=7,
    )
    c.box(
        66,
        83,
        94,
        96,
        "masked views",
        ["what agents may read", "no direct table access"],
        phase=5,
        shape="db",
        lsize=7,
    )
    c.box(
        100,
        83,
        126,
        96,
        "quarantine",
        ["rows moved out of marts", "moved, never copied"],
        phase=[2, 4],
        shape="db",
        lsize=7,
    )
    c.box(
        132,
        83,
        152,
        96,
        "Cloud KMS",
        ["wrapped key for", "deterministic encryption", "decrypt: sa-valuerecon"],
        phase=2,
        lsize=7,
    )
    c.box(
        157,
        83,
        180,
        96,
        "evidence_restricted",
        ["record_key · field", "key_version", "never values"],
        phase=2,
        shape="db",
        lsize=7,
        tsize=8.6,
    )
    c.box(
        185,
        83,
        207,
        96,
        "analytics",
        ["verified days only", "CLONE of audit", "writer: sa-publish ONLY"],
        phase=3,
        shape="db",
        lsize=7,
    )

    c.arrow(
        [(72, 129), (72, 125), (80, 125), (80, 116)],
        "write-once",
        (80.8, 121.6),
        lsize=6.8,
    )
    c.arrow([(113, 129), (113, 116)], "load txn", (113.8, 121.6), lsize=6.8)
    c.arrow(
        [(122, 129), (122, 125.5), (142, 125.5), (142, 116)],
        "totals · batch",
        (134, 121.6),
        lsize=6.8,
    )
    c.arrow([(94, 108), (100, 108)], "replay", (93.6, 110.2), style="dashed", lsize=6.8)
    c.arrow(
        [(152, 110), (154.5, 110), (154.5, 133), (157, 133)],
        "receipts",
        (155.2, 121.6),
        lsize=6.8,
    )
    c.arrow([(182, 129), (182, 116)], "builds", (182.8, 121.6), lsize=6.8)
    c.arrow(
        [(160, 104), (160, 99.5), (113, 99.5), (113, 96)],
        "quarantine_rows",
        (131, 101.2),
        style="dashed",
        lsize=6.8,
    )
    c.arrow([(170, 104), (170, 96)], "value recon", (170.8, 100.6), lsize=6.8)
    c.arrow([(152, 89.5), (157, 89.5)], style="dotted")

    # ---- Left column: identity, secrets, build --------------------------------
    c.group(
        2,
        30,
        61,
        120,
        "IAM — one identity per component  [P0]",
        edge="#2b6cb0",
        fill="#f7faff",
    )
    rows = [
        ("sa-ingest", "raw · ops (3 tables) · archive (create)"),
        ("sa-dbt", "audit · quarantine"),
        ("sa-valuerecon", "evidence_restricted · KMS decrypt"),
        ("sa-publish", "analytics  (the only writer)"),
        ("sa-engine", "ops · Cloud SQL · Pub/Sub"),
        ("sa-executor", "audit · quarantine · trigger DAGs"),
        ("sa-agents", "read masked views only"),
        ("sa-console", "Cloud SQL · engine-requests"),
    ]
    c.text(4.5, 113.5, "identity", size=8, weight="bold", color=MUTED)
    c.text(23, 113.5, "may write", size=8, weight="bold", color=MUTED)
    for i, (sa, can) in enumerate(rows):
        y = 109.5 - i * 4.6
        c.ax.add_patch(
            FancyBboxPatch(
                (4, y - 2),
                55,
                4,
                boxstyle="round,pad=0,rounding_size=0.6",
                fc="#ffffff" if i % 2 else "#eef4fb",
                ec="none",
                zorder=2,
            )
        )
        c.text(5, y, sa, size=8, weight="bold", color="#2b6cb0")
        c.text(23, y, can, size=7.6, color=INK)
    c.text(4.5, 69.5, "Operators impersonate for one hour;", size=7.6, color=MUTED)
    c.text(4.5, 66.6, "no service-account key files exist.", size=7.6, color=MUTED)
    c.text(
        4.5,
        63.0,
        "G4 proves it: ingest → analytics is denied.",
        size=7.6,
        weight="bold",
        color="#2e8b57",
    )
    c.box(
        4,
        46,
        59,
        59,
        "Secret Manager · Artifact Registry · Cloud Build",
        [
            "DB password (P0) · Slack signing secret (P5)",
            "container images for every Cloud Run service",
            "VPC + private services access: Cloud SQL has no public IP",
        ],
        phase=0,
        lsize=7,
        tsize=8.4,
    )
    c.box(
        4,
        32,
        59,
        43,
        "Cloud Monitoring · Logging",
        [
            "uptime checks · fallback alerts · budgets",
            "per-SA BigQuery quotas",
            "audit-log sink to a locked bucket",
        ],
        phase=6,
        lsize=7,
        tsize=8.6,
    )

    # ---- Lane C: control plane --------------------------------------------------
    c.group(
        64,
        28,
        211,
        76,
        "Control plane  (Cloud Run services · Pub/Sub · Cloud SQL)",
        edge="#7b3fb3",
        fill="#fbf8ff",
    )
    c.box(
        66,
        55,
        112,
        72,
        "console  ·  sa-console",
        [
            "FastAPI /api/* serves the React UI",
            "health · environment · sources · batches",
            "~+ reconciliation · publication · incidents",
            "~+ approvals · agents · metrics · outages",
            "opened via gcloud run services proxy",
        ],
        phase=0,
        lsize=7,
        tag=True,
    )
    c.box(
        66,
        31,
        112,
        51,
        "Cloud SQL  reconcile-state",
        [
            "request_journal · results",
            "publication_state · incidents",
            "run_claims (lease) · partition_locks",
            "action_journal · approvals",
            "zonal db-f1-micro · private IP",
        ],
        phase=0,
        shape="db",
        lsize=7,
    )
    c.text(118, 74.0, "Pub/Sub  [P0]", size=8, weight="bold", color="#7b3fb3")
    c.box(
        118,
        63,
        150,
        71,
        "run-complete",
        ["ingest + dbt results · DLQ"],
        phase=0,
        lsize=7,
        tag=False,
    )
    c.box(
        118,
        51,
        150,
        59,
        "engine-requests",
        ["console replay / retry"],
        phase=0,
        lsize=7,
        tag=False,
    )
    c.box(
        118,
        35,
        150,
        43,
        "notifications",
        ["alerts · approvals · escalations"],
        phase=0,
        lsize=7,
        tag=False,
    )
    c.box(
        156,
        31,
        182,
        72,
        "decision engine",
        [
            "sa-engine",
            "",
            "1  run-ID claim",
            "   (atomic, lease)",
            "2  evidence contract",
            "3  outage breaker [P6]",
            "4  five gates",
            "5  severity P1 / P2 / P3",
            "6  mode: auto ·",
            "   approve · engineer",
            "",
            "attempt cap 2",
            "kill switch",
            "policy.yml version",
        ],
        phase=4,
        lsize=7.2,
    )
    c.box(
        187,
        57,
        209,
        72,
        "publish job",
        [
            "sa-publish",
            "re-read state + breakers",
            "snapshot, then CLONE",
            "in dependency order",
        ],
        phase=3,
        lsize=7,
    )
    c.box(
        187,
        31,
        209,
        52,
        "executor",
        [
            "sa-executor",
            "reextract_from_raw",
            "rebuild_partition",
            "lookback_rebuild",
            "link_unknown_member",
            "quarantine_rows*",
            "!never: full_refresh,",
            "!rollback, source_delete",
        ],
        phase=4,
        lsize=7,
    )

    # Engine wiring
    c.arrow([(127, 131), (129, 131), (129, 71)], "announce", (129.8, 78.5), lsize=6.8)
    c.arrow([(150, 67), (156, 67)])
    c.arrow([(112, 63), (115, 63), (115, 55), (118, 55)], "retry", (112.6, 61), lsize=6.8)
    c.arrow([(150, 55), (156, 55)])
    c.arrow([(156, 39), (150, 39)])
    c.arrow([(182, 64.5), (187, 64.5)], "gate", (181.5, 66.6), lsize=6.8)
    c.arrow([(182, 42), (187, 42)], "1 action", (180.5, 44.2), lsize=6.8)
    c.arrow(
        [(156, 33), (112, 33)],
        "claims · locks · journal (before and after)",
        (117, 31.2),
        style="dotted",
        both=True,
        lsize=6.8,
    )
    c.arrow([(89, 55), (89, 51)], style="dotted", both=True)
    c.arrow([(198, 72), (198, 83)], "clone", (198.8, 77.5), lsize=6.8)
    c.arrow(
        [(209, 46), (210.2, 46), (210.2, 110), (207, 110)],
        "rebuild + re-verify",
        (193.5, 100),
        lsize=6.8,
    )

    # ---- Lane D: people, AI, external -----------------------------------------
    c.group(
        64,
        2,
        211,
        24,
        "Agents, approvals and code  (agents stay off until India residency, BAA and no-training are confirmed)",
        edge="#b03a48",
        fill="#fff8f8",
        size=9.5,
    )
    c.box(
        66,
        4,
        100,
        18.5,
        "Slack app",
        [
            "approve / reject buttons (L2)",
            "callbacks verified with",
            "the signing secret, logged",
        ],
        phase=5,
        lsize=7,
    )
    c.box(
        118,
        4,
        150,
        18.5,
        "Communication agent",
        ["text with counts and", "references only, no PHI"],
        phase=5,
        lsize=7,
        tsize=9,
    )
    c.box(
        156,
        4,
        182,
        18.5,
        "Diagnosis agent",
        [
            "strict JSON: cited facts,",
            "hypotheses, one catalog",
            "action, confidence ≥ 0.80",
        ],
        phase=5,
        lsize=7,
        tsize=9,
    )
    c.box(
        187,
        4,
        209,
        18.5,
        "Model Armor",
        ["prompt-injection", "screening", "golden set: 30 cases"],
        phase=5,
        lsize=7,
        tsize=9,
    )
    c.arrow([(134, 35), (134, 18.5)], "draft", (134.8, 26.5), lsize=6.8)
    c.arrow([(118, 11), (100, 11)])
    c.arrow([(169, 31), (169, 18.5)], "unknown case", (169.8, 26.5), both=True, lsize=6.8)
    c.arrow([(182, 11), (187, 11)], style="dotted", head=False)
    c.arrow([(76, 18.5), (76, 31)], "approval", (76.8, 26.5), lsize=6.8)

    c.box(
        2,
        2,
        61,
        26,
        "GitHub repository",
        [
            "policy/policy.yml — rules, catalog, never-auto, kill switch",
            "agents/registry.yml — versions, enabled: false",
            "CI: lint · tests · policy check · dbt parse",
            "terraform validate · DAG import · console build",
            "~P6: golden-set evaluation gate",
        ],
        pal=NEUTRAL,
        lsize=7,
        tag=False,
    )
    c.save("architecture-low-level.png")


if __name__ == "__main__":
    high_level()
    low_level()
    print("wrote", OUT)
