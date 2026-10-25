# Self-Reconciling Data Pipeline — POC

> **Bad data is never published, and well-understood failures heal themselves and are re-verified.**

A proof of concept that wraps a dbt + Airflow pipeline on Google Cloud in a control loop —
**detect → hold → decide → fix → verify** — and proves it on synthetic healthcare data
(EHR, lab and billing systems).

![Status](https://img.shields.io/badge/phase-1%20of%206%20built-2e8b57)
![Cloud](https://img.shields.io/badge/Google%20Cloud-asia--south1-4285F4)
![Airflow](https://img.shields.io/badge/Airflow-3.3.2-017CEE)
![dbt](https://img.shields.io/badge/dbt-BigQuery-FF694B)
![Data](https://img.shields.io/badge/data-100%25%20synthetic-7b8794)

---

## Contents

1. [Why this project exists](#1-why-this-project-exists)
2. [What it does](#2-what-it-does)
3. [Architecture](#3-architecture)
4. [Progress tracker](#4-progress-tracker)
5. [Quick start](#5-quick-start)
6. [Step by step: every phase](#6-step-by-step-every-phase)
7. [Test cases and demo](#7-test-cases-and-demo)
8. [Repository layout](#8-repository-layout)
9. [Costs and clean-up](#9-costs-and-clean-up)
10. [Keeping this README up to date](#10-keeping-this-readme-up-to-date)

📘 **Full operational detail lives in the runbook:** [`docs/RUNBOOK.md`](docs/RUNBOOK.md)

---

## 1. Why this project exists

Today's pipelines prove that they **ran**, not that the data is **right**.

- A nightly load can finish green while 800 hospital encounters are missing. The first person to
  notice is a ward manager looking at a wrong bed-occupancy report.
- Recovery is manual: L1 support searches logs and the runbook for 30 minutes, then escalates to
  L2 (data owner) and L3 (engineers).
- When an upstream system goes down, every dataset fails separately and on-call gets twenty alerts
  for one problem.

**The promise to report users:** you may see slightly old figures, but never wrong ones — and
delayed data is always marked as delayed.

## 2. What it does

| Step | What happens | Built in |
|---|---|---|
| **Detect** | Every extract writes a *receipt* (row count, distinct IDs, null amounts, ₹ total) from the same database snapshot; the warehouse is checked against it | Phase 1–2 |
| **Hold** | Data is built in an `audit` dataset; only verified days are cloned to `analytics`, which reports read | Phase 3 |
| **Decide** | A deterministic engine runs five gates and picks one safe action — or escalates | Phase 4 |
| **Fix and verify** | Safe fixes run automatically (L1); meaning-changing fixes need owner approval in Slack (L2); code problems go to engineers with evidence (L3). An incident closes only after re-verification | Phase 4–5 |
| **Advise** | AI agents explain unknown failures and suggest a catalog action — they never act | Phase 5 |
| **Group** | A source outage becomes one incident, not twenty, while other sources keep publishing | Phase 6 |

**Key ideas, in one line each**

- **Control totals** — three numbers, not one: 50 missing + 50 duplicated rows still give a matching count, but distinct IDs and ₹ totals catch it.
- **Immutable archive** — every extract is kept write-once in Cloud Storage, so any day can be replayed weeks later.
- **Idempotent loads** — delete the day, insert, write the receipt and log the batch in one transaction; retries never double data.
- **Write-Audit-Publish** — build, check, and only then publish.
- **Expected differences** — archived history, merged patients and late records are recognised and logged, not "fixed".
- **Catalog actions** — automation can only run a short list of safe fixes; `full_refresh`, `rollback` and `source_delete` are never automatic.
- **One identity per component** — IAM enforces the design: ingest *cannot* write `analytics`, even if the code is wrong.
- **Kill switch** — one setting turns every automatic action into a recommendation.

## 3. Architecture

### High level

![High-level architecture](docs/images/architecture-high-level.png)

Sources are synthetic Postgres databases. Airflow extracts each table together with its receipt,
archives it and loads BigQuery `raw`. dbt builds `audit` and tests it against the receipts. The
publication gate clones only verified days into `analytics`. Failures flow through Pub/Sub to the
decision engine, which fixes, asks for approval or escalates. Green boxes are built today; amber
boxes are planned.

### Low level

![Low-level design](docs/images/architecture-low-level.png)

Every component, the service account it runs as, the datasets it may write, the queues it uses
and the phase that builds it. The IAM table on the left is what Terraform enforces.

### Decision engine flow

![Decision flow](docs/images/decision-flow.png)

| Gate | Question | Example answer |
|---|---|---|
| 1 | Did the checks run? | 42 of 42 tests ran; control totals present |
| 2 | Can we trust them? | Replica lag 12 s; no outage group open |
| 3 | Is the difference expected? | Gap 8% > 0.1% tolerance → not expected |
| 4 | Which rule matches? | All 800 missing rows in one hour → `partial_extract` |
| 5 | Do guardrails allow a fix? | Attempt 1 of 2; not never-auto; kill switch off → auto |

### Technology

| Layer | Choice |
|---|---|
| Orchestration | Airflow 3.3.2 in Docker on the operator's Mac (Cloud Composer later) |
| Transformation and tests | dbt on BigQuery, run per model through Astronomer Cosmos |
| Warehouse | BigQuery: `raw`, `audit`, `analytics`, `quarantine`, `ops`, `evidence_restricted` |
| Archive | Cloud Storage with a retention policy (Bucket Lock opt-in) |
| Control plane | Cloud Run (console, engine, executor, publish job), Pub/Sub with dead letters, Cloud SQL Postgres (private IP) |
| Security | One service account per component, impersonation (no key files), Cloud KMS, Secret Manager |
| AI (Phase 5) | Diagnosis and Communication agents behind Model Armor; off until India residency, BAA and no-training are confirmed |
| Console | FastAPI backend + React frontend, one new screen per phase |
| Infrastructure | Terraform; GitHub Actions CI |

## 4. Progress tracker

Update this table and the badge at the top each time a phase's exit gate passes (see
[§10](#10-keeping-this-readme-up-to-date)).

| Phase | Name | Status | Exit gate | Console screen |
|---|---|---|---|---|
| 0 | Foundations | ✅ Built | ⬜ verified | Environment |
| 1 | Ingest, control totals and raw archive | ✅ Built | ⬜ verified | Sources and ingestion |
| 2 | dbt models and reconciliation checks | ⏳ Planned | ⬜ | Reconciliation |
| 3 | Publication gate and Write-Audit-Publish | ⏳ Planned | ⬜ | Publication ledger |
| 4 | Decision engine and recovery executor | ⏳ Planned | ⬜ | Incidents |
| 5 | AI agents, approvals and notifications | ⏳ Planned | ⬜ | Agents and approvals |
| 6 | Outage breaker, hardening and demo | ⏳ Planned | ⬜ | Operations |

Status legend: ⏳ Planned · 🚧 In progress · ✅ Built · ⬜ exit gate not yet checked · ☑️ exit gate passed

**Success criteria for the whole POC**

| Criterion | Target | Proven in | Result |
|---|---|---|---|
| Publication safety | 0 publishes of data that failed a blocking check | All phases | — |
| Automatic recovery | Partial extract healed and verified in under 30 minutes | P4, demo 1 | — |
| Blocked publication | Unit change and value mismatch held and escalated | P2–P4, demo 2 | — |
| No false alarms | Archived history and merged patients not flagged | P4–P5, demo 3 | — |
| Shared outage | One grouped incident, other sources still publish | P6, demo 4 | — |
| Explainability | Every decision shows gates, policy version and evidence | P4 | — |
| Agent safety | Malformed, out-of-catalog or injected output rejected | P5 | — |

## 5. Quick start

### Prerequisites (macOS)

| Tool | Install |
|---|---|
| Google Cloud account (free trial works) | https://cloud.google.com/free |
| `gcloud` CLI | `brew install --cask google-cloud-sdk` |
| Terraform ≥ 1.6 | `brew tap hashicorp/tap && brew install hashicorp/tap/terraform` |
| Docker Desktop, 4 GB+ memory | https://www.docker.com/products/docker-desktop |
| Python 3.11+ and Node 22 | `brew install python@3.12 node@22`, then `make dev-setup` |

### First run (about 30 minutes, mostly waiting for Cloud SQL)

```bash
# 1. Create a project and link billing
gcloud auth login
PROJECT=reconcile-poc-$RANDOM
gcloud projects create $PROJECT --name="Reconcile POC"
gcloud billing projects link $PROJECT --billing-account=XXXXXX-XXXXXX-XXXXXX
gcloud config set project $PROJECT

# 2. Application Default Credentials (used to impersonate service accounts; no key files)
gcloud auth application-default login
gcloud auth application-default set-quota-project $PROJECT

# 3. Build the cloud environment
cp infra/terraform.tfvars.example infra/terraform.tfvars   # set project_id and operator_email
make tf-init && make tf-plan        # ~120 resources to add, nothing to destroy
make tf-apply                       # Cloud SQL takes 10–15 minutes
make env                            # writes .env from Terraform outputs

# 4. Start Airflow + synthetic sources, generate data
make up                             # Airflow at http://localhost:8080
make generate DATE=2026-10-13 DAYS=3

# 5. Ingest a day and check it
make trigger DATE=2026-10-13
make verify  DATE=2026-10-13        # Phase 1 exit-gate checks G1–G5

# 6. Open the console
make deploy-console && make console # http://localhost:8081
```

## 6. Step by step: every phase

Each phase depends on the evidence the previous one produces. A phase is **done** only when its
exit gate passes. The full detail for each phase is in the runbook (links below).

### Phase 0 — Foundations ✅
**What you build:** Terraform for the whole environment (6 BigQuery datasets, archive bucket, VPC and
private Cloud SQL, Pub/Sub topics with dead letters, Artifact Registry, one service account per
component), the deterministic synthetic data generator, `policy/policy.yml` with the kill switch,
the FastAPI + React console shell, and CI.

**Why:** if identities and networking are improvised later, safety rules become promises instead of
facts. Building them first means every later phase inherits them.

**Do it:**
```bash
make tf-apply && make env
make generate DATE=2026-10-13 DAYS=3
make deploy-console && make console      # → Environment screen
```

**You achieve:** a secured, empty platform; the console's Environment screen shows every resource
checked live and the identity matrix read from real IAM permissions.

**Exit gate:** ⬜ destroy + apply rebuilds from scratch · ⬜ generator loads a business date
(`make status`) · ⬜ `/api/health` returns `ok` · ⬜ CI is green.
→ [Runbook §8](docs/RUNBOOK.md#8-phase-0--foundations)

### Phase 1 — Ingest, control totals and raw archive ✅
**What you build:** Airflow DAGs `ingest_ehr`, `ingest_lis`, `ingest_billing` (01:00 IST, load
yesterday) and `replay_batch`. Each run: probe → extract rows **and** totals in one repeatable-read
transaction → archive Parquet + manifest → load in one BigQuery transaction → publish
`ingest.run_complete`.

**Why:** reconciliation needs a trustworthy receipt, recovery needs an untouched copy to replay, and
retries must never double data.

**Do it:**
```bash
make trigger DATE=2026-10-13
make verify  DATE=2026-10-13
make replay  DATASET=encounters DATE=2026-10-13
```
Try a failure: trigger `ingest_ehr` with
`{"business_date": "2026-10-13", "datasets": ["encounters"], "inject": ["drop_hour"]}` — the job is
green, but the Sources screen shows rows landed in red against the control total.

**You achieve:** every load is idempotent, has its own receipt and an immutable original.

**Exit gate:** ⬜ G1 one receipt per batch · ⬜ G2 re-runs give identical counts · ⬜ G3 archive
replays into raw · ⬜ G4 ingest cannot write analytics · ⬜ G5 archive cannot be overwritten.
→ [Runbook §9](docs/RUNBOOK.md#9-phase-1--ingest-control-totals-and-raw-archive)

### Phase 2 — dbt models and reconciliation checks ⏳
**What you build:** staging models (incremental, de-duplicated, 3-day look-back) and marts in
`audit`; singular tests `recon_<dataset>` against control totals; `control_totals_present`; an
expected-test-count assertion; value-level reconciliation of lab results with KMS-backed
deterministic encryption; `meta.policy_outcome` on every test; Cosmos in Airflow; the
Reconciliation screen.

**Why:** Phase 1 records the receipt; Phase 2 actually checks the warehouse against it.

**Do it (planned):** `make dbt-build DATE=…` · `make dbt-value-recon DATE=…` · `make inject SCENARIO=offsetting_errors DATE=…`

**You achieve:** every injected failure produces the right failing test; no clinical value ever
lands in `evidence_restricted`.
→ [Runbook §10](docs/RUNBOOK.md#10-phase-2--dbt-models-and-reconciliation-checks)

### Phase 3 — Publication gate and Write-Audit-Publish ⏳
**What you build:** a publication state per dataset and date (`PENDING`, `PUBLISHED`, `HELD`,
`PUBLISHED_WITH_CAVEAT`), a dependency map from `manifest.json`, the publish job on Cloud Run
(`CREATE OR REPLACE TABLE … CLONE`, snapshot first for rollback), and the Publication ledger.

**Why:** checks are useless if bad data still reaches reports.

**Do it (planned):** `make publish DATE=…` · `make rollback DATASET=… DATE=…`

**You achieve:** a held dataset and everything that depends on it stay on the last verified day,
marked delayed.
→ [Runbook §11](docs/RUNBOOK.md#11-phase-3--publication-gate-and-write-audit-publish)

### Phase 4 — Decision engine and recovery executor ⏳
**What you build:** engine and executor on Cloud Run fed by Pub/Sub; atomic run-ID claims and
partition locks in Cloud SQL; the five gates; severity (P1/P2/P3) and response mode; the action
catalog; journal before and after every action; the Incidents screen.

**Why:** replaces the manual L1 loop with decisions that are explainable, replayable and auditable.

**Do it (planned):** `make deploy-engine` · `make inject SCENARIO=partial_extract DATE=…`

**You achieve:** a missing hour is replayed, rebuilt, re-verified and published in about 12 minutes
without a human.
→ [Runbook §12](docs/RUNBOOK.md#12-phase-4--decision-engine-and-recovery-executor)

### Phase 5 — AI agents, approvals and notifications ⏳
**What you build:** Diagnosis and Communication agents (strict JSON, cited evidence, confidence ≥
0.80, behind Model Armor), masked views, a Slack app for L2 approvals, a 30-case golden set that
blocks regressions, and the Agents and approvals screen.

**Precondition:** written confirmation of India residency, BAA cover and no training on prompts.
Until then `agents.enabled: false`.

**Do it (planned):** `make agents-eval` · `make deploy-agents`

**You achieve:** unknown failures get a cited diagnosis; owners approve meaning-changing fixes; no
agent can act on its own.
→ [Runbook §13](docs/RUNBOOK.md#13-phase-5--ai-agents-approvals-and-notifications)

### Phase 6 — Outage breaker, hardening and demo ⏳
**What you build:** the outage breaker (3 datasets or 50% of a source failing within 30 minutes),
the full failure-injection suite, fallback alerting, quotas, an audit-log sink, the evaluation gate in
CI and the Operations screen with KPIs.

**Do it (planned):** `make inject-suite` · `make demo SCENARIO=1..4`

**You achieve:** one grouped incident for a lab-system outage while billing and EHR keep
publishing; zero bad publishes across the whole suite; a KPI baseline against today's manual
process.
→ [Runbook §14](docs/RUNBOOK.md#14-phase-6--outage-breaker-hardening-and-demo)

## 7. Test cases and demo

21 test cases (T1–T21), each a failure injector plus an assertion, form the failure-injection
suite. Status today: T1 automated; T2, T3, T4, T9 partly in place; the rest planned.
See [Runbook §15](docs/RUNBOOK.md#15-test-cases-t1t21).

**Demo — four scenarios, live in the console**

| # | Scenario | Proves |
|---|---|---|
| 1 | Automatic recovery (missing hour) | Known failures heal and are re-verified within 30 minutes |
| 2 | Blocked publication (glucose unit change) | Bad data is never published |
| 3 | Expected difference (archived rows, merged patient) | No false alarms; owners approve meaning-changing fixes |
| 4 | Shared outage (lab system down) | Correlated failures handled as one |

## 8. Repository layout

```
infra/             Terraform: datasets, bucket, VPC, Cloud SQL, Pub/Sub, IAM (P0) + later services
data-gen/          Synthetic EHR / LIS / Billing / MPI sources and failure injectors
ingest/            Extract, archive, load, replay, verify + Airflow DAGs (P1)
dbt/reconcile/     dbt project: sources, models, reconciliation and value tests (P2+)
policy/            policy.yml: rules, action catalog, never-auto list, kill switch
engine/            Decision engine, executor, outage breaker (P4, P6)
agents/            Agent registry, Diagnosis and Communication agents, golden set (P5)
api/               FastAPI backend (one router per phase)
web/               React console (one screen per phase)
scripts/           write_env, deploy_console, check_policy, gen_dbt_sources
docs/
  RUNBOOK.md       Full runbook: every phase, operations, troubleshooting
  images/          Architecture PNGs used in this README
  diagrams/        Sources for the PNGs (draw.py, decision-flow.dot)
.github/workflows/ CI: lint, tests, policy check, dbt parse, Terraform validate, DAG import, console build
```

## 9. Costs and clean-up

The only continuous cost is Cloud SQL (about $10 / month); everything else scales to zero or stays
in the free tier at POC volume, and every BigQuery query is capped at 1 GiB billed.

```bash
gcloud sql instances patch reconcile-state --activation-policy=NEVER    # pause Cloud SQL
gcloud sql instances patch reconcile-state --activation-policy=ALWAYS   # resume
make down                                                               # stop local containers
make tf-destroy                                                         # delete the environment
```

All patient data is synthetic (seed 2026). Never load real patient data into this POC.

## 10. Keeping this README up to date

When a phase's exit gate passes:

1. **Progress tracker (§4):** change the phase's status to ✅ Built and its exit gate to ☑️; fill in
   any success-criteria results it proves.
2. **Badge:** edit `phase-1%20of%206%20built` at the top of this file.
3. **Phase section (§6):** tick the exit-gate boxes, remove "(planned)" from commands that now work.
4. **Diagrams:** add the phase number to `BUILT_PHASES` in `docs/diagrams/draw.py`, then regenerate —
   its boxes turn from amber to green:
   ```bash
   pip install matplotlib
   python3 docs/diagrams/draw.py
   dot -Tpng -Gdpi=150 docs/diagrams/decision-flow.dot -o docs/images/decision-flow.png
   ```
5. **Runbook:** update the phase table at the top of `docs/RUNBOOK.md`, the phase heading
   (⏳ Planned → ✅ Built), the test-case status column, and add a line to its change log.

---

*Source documents: Architecture design (v4), Understanding & Design, Phased POC Implementation Plan (16 Oct 2026).*
