"""Environment screen: live checks against the project, and the identity matrix computed from
the real dataset ACLs (so the screen shows what IAM enforces, not what we intended)."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import yaml

from ..core import gcp
from ..core.config import Settings
from ..schemas.models import IdentityRow, PolicyInfo, Resource

DATASETS = ["raw", "audit", "analytics", "quarantine", "ops", "evidence_restricted"]
TOPICS = ["run-complete", "engine-requests", "notifications"]
COMPONENT_LABELS = {"ingest": "Ingest", "dbt": "dbt build", "valuerecon": "Value recon", "publish": "Publish",
                    "engine": "Engine", "executor": "Executor", "agents": "AI agents", "api": "Console API"}
WRITE_ROLES = {"WRITER", "OWNER", "roles/bigquery.dataEditor", "roles/bigquery.dataOwner", "roles/bigquery.admin"}
READ_ROLES = {"READER", "roles/bigquery.dataViewer"}


def load_policy(settings: Settings) -> PolicyInfo:
    try:
        with open(settings.policy_file) as f:
            p = yaml.safe_load(f)
        return PolicyInfo(version=str(p["version"]), kill_switch=bool(p["kill_switch"]),
                          agents_enabled=bool(p["agents"]["enabled"]),
                          residency_confirmed=bool(p["agents"]["residency_confirmed"]),
                          actions=len(p["actions"]), rules=sum(len(v or []) for v in p["rules"].values()))
    except Exception:  # noqa: BLE001
        return PolicyInfo(version="unreadable", kill_switch=True, agents_enabled=False,
                          residency_confirmed=False, actions=0, rules=0)


def _datasets(settings: Settings) -> tuple[Resource, dict]:
    client = gcp.bigquery()
    found, acl = [], {}
    for ds in DATASETS:
        try:
            d = client.get_dataset(f"{settings.gcp_project}.{ds}")
            found.append(ds)
            acl[ds] = d.access_entries
        except Exception:  # noqa: BLE001
            pass
    state = "ready" if len(found) == len(DATASETS) else ("missing" if not found else "warning")
    return Resource(name=f"{len(DATASETS)} warehouse datasets", service="BigQuery", state=state,
                    label=f"{len(found)} of {len(DATASETS)}" if state != "ready" else "Ready",
                    detail=", ".join(found) or "none found"), acl


def _bucket(settings: Settings) -> Resource:
    try:
        b = gcp.storage().get_bucket(settings.archive_bucket)
        days = (b.retention_period or 0) // 86400
        locked = bool(b.retention_policy_locked)
        return Resource(name="Raw archive bucket", service="Cloud Storage", state="ready",
                        label="Locked" if locked else "Retention set",
                        detail=f"gs://{b.name} · retention {days} d · {'Bucket Lock ON' if locked else 'not locked'}")
    except Exception as exc:  # noqa: BLE001
        return Resource(name="Raw archive bucket", service="Cloud Storage", state="error", label="Error",
                        detail=str(exc)[:160])


def _topics(settings: Settings) -> Resource:
    pub = gcp.publisher()
    ok = 0
    for t in TOPICS + [f"{t}-dlq" for t in TOPICS]:
        try:
            pub.get_topic(request={"topic": pub.topic_path(settings.gcp_project, t)})
            ok += 1
        except Exception:  # noqa: BLE001
            pass
    total = len(TOPICS) * 2
    return Resource(name="Event topics + dead-letter", service="Pub/Sub",
                    state="ready" if ok == total else ("missing" if ok == 0 else "warning"),
                    label="Ready" if ok == total else f"{ok} of {total}", detail=", ".join(TOPICS))


def _ops_table_writers(settings: Settings) -> set[str]:
    """Ingest has table-level write on three ops tables; read that from the table IAM policy."""
    try:
        policy = gcp.bigquery().get_iam_policy(f"{settings.gcp_project}.ops.control_totals")
        return {m.split(":", 1)[-1] for b in policy.bindings if b["role"] == "roles/bigquery.dataEditor"
                for m in b["members"]}
    except Exception:  # noqa: BLE001
        return set()


def identity_matrix(settings: Settings, acl: dict) -> list[IdentityRow]:
    table_writers = _ops_table_writers(settings)
    rows = []
    for comp, email in settings.component_accounts().items():
        access = {}
        for ds in DATASETS:
            level = "—"
            for e in acl.get(ds, []):
                if (e.entity_id or "").lower() != email.lower():
                    continue
                if e.role in WRITE_ROLES:
                    level = "Write"
                elif e.role in READ_ROLES and level != "Write":
                    level = "Read"
            if ds == "ops" and level == "—" and email in table_writers:
                level = "Write"
            access[ds] = level
        rows.append(IdentityRow(component=COMPONENT_LABELS.get(comp, comp), email=email, access=access))
    return rows


def resources(settings: Settings, db_state: tuple[bool, str], last_batch_at, policy: PolicyInfo):
    with ThreadPoolExecutor(max_workers=3) as pool:
        f_ds = pool.submit(_datasets, settings)
        f_b = pool.submit(_bucket, settings)
        f_t = pool.submit(_topics, settings)
        ds_res, acl = f_ds.result()
        out = [ds_res, f_b.result()]
        topics = f_t.result()

    ok, detail = db_state
    out.append(Resource(name="Ops state database", service="Cloud SQL",
                        state="ready" if ok else ("off" if detail == "not configured" else "error"),
                        label="Ready" if ok else ("Not configured" if detail == "not configured" else "Error"),
                        detail=detail))
    out.append(topics)
    out.append(Resource(name="Console + API", service="Cloud Run", state="ready", label="Ready",
                        detail=f"this service, as {gcp.runs_as()}"))
    out.append(Resource(name="Orchestrator", service="Airflow 3.x (local Docker)",
                        state="ready" if last_batch_at else "pending",
                        label="Running" if last_batch_at else "No runs yet",
                        detail=f"last batch {last_batch_at:%d %b %H:%M} UTC" if last_batch_at else
                        "start it with `make up`, then trigger ingest_ehr"))
    out.append(Resource(name="recon-digest key", service="Cloud KMS", state="pending", label="Phase 2",
                        detail="value-level reconciliation key, created in Phase 2"))
    out.append(Resource(name="2 advisory agents", service="Agent Platform", state="off",
                        label="Off: residency check" if not policy.residency_confirmed else "Off",
                        detail="enabled in Phase 5 once residency and BAA coverage are confirmed"))
    return out, acl
