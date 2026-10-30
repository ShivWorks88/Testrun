from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

REPO_ROOT = Path(__file__).resolve().parents[3]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=None, extra="ignore")

    env: str = "cloud"                       # cloud | local
    gcp_project: str = ""
    gcp_region: str = "asia-south1"
    archive_bucket: str = ""
    impersonate_service_account: str = ""   # local dev only; on Cloud Run the API runs as sa-api
    bq_max_bytes_billed: int = 1 << 30

    # Ops state (Cloud SQL in the cloud, the docker-compose Postgres locally)
    database_url: str = ""                   # full URL wins if set
    db_host: str = ""
    db_port: int = 5432
    db_name: str = "reconcile_ops"
    db_user: str = "reconcile_app"
    db_password: str = ""
    db_ssl: bool = True

    # Identity and roles
    local_user: str = "local-dev@localhost"
    role_map: str = ""                       # "a@x.com=viewer,engineer,data_owner;b@y.com=viewer"
    default_roles: str = "viewer"

    # Service accounts shown on the Environment screen
    sa_ingest: str = ""
    sa_dbt: str = ""
    sa_valuerecon: str = ""
    sa_publish: str = ""
    sa_engine: str = ""
    sa_executor: str = ""
    sa_agents: str = ""
    sa_api: str = ""

    policy_file: str = str(REPO_ROOT / "policy" / "policy.yml")
    registry_file: str = str(REPO_ROOT / "agents" / "registry.yml")   # Phase 6: the assistant's entry
    static_dir: str = str(REPO_ROOT / "web" / "dist")
    git_sha: str = "dev"
    built_at: str = ""

    @property
    def sqlalchemy_url(self) -> str | None:
        if self.database_url:
            return self.database_url
        if self.db_host:
            return (f"postgresql+asyncpg://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/"
                    f"{self.db_name}")
        return None

    def component_accounts(self) -> dict[str, str]:
        base = {"ingest": self.sa_ingest, "dbt": self.sa_dbt, "valuerecon": self.sa_valuerecon,
                "publish": self.sa_publish, "engine": self.sa_engine, "executor": self.sa_executor,
                "agents": self.sa_agents, "api": self.sa_api}
        if self.gcp_project:   # derive the Terraform names when not given explicitly
            for k, v in base.items():
                if not v:
                    base[k] = f"sa-{k}@{self.gcp_project}.iam.gserviceaccount.com"
        return base


@lru_cache
def get_settings() -> Settings:
    return Settings()
