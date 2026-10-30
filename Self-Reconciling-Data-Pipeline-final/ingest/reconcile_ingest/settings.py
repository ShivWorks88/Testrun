from __future__ import annotations

import os
from dataclasses import dataclass
from zoneinfo import ZoneInfo

EXTRACTOR_VERSION = "ingest-0.1.0"


@dataclass(frozen=True)
class Settings:
    project: str
    archive_bucket: str
    source_dsn: str
    impersonate: str | None
    max_bytes_billed: int
    allow_injections: bool
    facility_tz: ZoneInfo
    location: str

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            project=os.environ.get("GCP_PROJECT", ""),
            archive_bucket=os.environ.get("ARCHIVE_BUCKET", ""),
            source_dsn=os.environ.get("SOURCE_DSN", "postgresql://reconcile:reconcile@localhost:5433/source_replica"),
            impersonate=os.environ.get("IMPERSONATE_SERVICE_ACCOUNT") or None,
            max_bytes_billed=int(os.environ.get("BQ_MAX_BYTES_BILLED", str(1 << 30))),
            allow_injections=os.environ.get("ALLOW_INJECTIONS", "false").lower() in ("1", "true", "yes"),
            facility_tz=ZoneInfo(os.environ.get("FACILITY_TZ", "Asia/Kolkata")),
            location=os.environ.get("GCP_REGION", "asia-south1"),
        )

    def require_gcp(self) -> None:
        missing = [n for n, v in [("GCP_PROJECT", self.project), ("ARCHIVE_BUCKET", self.archive_bucket)] if not v]
        if missing:
            raise RuntimeError(f"Missing environment: {', '.join(missing)} (run `make env` after terraform apply)")
