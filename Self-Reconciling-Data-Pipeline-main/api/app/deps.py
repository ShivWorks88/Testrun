"""Dependencies the tests override (BigQuery repository, Pub/Sub publisher)."""
from __future__ import annotations

from fastapi import Depends

from .bq.repository import BigQueryOpsRepository, OpsRepository
from .core import gcp
from .core.config import Settings, get_settings


def get_repo(settings: Settings = Depends(get_settings)) -> OpsRepository:
    return BigQueryOpsRepository(gcp.bigquery(), settings)


def get_publisher():
    return gcp.publisher()
