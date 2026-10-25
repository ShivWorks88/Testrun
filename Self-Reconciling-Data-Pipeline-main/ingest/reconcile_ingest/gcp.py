"""Google credentials without key files: Application Default Credentials, optionally
impersonating the component's service account (local Airflow, operator scripts)."""
from __future__ import annotations

from functools import lru_cache

SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]


@lru_cache(maxsize=8)
def credentials(impersonate: str | None):
    import google.auth
    from google.auth import impersonated_credentials

    base, _ = google.auth.default(scopes=SCOPES)
    if not impersonate:
        return base
    return impersonated_credentials.Credentials(
        source_credentials=base, target_principal=impersonate, target_scopes=SCOPES, lifetime=3600)


def bigquery_client(project: str, impersonate: str | None, location: str):
    from google.cloud import bigquery
    return bigquery.Client(project=project, credentials=credentials(impersonate), location=location)


def storage_client(project: str, impersonate: str | None):
    from google.cloud import storage
    return storage.Client(project=project, credentials=credentials(impersonate))


def publisher_client(impersonate: str | None):
    from google.cloud import pubsub_v1
    return pubsub_v1.PublisherClient(credentials=credentials(impersonate))
