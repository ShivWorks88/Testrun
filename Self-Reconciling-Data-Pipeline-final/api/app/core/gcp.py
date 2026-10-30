from __future__ import annotations

from functools import lru_cache

from .config import get_settings

SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]


@lru_cache
def credentials():
    import google.auth
    from google.auth import impersonated_credentials

    s = get_settings()
    base, _ = google.auth.default(scopes=SCOPES)
    if s.impersonate_service_account:
        return impersonated_credentials.Credentials(source_credentials=base,
                                                    target_principal=s.impersonate_service_account,
                                                    target_scopes=SCOPES, lifetime=3600)
    return base


@lru_cache
def bigquery():
    from google.cloud import bigquery as bq
    s = get_settings()
    return bq.Client(project=s.gcp_project, credentials=credentials(), location=s.gcp_region)


@lru_cache
def storage():
    from google.cloud import storage as gcs
    return gcs.Client(project=get_settings().gcp_project, credentials=credentials())


@lru_cache
def publisher():
    from google.cloud import pubsub_v1
    return pubsub_v1.PublisherClient(credentials=credentials())


def runs_as() -> str:
    s = get_settings()
    if s.impersonate_service_account:
        return s.impersonate_service_account
    creds = credentials()
    return getattr(creds, "service_account_email", None) or "application default credentials"
