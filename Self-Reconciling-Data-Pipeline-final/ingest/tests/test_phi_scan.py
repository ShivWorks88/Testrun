"""T26 — the patterns the patient-data scan uses (no BigQuery)."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

pytest.importorskip("google.cloud.bigquery")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from reconcile_ingest.phi_scan import find_record_ids, find_sensitive_values  # noqa: E402


def test_record_ids_are_found():
    for text in ('{"patient": "P-000123"}', "encounter E-20261014-00042 failed",
                 "result R-20261014-000001", "claim C-20261013-00007", "new patient P-20261014-0001"):
        assert find_record_ids(text), text


def test_pipeline_ids_are_not_patient_data():
    for text in ("INC-20261014-0ff41d", "APR-20261014-dd94e0", "b-20261014-claims-01",
                 '{"raw_rows": 10012, "receipt_rows": 10012, "missing_keys": 0}',
                 "check not_null_stg_claims_claim_id failed (3)", "2026-10-14"):
        assert not find_record_ids(text), text


def test_sensitive_keys_only_with_a_value():
    assert find_sensitive_values('{"given_name": "Asha"}') == ["given_name"]
    assert find_sensitive_values("result_value=4.1") == ["result_value"]
    assert not find_sensitive_values("unique_stg_claims_claim_id failed")
    assert not find_sensitive_values('{"check": "not_null_stg_encounters_patient_id", "failures": 2}')
