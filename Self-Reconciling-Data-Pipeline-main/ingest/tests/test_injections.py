"""Unit tests for the Phase 2 failure injectors (no database needed)."""
from zoneinfo import ZoneInfo

import pyarrow as pa

from reconcile_ingest.datasets import DATASETS
from reconcile_ingest.pipeline import _apply_injections

TZ = ZoneInfo("Asia/Kolkata")


def test_t5_offsetting_errors_keep_row_count_but_lose_keys():
    t = pa.table({"claim_id": [f"C{i:04d}" for i in range(200)],
                  "_row_id": [f"r{i:04d}" for i in range(200)]})
    out = _apply_injections(DATASETS["claims"], t, ["offsetting_errors"], TZ, "b-20261013-claims-01")
    assert out.num_rows == 200                                  # count looks fine
    assert len(set(out["claim_id"].to_pylist())) == 150         # 50 lost + 50 doubled
    assert len(set(out["_row_id"].to_pylist())) == 200          # copies get their own row ids


def test_t11_unit_change_converts_only_glucose():
    t = pa.table({"test_code": ["GLU", "HGB", "GLU"],
                  "result_value": [90.08, 13.5, None],
                  "result_unit": ["mg/dL", "g/dL", "mg/dL"]})
    out = _apply_injections(DATASETS["lab_results"], t, ["unit_change"], TZ, "b-20261013-lab_results-01")
    assert out["result_unit"].to_pylist() == ["mmol/L", "g/dL", "mmol/L"]
    assert out["result_value"].to_pylist() == [5.0, 13.5, None]
    assert out.num_rows == 3