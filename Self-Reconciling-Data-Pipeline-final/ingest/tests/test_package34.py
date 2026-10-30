"""Phase 6 packages 3 and 4 — late changes, archived history, orphans, merged patients, without BigQuery.

    source has fewer rows, day before the archive cutoff   → archived_history → log_expected (EXPECTED, no alarm)
    source has fewer rows, not explained by the archive    → count_excess → a person looks
    source changed a day we loaded (inside the look-back)  → late_change → rebuild_partition (auto)
    the same, older than the look-back                     → late_change_old → rebuild_partition for THAT day
    lab result whose encounter is archived                 → archived_parent → log_expected
    lab result collected late, next day not loaded yet     → young_orphan → link_unknown_member (EXPECTED)
    lab result whose encounter exists nowhere              → true_orphan → escalate L2
    rows on a patient ID the MPI retired                   → merged_patient → remap_merged_patient (approval)
"""
from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

pytest.importorskip("google.cloud.bigquery")
yaml = pytest.importorskip("yaml")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from reconcile_ingest import engine, source_checks  # noqa: E402
from reconcile_ingest.source_checks import classify_drift, classify_orphan  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
POLICY = yaml.safe_load((ROOT / "policy" / "policy.yml").read_text())
BD = dt.date(2026, 10, 14)
TZ = ZoneInfo("Asia/Kolkata")
LOOKBACK = 3


def decide(kind: str, attempts: int = 0, policy: dict | None = None) -> dict:
    return engine.gates({"type": kind, "dataset": "x", "key": f"{kind}:x:{BD}"}, policy or POLICY,
                        checks_ran=True, attempts=attempts)


# ── package 3: source drift classifier ──────────────────────────────────────
def test_fewer_source_rows_before_archive_cutoff_is_archived_history():
    day = dt.date(2026, 10, 12)
    assert classify_drift(day, BD, LOOKBACK, 0, 180, None, None, dt.date(2026, 10, 13)) == "archived_history"


def test_fewer_source_rows_without_archive_is_count_excess():
    day = dt.date(2026, 10, 12)
    assert classify_drift(day, BD, LOOKBACK, 170, 180, None, None, None) == "count_excess"
    assert classify_drift(day, BD, LOOKBACK, 170, 180, None, None, dt.date(2026, 10, 1)) == "count_excess"


def test_changed_signature_inside_lookback_is_late_change():
    day = dt.date(2026, 10, 13)
    assert classify_drift(day, BD, LOOKBACK, 200, 200, 7, 4, None) == "late_change"
    assert classify_drift(day, BD, LOOKBACK, 203, 200, None, None, None) == "late_change"   # rows added later


def test_change_older_than_lookback_is_late_change_old():
    assert classify_drift(dt.date(2026, 10, 9), BD, LOOKBACK, 200, 200, 7, 4, None) == "late_change_old"
    assert classify_drift(dt.date(2026, 10, 12), BD, LOOKBACK, 200, 200, 7, 4, None) == "late_change"  # bd-2


def test_identical_day_is_no_finding():
    assert classify_drift(dt.date(2026, 10, 13), BD, LOOKBACK, 200, 200, 4, 4, None) is None
    assert classify_drift(dt.date(2026, 10, 13), BD, LOOKBACK, 200, 200, None, None, None) is None


# ── package 4: orphan classifier ────────────────────────────────────────────
def at(h: int, m: int = 0) -> dt.datetime:
    return dt.datetime.combine(BD, dt.time(h, m), tzinfo=TZ)


def test_archived_parent_wins_over_age():
    assert classify_orphan(True, at(23, 30), BD, False, 2) == "archived_parent"


def test_late_result_with_next_day_not_loaded_is_young():
    assert classify_orphan(False, at(22, 30), BD, False, 2) == "young_orphan"


def test_late_result_but_next_day_loaded_is_true_orphan():
    assert classify_orphan(False, at(22, 30), BD, True, 2) == "true_orphan"


def test_morning_result_without_parent_is_true_orphan():
    assert classify_orphan(False, at(9), BD, False, 2) == "true_orphan"
    assert classify_orphan(False, None, BD, False, 2) == "true_orphan"


# ── facts → policy rules → decisions ────────────────────────────────────────
@pytest.mark.parametrize("kind, rule, mode, route", [
    ("archived_history", "archived_history", "log", "auto"),
    ("count_excess", "fallback_unknown", "escalate", "L2"),
    ("late_change", "late_change", "auto", "auto"),
    ("late_change_old", "change_beyond_lookback", "auto", "auto"),
    ("archived_parent", "archived_parent", "log", "auto"),
    ("young_orphan", "young_orphan", "link", "auto"),
    ("true_orphan", "true_orphan", "escalate", "L2"),
    ("merged_patient", "merged_patient_orphan", "approve", "L2"),
])
def test_each_finding_maps_to_its_rule(kind, rule, mode, route):
    d = decide(kind)
    assert (d["rule_id"], d["mode"], d["route"]) == (rule, mode, route)


def test_expected_modes_never_use_an_attempt_even_at_the_cap():
    cap = int(POLICY["limits"]["attempt_cap"])
    for kind in ("archived_history", "archived_parent", "young_orphan"):
        assert decide(kind, attempts=cap)["mode"] in ("log", "link")


def test_rebuild_and_remap_obey_the_attempt_cap():
    cap = int(POLICY["limits"]["attempt_cap"])
    assert decide("late_change", attempts=cap)["mode"] == "escalate"
    assert decide("merged_patient", attempts=cap)["mode"] == "escalate"


def test_kill_switch_turns_rebuild_and_cards_into_recommendations():
    killed = {**POLICY, "kill_switch": True}
    assert decide("late_change", policy=killed)["mode"] == "recommend"
    assert decide("merged_patient", policy=killed)["mode"] == "recommend"
    assert decide("archived_history", policy=killed)["mode"] == "recommend"


def test_new_actions_are_implemented_and_expected_is_an_open_state():
    assert {"rebuild_partition", "remap_merged_patient"} <= engine.IMPLEMENTED_ACTIONS
    assert "EXPECTED" in engine.OPEN_STATES
    assert engine.NO_DATA_ACTIONS["log_expected"] == "log"
    assert engine.NO_DATA_ACTIONS["link_unknown_member"] == "link"


def test_remap_needs_a_data_owner_and_is_never_automatic():
    assert POLICY["actions"]["remap_merged_patient"]["mode"] == "approve"
    assert "remap_merged_patient" in engine.CARD_TEXT
    assert "merged_patient" in engine.WHAT_HAPPENED


def test_outage_breaker_still_comes_first():
    d = engine.gates({"type": "late_change", "dataset": "claims", "key": "k", "breaker": "BILLING"}, POLICY,
                     checks_ran=True, attempts=0)
    assert d["mode"] == "hold"


# ── detector settings and safety ────────────────────────────────────────────
def test_detector_defaults_and_policy_values():
    d = source_checks.detectors(POLICY)
    assert d["drift"]["enabled"] and int(d["drift"]["window_days"]) >= LOOKBACK
    assert set(d["drift"]["datasets"]) <= set(source_checks.SOURCE_TABLE)
    assert set(d["drift"]["datasets"]) <= set(source_checks.EVENT_COLUMN)
    assert float(d["orphans"]["young_hours"]) > 0
    assert source_checks.detectors({})["drift"]["window_days"] == 7


def test_findings_carry_counts_only_no_identifiers():
    f = source_checks.drift_finding("late_change", "claims", dt.date(2026, 10, 13),
                                    {"source_rows": 200, "raw_rows": 200, "source_changes": 7, "raw_changes": 4})
    assert f["key"] == "late_change:claims:2026-10-13"
    assert all(isinstance(v, int) for v in f["evidence"].values())


def test_source_checks_never_block_the_engine(monkeypatch):
    def boom():
        raise ConnectionError("replica down")
    monkeypatch.setattr(source_checks, "source", boom)
    assert source_checks.run(None, BD, POLICY, LOOKBACK) == []


def test_drift_compares_the_latest_batch_only():
    src = (ROOT / "ingest" / "reconcile_ingest" / "source_checks.py").read_text()
    assert "select max(_batch_id)" in src.split("def drift(")[1].split("def orphans")[0]


def test_staging_maps_retired_ids_through_the_remap_table():
    for model in ("stg_claims", "stg_lab_results"):
        sql = (ROOT / "dbt" / "reconcile" / "models" / "staging" / f"{model}.sql").read_text()
        assert "source('engine_maps', 'patient_remaps')" in sql.replace('"', "'")
        assert "coalesce(m.surviving_id, r.patient_id)" in sql


def test_orphan_injection_is_a_source_injection():
    pipeline = pytest.importorskip("reconcile_ingest.pipeline")
    assert "orphan_results" in pipeline.SOURCE_INJECTIONS
    assert "orphan_results" not in pipeline.INJECTIONS   # reconciliation cannot (and should not) see it
    pa = pytest.importorskip("pyarrow")
    t = pa.table({"encounter_id": [f"E-{i}" for i in range(9)],
                  "collected_at": [at(h) for h in (8, 9, 10, 11, 7, 13, 22, 23, 23)]})
    out = pipeline._orphan_results(t, TZ, BD)["encounter_id"].to_pylist()
    assert out[:2] == list(pipeline.ARCHIVED_TEST_ENCOUNTERS)
    assert out[2:5] == [f"E-20261014-9999{k}" for k in range(3)]
    assert out[5] == "E-5"                                        # 13:00 is left alone
    assert out[6] == "E-6" and out[7:] == ["E-20261015-88880", "E-20261015-88881"]   # 22:00 is not "late"
