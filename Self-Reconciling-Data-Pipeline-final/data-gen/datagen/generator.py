"""Synthetic EHR / LIS / Billing source generator.

Deterministic: the same business date, seed and scale always produce the same rows, so a
date can be regenerated safely (rows for that date are deleted and re-inserted).

Edge cases built in (from the design's worked cases):
  * facility local day (Asia/Kolkata) vs UTC storage: early-morning admissions fall on the
    previous UTC date, and late events cross midnight into the next business date
  * lab results that arrive before their encounter exists ("young orphans", re-linked next day)
  * claims reversed after load (lookback window) with matching negative adjustments
  * patient merges (retired ID -> surviving ID) with child records still on the retired ID
  * EHR archiving of old encounters (see `archive`)
"""
from __future__ import annotations

import math
import random
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from importlib import resources
from zoneinfo import ZoneInfo

import psycopg
from faker import Faker

TZ = ZoneInfo("Asia/Kolkata")
UTC = ZoneInfo("UTC")
DEFAULT_SEED = 2026
BASE_PATIENTS = 50_000

FACILITIES = ["FAC-MUM-01", "FAC-MUM-02", "FAC-PUN-01"]
WARDS = ["MED-1", "MED-2", "SURG-1", "ICU", "PAED", "MAT", "ORTHO", "CARD"]
ENCOUNTER_TYPES = [("outpatient", 0.62), ("emergency", 0.18), ("inpatient", 0.20)]
ICD10 = ["E11.9", "I10", "J18.9", "N39.0", "K35.80", "I21.4", "J45.909", "A09", "R50.9",
         "E78.5", "N18.3", "M54.5", "O80", "S72.001A", "B34.9", "D64.9", "K21.9", "F32.9"]
PAYERS = ["PMJAY", "CGHS", "STAR-HEALTH", "HDFC-ERGO", "ICICI-LOMBARD", "SELF-PAY", "NIVA-BUPA"]
PAY_METHODS = ["NEFT", "UPI", "CARD", "CHEQUE", "CASH"]
ADJ_REASONS = ["CO-45", "CO-97", "PR-1", "OA-23", "REV-REVERSAL"]
DEPTS = ["GEN-MED", "EMERG", "CARDIO", "NEPHRO", "ENDO", "SURG", "OBGYN", "PAED"]

# panel -> list of (test_code, test_name, unit, mean, sd, ref_low, ref_high)
PANELS = {
    "CBC": ("Complete blood count", [
        ("HGB", "Haemoglobin", "g/dL", 13.2, 1.8, 12.0, 17.0),
        ("WBC", "White cell count", "10^9/L", 7.5, 2.5, 4.0, 11.0),
        ("PLT", "Platelets", "10^9/L", 260.0, 70.0, 150.0, 410.0),
    ]),
    "BMP": ("Basic metabolic panel", [
        ("GLU", "Glucose", "mg/dL", 104.0, 28.0, 70.0, 140.0),
        ("NA", "Sodium", "mmol/L", 139.0, 3.0, 135.0, 145.0),
        ("K", "Potassium", "mmol/L", 4.2, 0.5, 3.5, 5.1),
        ("CREAT", "Creatinine", "mg/dL", 1.0, 0.35, 0.6, 1.3),
    ]),
    "LIPID": ("Lipid profile", [
        ("CHOL", "Total cholesterol", "mg/dL", 185.0, 38.0, 0.0, 200.0),
        ("TRIG", "Triglycerides", "mg/dL", 150.0, 60.0, 0.0, 150.0),
    ]),
    "HBA1C": ("Glycated haemoglobin", [
        ("HBA1C", "HbA1c", "%", 6.4, 1.3, 4.0, 5.6),
    ]),
    "LFT": ("Liver function", [
        ("ALT", "Alanine aminotransferase", "U/L", 32.0, 15.0, 7.0, 56.0),
        ("AST", "Aspartate aminotransferase", "U/L", 30.0, 12.0, 10.0, 40.0),
    ]),
}
PANEL_WEIGHTS = {"CBC": 0.36, "BMP": 0.30, "LIPID": 0.12, "HBA1C": 0.10, "LFT": 0.12}

# Daily volumes at scale 1.0 (from the POC plan).
VOLUMES = {
    "encounters": 10_000,
    "new_patients": 1_500,
    "lab_order_rate": 0.52,      # share of encounters with a lab order  -> ~18,000 results
    "payment_rate": 0.60,        # share of claims paid                   -> ~6,000 payments
    "orphans": 12,               # lab orders (~40 results) whose encounter only arrives the next day
    "reversal_rate": 0.005,      # claims from the previous 3 days reversed today
    "extra_adjustments": 300,
    "merges": 3,
}


@dataclass
class DayResult:
    business_date: date
    counts: dict


def _rng(seed: int, d: date, salt: str = "") -> random.Random:
    return random.Random(f"{seed}:{d.isoformat()}:{salt}")


def _day_bounds(d: date) -> tuple[datetime, datetime]:
    start = datetime.combine(d, time(0, 0), tzinfo=TZ)
    return start, start + timedelta(days=1)


def _diurnal_ts(rng: random.Random, d: date) -> datetime:
    """A local timestamp within day d, weighted towards daytime but with night activity."""
    hour = min(23, max(0, int(rng.gauss(13, 5))))
    if rng.random() < 0.12:              # emergencies at any hour
        hour = rng.randrange(24)
    return datetime.combine(d, time(hour, rng.randrange(60), rng.randrange(60)), tzinfo=TZ)


def _money(rng: random.Random, mu: float, sigma: float, lo: float, hi: float) -> Decimal:
    v = min(hi, max(lo, rng.lognormvariate(mu, sigma)))
    return Decimal(f"{v:.2f}")


def _base_patient_id(n: int) -> str:
    return f"P-{n:06d}"


def base_count(scale: float) -> int:
    """Size of the base patient population for a scale factor."""
    return BASE_PATIENTS if scale >= 1 else max(100, int(BASE_PATIENTS * scale))


def _patient_for_id(seed: int, ident: str, base_n: int) -> str:
    """Stable base-population patient for an ID (used for orphan encounters)."""
    return _base_patient_id(1 + random.Random(f"{seed}:{ident}").randrange(base_n))


def _load_schema(conn: psycopg.Connection) -> None:
    sql = resources.files("datagen").joinpath("schema.sql").read_text()
    conn.execute(sql)


def _copy(conn: psycopg.Connection, table: str, cols: list[str], rows: list[tuple]) -> None:
    if not rows:
        return
    with conn.cursor() as cur:
        with cur.copy(f"COPY {table} ({', '.join(cols)}) FROM STDIN") as cp:
            for r in rows:
                cp.write_row(r)


# ------------------------------------------------------------------ init

def init(dsn: str, seed: int = DEFAULT_SEED, scale: float = 1.0) -> int:
    """Create the source schemas and the base patient population. Idempotent."""
    n = base_count(scale)
    with psycopg.connect(dsn) as conn:
        _load_schema(conn)
        existing = conn.execute("select count(*) from ehr.patients where patient_id like 'P-0%'").fetchone()[0]
        if existing >= n:
            return existing
        fake = Faker("en_IN")
        fake.seed_instance(seed)
        rng = random.Random(f"{seed}:base")
        created = datetime(2024, 1, 1, tzinfo=TZ)
        rows = []
        for i in range(existing + 1, n + 1):
            sex = rng.choice(["F", "M"])
            rows.append((
                _base_patient_id(i), f"MRN{100000 + i}",
                fake.first_name_female() if sex == "F" else fake.first_name_male(), fake.last_name(),
                date(1940, 1, 1) + timedelta(days=rng.randrange(80 * 365)), sex,
                rng.choice(FACILITIES), created, created,
            ))
        _copy(conn, "ehr.patients",
              ["patient_id", "mrn", "given_name", "family_name", "birth_date", "sex", "facility_id",
               "created_at", "updated_at"], rows)
        return n


# ------------------------------------------------------------------ one business date

def _delete_day(conn: psycopg.Connection, d: date) -> None:
    tag = d.strftime("%Y%m%d")
    start, end = _day_bounds(d)
    for table, col, prefix in [
        ("ehr.diagnoses", "diagnosis_id", "G"), ("ehr.encounters", "encounter_id", "E"),
        ("ehr.patients", "patient_id", "P"), ("lis.lab_results", "lab_result_id", "R"),
        ("lis.lab_panels", "lab_panel_id", "N"), ("lis.lab_orders", "lab_order_id", "O"),
        ("billing.adjustments", "adjustment_id", "A"), ("billing.payments", "payment_id", "Y"),
        ("billing.claims", "claim_id", "C"),
    ]:
        conn.execute(f"delete from {table} where {col} like %s", (f"{prefix}-{tag}-%",))
    conn.execute("delete from mpi.patient_merges where merged_ts >= %s and merged_ts < %s", (start, end))


def generate_day(dsn: str, d: date, seed: int = DEFAULT_SEED, scale: float = 1.0) -> DayResult:
    tag = d.strftime("%Y%m%d")
    nxt = (d + timedelta(days=1)).strftime("%Y%m%d")
    rng = _rng(seed, d)
    fake = Faker("en_IN")
    fake.seed_instance(f"{seed}:{tag}")

    n_enc = max(1, int(VOLUMES["encounters"] * scale))
    n_new = max(1, int(VOLUMES["new_patients"] * scale))
    n_orphans = max(1, int(round(VOLUMES["orphans"] * scale)))
    base_n = base_count(scale)

    patients, encounters, diagnoses = [], [], []
    orders, panels, results = [], [], []
    claims, payments, adjustments, merges = [], [], [], []

    # New patients registered today.
    new_ids = []
    for i in range(n_new):
        pid = f"P-{tag}-{i:04d}"
        new_ids.append(pid)
        sex = rng.choice(["F", "M"])
        ts = _diurnal_ts(rng, d)
        patients.append((pid, f"MRN{tag}{i:04d}",
                         fake.first_name_female() if sex == "F" else fake.first_name_male(), fake.last_name(),
                         date(1940, 1, 1) + timedelta(days=rng.randrange(84 * 365)), sex,
                         rng.choice(FACILITIES), ts, ts))

    def pick_patient() -> str:
        if rng.random() < 0.15:
            return rng.choice(new_ids)
        return _base_patient_id(1 + rng.randrange(base_n))

    def enc_types() -> str:
        r, acc = rng.random(), 0.0
        for t, w in ENCOUNTER_TYPES:
            acc += w
            if r <= acc:
                return t
        return "outpatient"

    def add_encounter(eid: str, pid: str, admit: datetime) -> None:
        etype = enc_types()
        if etype == "inpatient":
            discharge = admit + timedelta(hours=rng.uniform(20, 120)) if rng.random() < 0.55 else None
        elif etype == "emergency":
            discharge = admit + timedelta(hours=rng.uniform(2, 10))
        else:
            discharge = admit + timedelta(minutes=rng.uniform(20, 90))
        # Late closures: some encounters close after the day ends (updated_at moves on).
        status = "closed" if discharge is not None else "open"
        encounters.append((eid, pid, rng.choice(FACILITIES), etype,
                           rng.choice(WARDS) if etype == "inpatient" else None,
                           admit, discharge, status, discharge or admit))
        for k in range(rng.choice([1, 2, 2, 3])):
            diagnoses.append((f"G-{eid[2:]}-{k}", eid, rng.choice(ICD10), k + 1,
                              admit + timedelta(minutes=rng.uniform(5, 240))))

    # Encounters promised yesterday by early lab results (they arrive today, early morning).
    for k in range(n_orphans):
        eid = f"E-{tag}-{90000 + k}"
        add_encounter(eid, _patient_for_id(seed, eid, base_n),
                      datetime.combine(d, time(rng.randrange(0, 6), rng.randrange(60)), tzinfo=TZ))

    # Today's encounters.
    enc_index = []
    for i in range(n_enc):
        eid = f"E-{tag}-{i:05d}"
        pid = pick_patient()
        admit = _diurnal_ts(rng, d)
        add_encounter(eid, pid, admit)
        enc_index.append((eid, pid, admit))

    # Lab orders, panels and results.
    panel_codes = list(PANEL_WEIGHTS)
    panel_w = [PANEL_WEIGHTS[p] for p in panel_codes]
    o_n = n_n = r_n = 0

    def add_order(eid: str, pid: str, ordered: datetime) -> None:
        nonlocal o_n, n_n, r_n
        oid = f"O-{tag}-{o_n:05d}"
        o_n += 1
        orders.append((oid, eid, pid, rng.choice(["routine", "routine", "urgent", "stat"]),
                       rng.choice(DEPTS), ordered))
        for code in sorted(set(rng.choices(panel_codes, panel_w, k=1 if rng.random() < 0.7 else 2))):
            nid = f"N-{tag}-{n_n:05d}"
            n_n += 1
            name, tests = PANELS[code]
            panels.append((nid, oid, code, name, ordered))
            collected = ordered + timedelta(minutes=rng.uniform(5, 60))
            for tcode, tname, unit, mean, sd, lo, hi in tests:
                resulted = collected + timedelta(minutes=rng.uniform(30, 360))  # may cross midnight
                value = None if rng.random() < 0.003 else Decimal(f"{max(0.1, rng.gauss(mean, sd)):.3f}")
                results.append((f"R-{tag}-{r_n:06d}", oid, nid, eid, pid, tcode, tname, value, unit,
                                Decimal(f"{lo:.3f}"), Decimal(f"{hi:.3f}"), collected, resulted))
                r_n += 1

    for eid, pid, admit in enc_index:
        if rng.random() < VOLUMES["lab_order_rate"]:
            add_order(eid, pid, admit + timedelta(minutes=rng.uniform(10, 360)))
    # Young orphans: results whose encounter is only created tomorrow.
    for k in range(n_orphans):
        eid = f"E-{nxt}-{90000 + k}"
        start, _ = _day_bounds(d)
        add_order(eid, _patient_for_id(seed, eid, base_n), start + timedelta(hours=22, minutes=rng.uniform(0, 100)))

    # Claims, payments and adjustments.
    for i, (eid, pid, admit) in enumerate(enc_index):
        cid = f"C-{tag}-{i:05d}"
        submitted = admit + timedelta(hours=rng.uniform(1, 20))
        amount = None if rng.random() < 0.001 else _money(rng, 9.0, 1.0, 300, 450_000)
        adjudicated = submitted + timedelta(hours=rng.uniform(2, 48)) if rng.random() < 0.7 else None
        claims.append((cid, eid, pid, rng.choice(PAYERS), amount,
                       "adjudicated" if adjudicated else "submitted", submitted, adjudicated))
        if amount is not None and rng.random() < VOLUMES["payment_rate"]:
            paid = submitted + timedelta(hours=rng.uniform(1, 30))
            frac = Decimal(str(rng.choice([1, 1, 1, 0.9, 0.8, 0.5])))
            payments.append((f"Y-{tag}-{len(payments):05d}", cid,
                             (amount * frac).quantize(Decimal("0.01")), rng.choice(PAY_METHODS), paid))
    for k in range(int(VOLUMES["extra_adjustments"] * scale)):
        cid = f"C-{tag}-{rng.randrange(n_enc):05d}"
        adjustments.append((f"A-{tag}-{k:05d}", cid, _money(rng, 6.0, 1.0, 50, 25_000) * -1,
                            rng.choice(ADJ_REASONS[:-1]), _diurnal_ts(rng, d)))

    # Patient merges: a retired ID (with history) merged into a surviving ID.
    for _ in range(VOLUMES["merges"]):
        retired = _base_patient_id(1 + rng.randrange(base_n))
        surviving = _base_patient_id(1 + rng.randrange(base_n))
        if retired != surviving:
            merges.append((retired, surviving, _diurnal_ts(rng, d)))

    with psycopg.connect(dsn) as conn:
        _load_schema(conn)
        with conn.transaction():
            _delete_day(conn, d)
            _copy(conn, "ehr.patients", ["patient_id", "mrn", "given_name", "family_name", "birth_date", "sex",
                                         "facility_id", "created_at", "updated_at"], patients)
            _copy(conn, "ehr.encounters", ["encounter_id", "patient_id", "facility_id", "encounter_type", "ward",
                                           "admit_ts", "discharge_ts", "status", "updated_at"], encounters)
            _copy(conn, "ehr.diagnoses", ["diagnosis_id", "encounter_id", "icd10_code", "rank", "recorded_ts"],
                  diagnoses)
            _copy(conn, "lis.lab_orders", ["lab_order_id", "encounter_id", "patient_id", "priority",
                                           "ordering_dept", "ordered_ts"], orders)
            _copy(conn, "lis.lab_panels", ["lab_panel_id", "lab_order_id", "panel_code", "panel_name",
                                           "ordered_ts"], panels)
            _copy(conn, "lis.lab_results", ["lab_result_id", "lab_order_id", "lab_panel_id", "encounter_id",
                                            "patient_id", "test_code", "test_name", "result_value", "result_unit",
                                            "reference_low", "reference_high", "collected_at", "resulted_ts"],
                  results)
            _copy(conn, "billing.claims", ["claim_id", "encounter_id", "patient_id", "payer", "claim_amount",
                                           "status", "submitted_ts", "adjudicated_ts"], claims)
            _copy(conn, "billing.payments", ["payment_id", "claim_id", "paid_amount", "method", "paid_ts"], payments)
            _copy(conn, "billing.adjustments", ["adjustment_id", "claim_id", "adjustment_amount", "reason_code",
                                                "adjusted_ts"], adjustments)
            with conn.cursor() as cur:
                cur.executemany(
                    "insert into mpi.patient_merges values (%s, %s, %s) on conflict (retired_id) do nothing", merges)

            # Reversals after load: a few claims from the previous 3 days are reversed today.
            reversed_n = _reverse_recent_claims(conn, d, rng, scale)

            conn.execute(
                "insert into public.generated_dates (business_date, seed, scale) values (%s, %s, %s) "
                "on conflict (business_date) do update set seed = excluded.seed, scale = excluded.scale, "
                "generated_at = now()", (d, seed, scale))

    return DayResult(d, {
        "patients": len(patients), "encounters": len(encounters), "diagnoses": len(diagnoses),
        "lab_orders": len(orders), "lab_panels": len(panels), "lab_results": len(results),
        "claims": len(claims), "payments": len(payments),
        "adjustments": len(adjustments) + reversed_n, "patient_merges": len(merges),
        "claims_reversed": reversed_n, "orphan_lab_results": sum(1 for r in results if f"E-{nxt}-" in r[3]),
    })


def _reverse_recent_claims(conn: psycopg.Connection, d: date, rng: random.Random, scale: float) -> int:
    tag = d.strftime("%Y%m%d")
    start, _ = _day_bounds(d)
    lookback_start, _ = _day_bounds(d - timedelta(days=3))
    candidates = conn.execute(
        "select claim_id, claim_amount from billing.claims where submitted_ts >= %s and submitted_ts < %s "
        "and claim_amount is not null order by claim_id",
        (lookback_start, start)).fetchall()
    if not candidates:
        return 0
    k = min(len(candidates), max(1, math.ceil(VOLUMES["encounters"] * scale * VOLUMES["reversal_rate"])))
    chosen = rng.sample(candidates, k)
    rows = []
    for i, (cid, amount) in enumerate(chosen):
        ts = _diurnal_ts(rng, d)
        conn.execute("update billing.claims set status = 'reversed', adjudicated_ts = %s where claim_id = %s",
                     (ts, cid))
        rows.append((f"A-{tag}-R{i:04d}", cid, -amount, "REV-REVERSAL", ts))
    _copy(conn, "billing.adjustments", ["adjustment_id", "claim_id", "adjustment_amount", "reason_code",
                                        "adjusted_ts"], rows)
    return len(rows)


# ------------------------------------------------------------------ retention

def archive(dsn: str, before: date) -> int:
    """Simulate the EHR retention job: move encounters admitted before `before` out of the live
    table into the archive index. Their child records (labs, claims) stay, so they become orphans
    that the reconciliation must recognise as expected, not broken."""
    start, _ = _day_bounds(before)
    with psycopg.connect(dsn) as conn, conn.transaction():
        _load_schema(conn)
        moved = conn.execute(
            "with gone as (delete from ehr.encounters where admit_ts < %s returning encounter_id) "
            "insert into ehr.archive_index (encounter_id, archived_ts) select encounter_id, now() from gone "
            "on conflict do nothing", (start,)).rowcount
        conn.execute("update ehr.archive_policy set cutoff_date = %s where id = 1", (before,))
        return moved


def status(dsn: str) -> dict:
    with psycopg.connect(dsn) as conn:
        _load_schema(conn)
        dates = [r[0].isoformat() for r in conn.execute(
            "select business_date from public.generated_dates order by 1").fetchall()]
        counts = {}
        for t in ["ehr.patients", "ehr.encounters", "ehr.diagnoses", "lis.lab_orders", "lis.lab_panels",
                  "lis.lab_results", "billing.claims", "billing.payments", "billing.adjustments",
                  "mpi.patient_merges", "ehr.archive_index"]:
            counts[t] = conn.execute(f"select count(*) from {t}").fetchone()[0]
        cutoff = conn.execute("select cutoff_date from ehr.archive_policy where id = 1").fetchone()[0]
        return {"generated_dates": dates, "row_counts": counts,
                "archive_cutoff": cutoff.isoformat() if cutoff else None}
