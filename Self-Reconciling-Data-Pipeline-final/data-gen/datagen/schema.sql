-- Synthetic source replicas for the POC. They mimic the EHR, LIS and Billing read replicas
-- plus the master patient index and the EHR archive index. All patient data is synthetic.
-- Timestamps are stored in UTC (timestamptz); facilities define "a day" in local time.

CREATE SCHEMA IF NOT EXISTS ehr;
CREATE SCHEMA IF NOT EXISTS lis;
CREATE SCHEMA IF NOT EXISTS billing;
CREATE SCHEMA IF NOT EXISTS mpi;

CREATE TABLE IF NOT EXISTS ehr.patients (
    patient_id   text PRIMARY KEY,
    mrn          text NOT NULL,
    given_name   text NOT NULL,
    family_name  text NOT NULL,
    birth_date   date NOT NULL,
    sex          text NOT NULL,
    facility_id  text NOT NULL,
    created_at   timestamptz NOT NULL,
    updated_at   timestamptz NOT NULL
);
CREATE INDEX IF NOT EXISTS patients_updated_at ON ehr.patients (updated_at);

CREATE TABLE IF NOT EXISTS ehr.encounters (
    encounter_id   text PRIMARY KEY,
    patient_id     text NOT NULL,
    facility_id    text NOT NULL,
    encounter_type text NOT NULL,           -- inpatient | outpatient | emergency
    ward           text,
    admit_ts       timestamptz NOT NULL,
    discharge_ts   timestamptz,
    status         text NOT NULL,           -- open | closed
    updated_at     timestamptz NOT NULL
);
CREATE INDEX IF NOT EXISTS encounters_admit_ts ON ehr.encounters (admit_ts);

CREATE TABLE IF NOT EXISTS ehr.diagnoses (
    diagnosis_id text PRIMARY KEY,
    encounter_id text NOT NULL,
    icd10_code   text NOT NULL,
    rank         int  NOT NULL,
    recorded_ts  timestamptz NOT NULL
);
CREATE INDEX IF NOT EXISTS diagnoses_recorded_ts ON ehr.diagnoses (recorded_ts);

-- Retention: the EHR archives old encounters out of the live tables.
CREATE TABLE IF NOT EXISTS ehr.archive_index (
    encounter_id text PRIMARY KEY,
    archived_ts  timestamptz NOT NULL
);
CREATE TABLE IF NOT EXISTS ehr.archive_policy (
    id          int PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    cutoff_date date
);
INSERT INTO ehr.archive_policy (id, cutoff_date) VALUES (1, NULL) ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS lis.lab_orders (
    lab_order_id  text PRIMARY KEY,
    encounter_id  text NOT NULL,
    patient_id    text NOT NULL,
    priority      text NOT NULL,            -- routine | urgent | stat
    ordering_dept text NOT NULL,
    ordered_ts    timestamptz NOT NULL
);
CREATE INDEX IF NOT EXISTS lab_orders_ordered_ts ON lis.lab_orders (ordered_ts);

CREATE TABLE IF NOT EXISTS lis.lab_panels (
    lab_panel_id text PRIMARY KEY,
    lab_order_id text NOT NULL,
    panel_code   text NOT NULL,
    panel_name   text NOT NULL,
    ordered_ts   timestamptz NOT NULL
);
CREATE INDEX IF NOT EXISTS lab_panels_ordered_ts ON lis.lab_panels (ordered_ts);

CREATE TABLE IF NOT EXISTS lis.lab_results (
    lab_result_id  text PRIMARY KEY,
    lab_order_id   text NOT NULL,
    lab_panel_id   text NOT NULL,
    encounter_id   text NOT NULL,
    patient_id     text NOT NULL,
    test_code      text NOT NULL,
    test_name      text NOT NULL,
    result_value   numeric(12,3),
    result_unit    text NOT NULL,
    reference_low  numeric(12,3),
    reference_high numeric(12,3),
    collected_at   timestamptz NOT NULL,
    resulted_ts    timestamptz NOT NULL
);
CREATE INDEX IF NOT EXISTS lab_results_resulted_ts ON lis.lab_results (resulted_ts);

CREATE TABLE IF NOT EXISTS billing.claims (
    claim_id       text PRIMARY KEY,
    encounter_id   text NOT NULL,
    patient_id     text NOT NULL,
    payer          text NOT NULL,
    claim_amount   numeric(14,2),
    status         text NOT NULL,           -- submitted | adjudicated | reversed
    submitted_ts   timestamptz NOT NULL,
    adjudicated_ts timestamptz
);
CREATE INDEX IF NOT EXISTS claims_submitted_ts ON billing.claims (submitted_ts);

CREATE TABLE IF NOT EXISTS billing.payments (
    payment_id  text PRIMARY KEY,
    claim_id    text NOT NULL,
    paid_amount numeric(14,2),
    method      text NOT NULL,
    paid_ts     timestamptz NOT NULL
);
CREATE INDEX IF NOT EXISTS payments_paid_ts ON billing.payments (paid_ts);

CREATE TABLE IF NOT EXISTS billing.adjustments (
    adjustment_id     text PRIMARY KEY,
    claim_id          text NOT NULL,
    adjustment_amount numeric(14,2),
    reason_code       text NOT NULL,
    adjusted_ts       timestamptz NOT NULL
);
CREATE INDEX IF NOT EXISTS adjustments_adjusted_ts ON billing.adjustments (adjusted_ts);

CREATE TABLE IF NOT EXISTS mpi.patient_merges (
    retired_id   text PRIMARY KEY,
    surviving_id text NOT NULL,
    merged_ts    timestamptz NOT NULL
);

-- Which business dates the generator has produced (for the console and the runbook).
CREATE TABLE IF NOT EXISTS public.generated_dates (
    business_date date PRIMARY KEY,
    seed          bigint NOT NULL,
    scale         numeric NOT NULL,
    generated_at  timestamptz NOT NULL DEFAULT now()
);
