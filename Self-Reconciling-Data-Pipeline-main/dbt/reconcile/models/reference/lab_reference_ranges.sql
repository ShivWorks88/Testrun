{#
  Expected unit and plausible range per lab test. Derived from the clean data of 11–14 Oct 2026
  (range = half the lowest value seen .. 1.5 × the highest). Kept as a model, not a seed, because
  dbt seeds read the sodium code "NA" as an empty value.
#}
{{ config(materialized = 'table') }}

select * from unnest([
    struct('ALT'   as test_code, 'U/L'     as expected_unit, 0.05   as min_plausible, 125.175 as max_plausible),
    struct('AST',   'U/L',     0.05,   106.455),
    struct('CHOL',  'mg/dL',   28.154, 490.151),
    struct('CREAT', 'mg/dL',   0.05,   3.512),
    struct('GLU',   'mg/dL',   0.05,   305.924),
    struct('HBA1C', '%',       1.124,  16.379),
    struct('HGB',   'g/dL',    2.774,  29.472),
    struct('K',     'mmol/L',  1.276,  8.943),
    struct('NA',    'mmol/L',  63.811, 224.877),
    struct('PLT',   '10^9/L',  2.75,   754.881),
    struct('TRIG',  'mg/dL',   0.05,   519.725),
    struct('WBC',   '10^9/L',  0.05,   26.093)
])