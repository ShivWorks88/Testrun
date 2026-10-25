"""Static facts about the sources, for the console. Kept in sync with
ingest/reconcile_ingest/datasets.py by tests/test_catalog_sync.py."""
from __future__ import annotations

SOURCE_SYSTEMS = {
    "EHR": {"label": "EHR", "description": "Encounters, patients, diagnoses",
            "access": "Read replica · repeatable-read snapshot · facility local time"},
    "LIS": {"label": "LIS", "description": "Lab results, panels, orders",
            "access": "Interface replica · critical fields: value, unit, collected time"},
    "BILLING": {"label": "Billing", "description": "Claims, payments, adjustments",
                "access": "Read replica · amounts in ₹, 2 dp · 3-day lookback for reversals"},
}

DATASETS = {
    "patients": ("EHR", "merged patients keep child records on the retired ID"),
    "encounters": ("EHR", "UTC vs facility-local day; late closures; archived history"),
    "diagnoses": ("EHR", "recorded after admission, can cross midnight"),
    "lab_orders": ("LIS", "orders for encounters that only arrive the next day"),
    "lab_panels": ("LIS", "one or two panels per order"),
    "lab_results": ("LIS", "results before encounters; 0.3% null values"),
    "claims": ("BILLING", "reversals after load (3-day lookback)"),
    "payments": ("BILLING", "partial payments; paid across midnight"),
    "adjustments": ("BILLING", "negative amounts; reversal adjustments"),
}
