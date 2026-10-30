# Phase 2: KMS key for value-level reconciliation (runbook §10, T6).
# BigQuery deterministic encryption uses a keyset wrapped by this key, so lab values can be
# compared as encrypted fingerprints and evidence never contains values.
# Only sa-valuerecon may use the key.
#
# Note: Google never deletes KMS key rings. `terraform destroy` disables the key versions,
# but the key ring name stays reserved in this project.

resource "google_kms_key_ring" "reconcile" {
  project  = var.project_id
  name     = "reconcile"
  location = var.region

  depends_on = [google_project_service.enabled]
}

resource "google_kms_crypto_key" "recon_digest" {
  name     = "recon-digest"
  key_ring = google_kms_key_ring.reconcile.id
  purpose  = "ENCRYPT_DECRYPT"
}

resource "google_kms_crypto_key_iam_member" "valuerecon" {
  crypto_key_id = google_kms_crypto_key.recon_digest.id
  # BigQuery calls KMS on the caller's behalf (KEYS.NEW_WRAPPED_KEYSET, KEYS.KEYSET_CHAIN),
  # which needs the "via delegation" role rather than direct encrypt/decrypt.
  role   = "roles/cloudkms.cryptoKeyEncrypterDecrypterViaDelegation"
  member = "serviceAccount:${google_service_account.sa["valuerecon"].email}"
}