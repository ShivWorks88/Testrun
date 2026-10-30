# Event topics, each with a dead-letter topic so no event is silently lost.
#   run-complete     ingest/dbt runs finished (Phase 1 emits it; the engine consumes it in Phase 4)
#   engine-requests  console requests (replay, retry, approve); the engine is the only consumer
#   notifications    messages for Slack / tickets (Phase 5)

locals {
  topics       = ["run-complete", "engine-requests", "notifications"]
  pubsub_agent = "serviceAccount:service-${data.google_project.this.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

resource "google_pubsub_topic" "main" {
  for_each                   = toset(local.topics)
  name                       = each.value
  message_retention_duration = "604800s"
  depends_on                 = [google_project_service.enabled]
}

resource "google_pubsub_topic" "dlq" {
  for_each                   = toset(local.topics)
  name                       = "${each.value}-dlq"
  message_retention_duration = "604800s"
  depends_on                 = [google_project_service.enabled]
}

resource "google_pubsub_subscription" "engine" {
  for_each                   = toset(local.topics)
  name                       = "${each.value}-engine"
  topic                      = google_pubsub_topic.main[each.value].id
  ack_deadline_seconds       = 60
  message_retention_duration = "604800s"
  expiration_policy { ttl = "" } # never expire

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.dlq[each.value].id
    max_delivery_attempts = 5
  }

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }
}

# Keep dead letters readable.
resource "google_pubsub_subscription" "dlq" {
  for_each                   = toset(local.topics)
  name                       = "${each.value}-dlq-hold"
  topic                      = google_pubsub_topic.dlq[each.value].id
  message_retention_duration = "604800s"
  expiration_policy { ttl = "" }
}

# The Pub/Sub service agent must publish to the DLQ and ack on the source subscription.
resource "google_pubsub_topic_iam_member" "dlq_publisher" {
  for_each = toset(local.topics)
  topic    = google_pubsub_topic.dlq[each.value].name
  role     = "roles/pubsub.publisher"
  member   = local.pubsub_agent
}

resource "google_pubsub_subscription_iam_member" "dlq_subscriber" {
  for_each     = toset(local.topics)
  subscription = google_pubsub_subscription.engine[each.value].name
  role         = "roles/pubsub.subscriber"
  member       = local.pubsub_agent
}
