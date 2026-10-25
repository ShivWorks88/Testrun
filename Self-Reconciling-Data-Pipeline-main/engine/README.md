# engine/ — decision engine and recovery executor (Phase 4)

Not built yet. What is already in place for it:

- `policy/policy.yml` — the rules, action catalog and never-auto list it will read (validated in CI)
- Pub/Sub `run-complete` (ingest emits it today) and `engine-requests` (the console queues replays to it)
- subscriptions `run-complete-engine` and `engine-requests-engine`, each with a dead-letter topic
- service accounts `sa-engine` and `sa-executor` with their dataset permissions
- Cloud SQL `reconcile_ops` for incidents, locks and the action journal

Until Phase 4, a replay request queued from the console is executed by running the `replay_batch` DAG.
