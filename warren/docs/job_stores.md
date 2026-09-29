# Job stores

`MongoDBJobStore` holds job definitions and completion status. Per-item,
per-stage outcomes live in `MongoDBJobResultsStore`; publishing outcomes live
in `MongoDBPublishingTracker`. Call `setup()` on each store before use.

## Retention

Retention is unmanaged by default, leaving existing indexes untouched.
Configure MongoDB expiry through `RuntimeConfig`:

```yaml
retention:
  job_records_ttl_seconds: 604800
  job_records_max_age_seconds: 2592000
```

| Collection | TTL field | Setting |
| --- | --- | --- |
| `jobs` | `status.completed_at` | `job_records_ttl_seconds` |
| `job_results` | `time` | `job_records_ttl_seconds` |
| `job_publishing_results` | `time` | `job_records_ttl_seconds` |
| `jobs` | `created_at` | `job_records_max_age_seconds` |

The result stores already write `time` as a BSON datetime on every insert or
update, including publishing failures with `doc_id=None`. Existing records
therefore need no timestamp backfill. Jobs without a completion datetime are
ignored by the completion TTL index.

The optional maximum age bounds jobs that never complete. It applies to all
jobs from creation, including completed jobs, so it may expire a job before
its completion-based TTL. When both settings are set, maximum age must be at
least the completion TTL. Values must be positive. Choose durations longer than the jobs
and status history you need to retain. MongoDB removes expired rows
asynchronously, not at an exact deadline.

At setup, positive settings create TTL indexes or replace conflicting options.
`null` leaves existing indexes untouched, even if another process created them.
Removing a TTL index is an operator action.

The status runner and bundled publishing scripts pass the settings to their
stores. Custom factories should pass `config.retention.job_records_ttl_seconds`
to all three MongoDB stores and `config.retention.job_records_max_age_seconds`
to `MongoDBJobStore`. Injected stores own their retention policy. Memory stores
remain in-process stores without background expiry.

## Completion

The status worker recounts successful final-stage items and distinct failed
item IDs across stages. A partial index on `(job_id, doc_id)` includes only
rows where `hard_failure` exists. The failure-count query uses the same
`$exists` predicate so MongoDB can use that index.

`update_completion` updates only jobs whose `status.completed_at` is null or
missing, and returns whether the update was applied. Completed jobs keep
their status and timestamp; this method cannot reopen them. The memory store
uses the same contract. Only the status worker that applies the completion
update emits `job-completed`.

The completion update and signal publication are separate operations. A
process failure or an ambiguous write response after committing completion
can still lose the signal; conditional completion prevents duplicate signals
from competing workers but does not guarantee delivery.
