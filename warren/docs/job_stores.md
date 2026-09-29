# Job stores

`MongoDBJobStore` holds job definitions and completion status. Per-item,
per-stage outcomes live in `MongoDBJobResultsStore`; publishing outcomes live
in `MongoDBPublishingTracker`. Call `setup()` on each store before use.

## Retention

Job records are retained indefinitely by default. Configure MongoDB expiry
through `RuntimeConfig`:

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
least the completion TTL. Values must be nonnegative; zero makes a record
eligible for expiry at its timestamp. Choose durations longer than the jobs
and status history you need to retain. MongoDB removes expired rows
asynchronously, not at an exact deadline.

At setup, stores replace indexes on these keys when their options differ.
Setting a value back to `null` removes its TTL index. Use the same retention
configuration in all processes sharing these collections.

The status runner and bundled publishing scripts pass the settings to their
stores. Custom factories should pass `config.retention.job_records_ttl_seconds`
to all three MongoDB stores and `config.retention.job_records_max_age_seconds`
to `MongoDBJobStore`. Injected stores own their retention policy. Memory stores
remain in-process stores without background expiry.
