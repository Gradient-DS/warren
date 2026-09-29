# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.6.2] — 2026-09-29

> Configurable publication routing, and release versions derived from Git tags. Patch: additive and backward compatible.

### Added

- Optional `route_func` on `JobPublicationWorkerRunner` configures routing for
  published documents. The standard publication worker launcher reads it from
  `PipelineSpec.publication=PublishSpec(route_func=...)`; omitting the spec keeps
  the existing exchange-based routing.

### Fixed

- Release versions now come from Git tags via hatch-vcs, with a `0.0.0`
  fallback when version detection is unavailable. Publishing rejects
  distributions whose versions do not match the GitHub Release tag, preventing
  stale versions from being uploaded to PyPI.

## [0.6.1] — 2026-09-29

> Queue lanes and message priority: optional `lane` and `priority` on the message envelope, lane-prefixed routing keys and AMQP priority on publish. Patch: additive and backward compatible.

### Added

- Optional application-defined `lane` and AMQP `priority` fields on processing
  messages, preserved by `derive()` and retry replay. Priority accepts integers
  from 0 through 255, excluding booleans.
- Optional `prefix_field` and `default_prefix` on `MessageFieldRouter` for
  lane-prefixed routing keys. RabbitMQ publishers forward message priority;
  the memory backend supports lane routing and ignores priority ordering;
  the Kafka backend ignores priority.

## [0.6.0] — 2026-09-29

> Production hardening: connection auth and startup retries, handler timeouts, consumer concurrency, multi-key bindings, bulk result writes, cache expiry, job-record retention, scopes and a single active retry scheduler. Minor, not patch: unknown config keys are now rejected.

### Added

- `erase_scope` removes scoped MongoDB content, Redis keys, and control rows with deletion counts, falling back to collection drops when database drops are denied; see `warren/docs/scoping.md`.
- Optional scoping isolates content in per-scope MongoDB databases and Redis namespaces, validates at storage access, labels control rows with valid scopes, and supplies worker factories with a database resolver.
- Message scopes propagate through worker, failure, retry, and job-publication envelopes and into handler executor threads; runtime scoping defaults to disabled.
- Optional MongoDB job-record retention covers completion, result updates, and publishing outcomes, including failures without an item ID; a maximum-age backstop bounds unfinished jobs, and setup reconciles explicitly configured TTL indexes.
- Runtime `documents.cache_ttl_seconds` (86400) and `results.cache_ttl_seconds` (3600) configure positive cache expiry; fetched payload TTLs apply to both shared and job-scoped keys.
- `ResultsStoreInterface.store_many` batches result upserts into one unordered MongoDB bulk write and an optional cache pipeline; memory stores support the API, and the last item wins for repeated keys.
- Consumer concurrency is configurable on RabbitMQ and memory, health responses include in-flight handler counts, RabbitMQ prefetch is at least explicit concurrency, and Kafka rejects concurrency above one.
- In-process pipelines accept per-worker instance counts and runtime memory handler settings; unset concurrency uses RabbitMQ prefetch or one handler on Kafka and memory.
- Worker queues accept multiple `binding_keys` with `binding_key` retained as a single-key constructor alias; fanout ignores keys, and overlapping RabbitMQ or memory bindings deliver once per matching queue.
- Optional handler timeouts on all three backends use the existing retry policy, and liveness fails when a handler exceeds twice its timeout, including handlers that ignore cancellation.
- Startup connection attempts support bounded exponential backoff, MongoDB and Redis pings, and cleanup between failures; the default remains one attempt, and memory needs no external connections.
- MongoDB and Redis runtime settings accept connection strings, credentials, TLS, pool limits, and timeouts; YAML string values expand `${VAR}` and reject unset variables.
- Store deletes accept expected field values so retry cleanup cannot delete a replacement written by a standby.

### Changed

- Job hard-failure counts use a partial MongoDB index on `(job_id, doc_id)` while retaining distinct-item counting across stages.
- Multi-part result reads bypass caches and query persistence in ascending `part_idx` order; `try_cache` is deprecated and ignored, single-part reads retain their cache, and Redis prefix scans use count 1000.
- Runtime configuration rejects unknown keys at every modeled level.
- `MemoryDocumentStore.update` raises `DocumentAlreadyExistsError` when an update would duplicate a unique key, matching MongoDB's rejection.
- README documents deployment settings, routing bindings, retry ownership, retention, and scoping; ROADMAP removes completed batch-write and prefetch/concurrency work.

### Fixed

- Unset job-record retention leaves existing TTL indexes untouched, so processes without retention settings cannot remove them. Configured TTLs must be positive; removing an index is an operator action.
- Distributed retry workers share a MongoDB lease, renew every third of `retry.lease_ttl_seconds` (default 30), cancel local retries on lease loss, and recover persisted retries after standby takeover.
- Concurrent status workers emit only one completion signal by conditionally setting completion time; `update_completion` returns whether it applied the update and preserves already-completed status and timestamps.
- Binary result caches expire after 3600 seconds by default, retry caches use the maximum delay cap plus 60 seconds, Redis defaults of `None` use 3600 seconds, and framework cache writes pass TTLs explicitly.
- `MemoryDocumentStore.insert` uses hash indexes for identity and unique keys instead of scanning every row; the recorded 2,770-result benchmark improved from 2.7 s to 0.04 s.
- The `dev` extra includes the `rmq`, `kafka`, `http`, and `s3` extras imported by tests, so the documented `pip install -e ".[dev]"` supplies the test dependencies.

## [0.5.0] — 2026-09-19

> Adds the in-process `memory` backend: a whole pipeline in one process, no
> infrastructure and no new dependency. Minor, not patch: the `mongo_client` /
> `redis_client` fields on `RuntimeInfra` and `WorkerFactoryContext` are now
> typed as optional.

### Added

- In-process `memory` backend: run a whole pipeline in one process with no
  RabbitMQ, Kafka, MongoDB or Redis. Pure standard library, no extra to install.
  Supports fanout, topic and direct exchanges with the same retry and
  failure-envelope behaviour as the other backends. See `warren/docs/memory.md`
  and `python -m examples.rag.run_local`.
- In-process stores: `MemoryDocumentStore`, `MemoryJobStore`,
  `MemoryJobResultsStore`, `MemoryCache`, shared through `MemoryStoreRegistry`.
- `warren.runtime.in_process`: `create_in_process_runners` and `run_in_process`.
- `DefaultWorkerRunner`, `JobStatusWorkerRunner` and `RetryWorkerRunner` accept an
  optional `infra=` so several runners can share one `RuntimeInfra`.

### Changed

- `RuntimeInfra.mongo_client` / `redis_client` and
  `WorkerFactoryContext.mongo_client` / `redis_client` are now typed as optional.
  They are `None` only on `backend: memory`; on RabbitMQ and Kafka they are
  always set, as before.

### Fixed

- `JobStatusWorkerRunner`, `RetryWorkerRunner` and `JobPublicationWorkerRunner`
  ignored the `health:` section of `RuntimeConfig` and always served the health
  endpoint with default settings (enabled, port 8080). They now honour it, as
  `DefaultWorkerRunner` already did.
- `MongoDBJobResultsStore.get_stage_counts` reported `soft_failed: 0` for every
  stage. Field presence was tested by comparing `$type` output to `"missing"` as
  strings, which is false for array fields.

## [0.4.0] — 2026-09-18

> `ConsumerManagerInterface` gains `health()` and `WorkerRunnerBase.run()`
> gains a watchdog that can end the process.

### Added

- **Configurable AMQP heartbeat**: `rabbitmq.connection.heartbeat` (seconds)
  on `RMQConnectionConfig`. RabbitMQ adopts the client's `Tune-Ok` value
  as-is, so this setting alone decides the negotiated timeout; unset keeps
  aiormq's 60. aiormq's silent-broker detection scales with it
  (`(heartbeat + 1) × 3` s).
- **Consumer health**: `ConsumerHealth` and `health()` on both consumer
  managers (connected / blocked / channel open / consumer registered), a
  watchdog in `WorkerRunnerBase.run()` that ends the run with `WarrenError`
  when the consumer has been lost with a live connection for longer than
  `health.consumer_lost_grace_s` (default 60 s), and a stdlib readiness
  endpoint (`GET /ready`, `GET /live`; `health.port`, default 8080, **on by
  default** — a failed bind is logged and the worker carries on). Blocked
  connections (`Connection.Blocked`) are reported as not ready and logged
  on entry and exit; they never trigger an exit.
- **Typed throttling in the URL resolver**: `DocumentThrottledError`
  (`retry_after`, `status_code`) on HTTP 429, or 503 carrying `Retry-After`;
  `parse_retry_after` handles delay-seconds and HTTP-dates. A bare 503 keeps
  the slot-consuming retry ladder. Per-host rate limiting is left to the
  pipeline (`build_client(transport=...)`).
- **Retry policy from YAML**: `retry.policy` (a `RetryConfig`) now reaches
  every consumer manager the runner builds; previously the library defaults
  (`max_delay_cap: 300`) applied everywhere.
- **Delivery counting**: `rabbitmq.consumer.max_deliveries` dead-letters a
  message after N broker deliveries by replaying each redelivery through
  the retry worker with a `delivery_count` in the body (a requeue carries
  the message back unchanged). `redelivery_delay` (default 5 s) is the
  replay delay. Off unless set. The hard-failure envelope's error text names
  the mechanism (`poison message: delivered N times`).
- `rabbitmq.consumer.queue_arguments` — forwarded verbatim into the worker
  queue declaration (`x-queue-type`, `x-delivery-limit`, a DLX, …). Not
  validated; the broker refuses to re-declare an existing queue with
  different arguments.

- `resolve_http.build_client()` constructs the resolver's `AsyncClient` and
  owns its redirect and timeout policy, configurable by environment:
  `HTTP_FOLLOW_REDIRECTS` (default `true`), `HTTP_TIMEOUT_S` (default `60`)
  and `HTTP_MAX_REDIRECTS` (default `20`). An unparseable
  `HTTP_FOLLOW_REDIRECTS` raises rather than reading as `false`, so a typo
  cannot silently switch redirect following off.

### Fixed

- **A delivery whose channel is dead is never settled or published for.**
  After a reconnect every in-flight delivery pins a closed channel; `ack()`
  raised out of the success path into the hard-failure handler, which
  published a false failure record and then raised again — unlogged, since
  the task's exception was never retrieved. The consumer manager now checks
  the channel before publishing, absorbs transport errors from
  ack/nack/reject with one ERROR line, and logs task exceptions. The
  broker's own requeue redelivers the message.
- **Retry delay was halved on the first deferral.** With
  `retry_count_consumed=False` on a never-retried message the backoff
  exponent was `-1`; it now clamps at 0 (both backends).

- **`ResultDoc.created_at` is stored as a BSON `Date`, not an ISO string.**
  A TTL index over a string field is inert: MongoDB's TTL monitor deletes
  only BSON Dates and skips every other type without logging a word, so a
  retention backstop declared over `created_at` deleted nothing. Nothing
  reads the field. Rows written before this change keep their ISO string
  and are not expired; `ResultDoc` still parses them into a `datetime`.
  (Forward-port of 0.2.4.)
- **`RedisDictCache` renders `datetime` values as ISO 8601 text** instead of
  refusing them. `DefaultResultsStore` caches the same dict it writes to
  MongoDB, so without this the cache write raised a `TypeError` that the
  store swallows as a log line — the results cache would have gone quietly
  dead. (Forward-port of 0.2.4.)

- **The HTTP(S) URL resolver now follows redirects.** `httpx` defaults
  `follow_redirects` to `False`, so a `307` or `303` reached
  `raise_for_status()` and surfaced as a document resolution failure rather
  than as the document. Document URLs redirect routinely: repository
  permalinks to a CDN, DOIs to a publisher, a landing path to the file
  itself. Measured on a real corpus, 91 of 96 download failures in one
  500-document run were redirects whose target was the requested PDF.

### Removed

- `warren.storage.utils.current_time_str` — `created_at` was its only caller.

## [0.2.4] — 2026-08-19

Maintenance release, cut from `v0.2.3` (not from `main`).

### Fixed

- `ResultDoc.created_at` is stored as a BSON `Date`, not an ISO string; a TTL
  index over a string field is inert. MongoDB's TTL monitor deletes only BSON
  Dates and skips every other type without logging a word, so a retention
  backstop declared over `created_at` deleted nothing. Nothing reads the field.
  Rows written before 0.2.4 keep their ISO string and are not expired by a TTL
  index; `ResultDoc` still parses them into a `datetime` on read.
- `RedisDictCache` renders `datetime` values as ISO 8601 text instead of
  refusing them. `DefaultResultsStore` caches the same dict it writes to
  MongoDB, so without this the cache write raised a `TypeError` that the store
  swallows as a log line — the results cache would have gone quietly dead.
  Cached dicts therefore carry `created_at` as ISO text; models that declare a
  `datetime` field parse it back on validation.

### Removed

- `warren.storage.utils.current_time_str` — `created_at` was its only caller.

## [0.3.0] — 2026-08-08

### Added

- **Flexible routing** (#3): pipelines can now run on `topic` and `direct`
  exchanges in addition to `fanout`.
  - Per-worker `binding_key` and `publish: PublishSpec` in the pipeline spec.
  - `CapabilityWorkerBase` — workers declare `accepts`/`produces` instead of
    implementing filtering by hand.
  - Job-defined routing: a `RoutingPlan` submitted with the job decides which
    workers process which data types (`RoutingPlanRouter`), with two-layer
    validation (spec-time and plan-time).
  - Framework-managed observer exchange so job-status observation works on
    direct pipelines; control/data publisher split and retry-by-replay.
- Runnable RAG example (`examples/rag/`): real PDFs (downloaded over HTTP) →
  text → chunks → OpenAI embeddings.
- Synthetic example pipeline wired per exchange type under
  `examples/exchanges/{fanout,topic,direct}` with a shared
  `examples/inspect_job` live per-stage job view.
- Routing design doc (`warren/docs/routing.md`).

### Changed

- **Breaking:** the exchange is now defined in the `PipelineSpec`, not in
  `config.yaml`.
- **Breaking:** the `terminal` flag on worker specs was removed; a worker with
  `publish: None` simply does not publish results downstream.
- README and docs repositioned around the general distributed-processing use
  case: "How Warren compares" and "Choosing a backend"/"Choosing an exchange"
  sections; USAGE.md and runtime docs refreshed.
- Kafka remains **fanout-only by design** for now: selecting `topic`/`direct`
  with the Kafka backend fails fast at startup (see ROADMAP.md).

## [0.2.3] — 2026-07-22

### Fixed

- Document byte-cache keys are now scoped to the job (#6), so identical source
  documents in different jobs no longer share cache entries.

## [0.2.2] — 2026-06-30

### Added

- HTTP(S) URL document resolver as the `warren[http]` extra (#5).

## [0.2.1] — 2026-06-25

### Added

- S3 document resolver (`provider=s3` on cloud document locations) as the
  `warren[s3]` extra (#4).
- Cloud resolver dispatch by provider (`gcs` | `s3`).

## [0.2.0] — 2026-06-25

### Added

- Kafka pub/sub backend on `aiokafka` (#2), wire-compatible envelopes with the
  RabbitMQ backend (enforced by parity tests).
- Backend-selectable runtime: `RuntimeConfig.backend` chooses the transport.

### Changed

- **Breaking:** transport and cloud-storage dependencies moved to optional
  extras (`rmq`, `kafka`, `gcs`); a missing extra raises
  `OptionalDependencyError` at use time.

## [0.1.1] — 2026-06-11

### Added

- PyPI publishing workflow (publishes on GitHub Release) (#1).
- Community files: contributing guidelines, code of conduct, security policy,
  CODEOWNERS, pull request template.

## [0.1.0] — 2026-06-09

Initial release.

- Message-driven document/item processing framework: self-selecting workers on
  a RabbitMQ fanout exchange, claim-check data plane (messages carry
  references, stores carry payloads), MongoDB/Redis-backed storage.
- Bounded jobs with a per-document, per-stage status ledger and a
  backend-independent retry system (soft/hard failure envelopes).
- GCS document resolver, runtime worker scripts, and a synthetic example
  pipeline.
