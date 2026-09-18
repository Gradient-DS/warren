# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
