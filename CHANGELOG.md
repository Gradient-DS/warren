# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
