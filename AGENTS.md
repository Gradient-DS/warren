# AGENTS.md

Warren is a message-driven distributed processing framework: self-selecting
workers on a RabbitMQ or Kafka backend, a claim-check data plane (messages
carry references, MongoDB/Redis-backed stores carry payloads), bounded jobs
with a per-item, per-stage status ledger, and a backend-independent retry
system. Published to PyPI as `warren` (Apache-2.0).

## Commands

```bash
pip install -e ".[dev,rmq,kafka]"   # Dev install
pytest                              # Tests (no infrastructure needed)
ruff check && ruff format           # Lint + format (config in ruff.toml)
```

The runnable examples need local RabbitMQ + MongoDB + Redis — the README
Quickstart has the docker commands and worker invocations.

## Structure

- `warren/` — the framework: worker base classes, pubsub abstractions, storage
  interfaces. `warren/runtime/` holds the concrete wiring (RabbitMQ/Kafka +
  MongoDB + Redis) behind `RuntimeConfig`.
- `warren/docs/` — design docs per subsystem (routing, workers, retry design,
  RabbitMQ, Kafka, stores).
- `runtime_scripts/` — worker entry points (`start_worker`,
  `start_job_status_worker`, `start_retry_worker`, …).
- `examples/exchanges/{fanout,topic,direct}` — the same synthetic three-stage
  pipeline wired onto each exchange type; `fanout` is the Quickstart.
- `examples/rag/` — a real workload: PDFs → text → chunks → OpenAI embeddings.
- `tests/` — pure-Python test suite; transport parity is tested without a
  broker.

Transport backends and document resolvers are optional extras (`rmq`, `kafka`,
`gcs`, `s3`, `http`); using a missing extra raises `OptionalDependencyError`.

## Conventions

- Python 3.12+, ruff (lint + format), type hints throughout.
- The framework is application-agnostic: workers pass generic dict payloads
  and the framework never assumes a document type or product domain. Don't
  add application-specific logic or docstrings to framework interfaces.
- Adding a worker type is purely additive — no routing changes, no upstream
  modifications.
- Program to abstractions (Protocols/ABCs), inject dependencies, compose over
  inherit, isolate I/O at boundaries.
- Kafka is fanout-only by design for now; `topic`/`direct` fail fast at
  startup (see ROADMAP.md).

## Releasing

Work lands on `main` via PRs. Publishing to PyPI is triggered by a GitHub
Release (not by pushing a tag). Update CHANGELOG.md as part of any
user-visible change.

The version in `pyproject.toml` always matches the last published release;
between releases, new work accumulates under `[Unreleased]` in CHANGELOG.md.
Cutting a release is a single PR that bumps the version and dates the
changelog heading, followed by a tag + GitHub Release on the merge commit.
