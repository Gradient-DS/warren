# CLAUDE.md

Warren is an open-source (PyPI, Apache-2.0) message-driven document processing framework: self-selecting workers on a RabbitMQ or Kafka backend with MongoDB/Redis-backed storage. **Deliberately generic — no soev-specific logic belongs here.** The framework (worker base classes, storage interfaces, pubsub abstractions) is transport-agnostic; concrete wiring lives in `warren/runtime/`.

**Fit in the system:** the soev document pipeline built on warren lives in `soev-solutions` (repo `genai-utils`); the message schema defined here is the pipeline contract. Company context, cross-repo architecture, and the decision log live in the sibling `soev-docs` repo (`../soev-docs`, or `$SOEV_DOCS_PATH`, or `~/gradient/soev-docs`) — start at `architecture/repo-map.md`.

## Commands

```bash
pip install -e ".[dev,rmq,kafka]"   # Dev install
pytest                              # Tests
ruff check && ruff format           # Lint + format (config in ruff.toml)
```

The quickstart pipeline (`examples/fake/`, parse → chunk → embed over synthetic data) needs local RabbitMQ + MongoDB + Redis — see README for the docker commands and `runtime_scripts.start_worker` invocations.

## Structure

- `warren/` — framework + `warren/runtime/` (RabbitMQ/Kafka + MongoDB + Redis wiring)
- `examples/fake/` — minimal three-stage reference pipeline
- `runtime_scripts/` — worker entry points
- Transport backends and document resolvers are optional extras (`rmq`, `kafka`, `gcs`, `s3`, `http`); a missing extra raises `OptionalDependencyError`.

## Releasing

`main` only; publishes to PyPI on GitHub Release.

## Conventions

- Python 3.12+, ruff, type hints. Adding a worker type is purely additive — no routing changes, no upstream modifications.
- Keep it generic: if a change encodes soev/product behavior, it belongs in `soev-solutions`, not here.
- Claude scratch space (`thoughts/`, `plans/`, `.superpowers/`, `CLAUDE.local.md`) and personal `.claude/` workflow files are git-ignored — see `soev-docs/claude-code.md`.
