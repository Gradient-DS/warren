# Warren

[![tests](https://github.com/Gradient-DS/warren/actions/workflows/tests.yml/badge.svg)](https://github.com/Gradient-DS/warren/actions/workflows/tests.yml) [![PyPI version](https://img.shields.io/pypi/v/warren)](https://pypi.org/project/warren/) [![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)

Warren is a message-driven document processing framework. You define a pipeline as a set of worker types, each consuming messages from a shared RabbitMQ fanout exchange and **self-selecting** which messages to process. Workers run as independent processes — you scale by adding replicas of any worker type.

A typical flow: a job enters the pipeline as a message on the fanout exchange. Every worker type receives a copy in its own queue, but only processes the messages relevant to it — each worker's `should_process()` decides whether to act or discard. When a worker processes a message, it writes its results to a cached storage layer (MongoDB + Redis), then publishes a new message describing the *location* of those results. Downstream workers pick that up, fetch what they need from storage, and publish their own result locations. Adding a new worker type is purely additive — no routing configuration changes, no upstream modifications.

Warren separates the **framework** (worker base classes, storage interfaces, pubsub abstractions — transport-agnostic) from the **runtime** (concrete wiring for RabbitMQ + MongoDB + Redis, shipped in `warren/runtime/`).

## Installation

```bash
pip install warren
```

Requires Python 3.12+. For development:

```bash
git clone https://github.com/Gradient-DS/warren.git
cd warren
pip install -e .[dev]
```

## Quickstart — the fake example pipeline

`examples/fake/` is a minimal three-stage pipeline (parse → chunk → embed) over synthetic pre-baked data: 4 fake documents produce 18 chunks and 18 embeddings. No external data dependencies — just local infrastructure.

**1. Start RabbitMQ, MongoDB, and Redis** (e.g. via Docker):

```bash
docker run -d --name warren-rabbitmq -p 5672:5672 rabbitmq:4
docker run -d --name warren-mongodb -p 27017:27017 mongo:8
docker run -d --name warren-redis -p 6379:6379 redis:7
```

**2. Start the three workers** (one terminal each, from the repo root):

```bash
python -m runtime_scripts.start_worker --pipeline-spec ./examples/fake --worker-type document_parser --config-file examples/fake/config.yaml
python -m runtime_scripts.start_worker --pipeline-spec ./examples/fake --worker-type text_chunker --config-file examples/fake/config.yaml
python -m runtime_scripts.start_worker --pipeline-spec ./examples/fake --worker-type embedding_generator --config-file examples/fake/config.yaml
```

Optionally also start the support workers (job completion tracking and retry management). They take `--pipeline-spec` too, so they can resolve which exchange to observe:

```bash
python -m runtime_scripts.start_job_status_worker --pipeline-spec ./examples/fake --config-file examples/fake/config.yaml
python -m runtime_scripts.start_retry_worker --pipeline-spec ./examples/fake --config-file examples/fake/config.yaml
```

**3. Publish the fake documents:**

```bash
python -m examples.fake.publish_jobs --job-name demo-001 --config-file examples/fake/config.yaml
```

Watch the worker terminals: the parser picks up the documents, the chunker picks up the parsed results, the embedder picks up the chunks. Results land in MongoDB collections `parsed_documents`, `chunks`, and `embeddings` (database `e2e_test`, per the example config).

## Defining your own pipeline

A pipeline is a directory with a `pipeline_spec.py` (exporting a `PIPELINE: PipelineSpec`) and a `config.yaml` (a `RuntimeConfig`). Each worker module owns a `create(ctx: WorkerFactoryContext)` factory; the spec references factories via lazy-import wrappers so different deployment images only load the dependencies they need.

The `PipelineSpec` also defines the **exchanges** (topology) and how each worker is wired to them: `consume_exchange`, an optional `binding_key`, and a list of `publish` targets (`config.yaml` holds only per-environment infra — broker/Mongo/Redis hosts, credentials, prefetch). A `fanout` exchange broadcasts to every worker (each self-selects via `should_process`); a `topic` or `direct` exchange lets the broker route by routing key.

Three runnable examples show the spread:

- `examples/fake/` — **fanout**: every worker sees every message, self-selects.
- `examples/topic/` — **topic**: the broker routes by `data_type` (workers bind the type they consume).
- `examples/routed/` — **direct + job-defined routing**: workers are `CapabilityWorkerBase` (declare `accepts`/`produces`); each *job* ships a `RoutingPlan` in `job_parameters` that decides the path through the same deployed workers. The plan is validated against the workers' declared capabilities before publishing (`validate_routing_plan`).

**Read [`warren/runtime/USAGE.md`](warren/runtime/USAGE.md)** — the full usage guide: core concepts (`PipelineSpec`, `WorkerSpec`, `WorkerFactoryContext`, `RuntimeConfig`, `DefaultWorkerRunner`), the launcher scripts, custom runners, and recommended project layout.

Deeper design docs live in [`warren/docs/`](warren/docs/): workers, storage and caching, document store, RabbitMQ topology, results store, and the retry system.

## Launchers

`runtime_scripts/` ships the process launchers, also installed as console scripts:

| Console script | Module | Purpose |
|---|---|---|
| `warren-worker` | `runtime_scripts.start_worker` | Any worker type from a `PipelineSpec` |
| `warren-job-publication-worker` | `runtime_scripts.start_job_publication_worker` | Job submission → per-document publication |
| `warren-job-status-worker` | `runtime_scripts.start_job_status_worker` | Completion detection, progress tracking |
| `warren-retry-worker` | `runtime_scripts.start_retry_worker` | Soft-failure re-delivery with backoff |
| `warren-purge-queues` | `runtime_scripts.purge_queues` | Queue/exchange cleanup between runs |

## Development

```bash
pip install -e .[dev]
python -m pytest tests -q
ruff check . && ruff format --check .
```

## Contributing

Contributions are welcome — see [CONTRIBUTING.md](CONTRIBUTING.md). To report a security issue, follow [SECURITY.md](SECURITY.md) (never a public issue).

## License

Apache-2.0 — see [LICENSE](LICENSE).
