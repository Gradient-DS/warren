# Warren

[![tests](https://github.com/Gradient-DS/warren/actions/workflows/tests.yml/badge.svg)](https://github.com/Gradient-DS/warren/actions/workflows/tests.yml) [![PyPI version](https://img.shields.io/pypi/v/warren)](https://pypi.org/project/warren/) [![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)

Warren is a message-driven document processing framework. You define a pipeline as a set of worker types, each consuming messages from a shared fanout exchange (RabbitMQ) or topic (Kafka) and **self-selecting** which messages to process. Workers run as independent processes — you scale by adding replicas of any worker type. The backend is selected by `backend:` in your `RuntimeConfig` YAML (`rabbitmq` by default, or `kafka` for fanout pipelines); nothing else changes.

A typical flow: a job enters the pipeline as a message on the fanout exchange. Every worker type receives a copy in its own queue, but only processes the messages relevant to it — each worker's `should_process()` decides whether to act or discard. When a worker processes a message, it writes its results to a cached storage layer (MongoDB + Redis), then publishes a new message describing the *location* of those results. Downstream workers pick that up, fetch what they need from storage, and publish their own result locations. Adding a new worker type is purely additive — no routing configuration changes, no upstream modifications.

Warren separates the **framework** (worker base classes, storage interfaces, pubsub abstractions — transport-agnostic) from the **runtime** (concrete wiring for RabbitMQ or Kafka + MongoDB + Redis, shipped in `warren/runtime/`).

## Installation

The transport backends and cloud storage are optional extras — install the ones you use. The Quickstart below runs on RabbitMQ, so install the `rmq` extra:

```bash
pip install "warren[rmq]"
```

Use `warren[kafka]` to run on Kafka instead. Document resolvers are extras too: `warren[gcs]` (Google Cloud Storage), `warren[s3]` (Amazon S3 / S3-compatible), and `warren[http]` (plain HTTP(S) URLs, e.g. presigned GET links). Selecting a backend or resolver without its extra raises a clear `OptionalDependencyError`.

Requires Python 3.12+. For development:

```bash
git clone https://github.com/Gradient-DS/warren.git
cd warren
pip install -e ".[dev,rmq,kafka]"
```

## Quickstart — the synthetic fanout pipeline

`examples/exchanges/fanout/` is a minimal three-stage pipeline (parse → chunk → embed) over synthetic pre-baked data: 4 stand-in documents produce 18 chunks and 18 embeddings. No external data dependencies — just local infrastructure. (It's one of three sibling examples under `examples/exchanges/`, one per exchange type — see [Choosing an exchange](#choosing-an-exchange).)

**1. Start RabbitMQ, MongoDB, and Redis** (e.g. via Docker):

```bash
docker run -d --name warren-rabbitmq -p 5672:5672 rabbitmq:4
docker run -d --name warren-mongodb -p 27017:27017 mongo:8
docker run -d --name warren-redis -p 6379:6379 redis:7
```

**2. Start the three workers** (one terminal each, from the repo root):

```bash
python -m runtime_scripts.start_worker --pipeline-spec ./examples/exchanges/fanout --worker-type document_parser --config-file examples/exchanges/fanout/config.yaml
python -m runtime_scripts.start_worker --pipeline-spec ./examples/exchanges/fanout --worker-type text_chunker --config-file examples/exchanges/fanout/config.yaml
python -m runtime_scripts.start_worker --pipeline-spec ./examples/exchanges/fanout --worker-type embedding_generator --config-file examples/exchanges/fanout/config.yaml
```

Optionally also start the support workers (job completion tracking and retry management). They take `--pipeline-spec` too, so they can resolve which exchange to observe:

```bash
python -m runtime_scripts.start_job_status_worker --pipeline-spec ./examples/exchanges/fanout --config-file examples/exchanges/fanout/config.yaml
python -m runtime_scripts.start_retry_worker --pipeline-spec ./examples/exchanges/fanout --config-file examples/exchanges/fanout/config.yaml
```

**3. Publish the documents:**

```bash
python -m examples.exchanges.publish --job-name demo-001 --config-file examples/exchanges/fanout/config.yaml
```

Watch the worker terminals: the parser picks up the documents, the chunker picks up the parsed results, the embedder picks up the chunks. Results land in MongoDB collections `parsed_documents`, `chunks`, and `embeddings` (database `warren_fanout`, per the example config).

**4. Watch the run (optional).** With a job-status worker running (the support worker above), `inspect_job` polls the job by name and prints a live per-stage view until it completes:

```bash
python -m examples.inspect_job --job-name demo-001 --config-file examples/exchanges/fanout/config.yaml
```

```
  stage                   total     ok   soft   hard
  ---------------------- ------ ------ ------ ------
  embedded_document           4      4      0      0
  ...
  state: COMPLETED
```

## Get started for real — PDFs to embeddings

The quickstart proves the plumbing with synthetic data. `examples/rag/` does
**real work**: it downloads real PDFs, extracts their text with
[`pypdf`](https://pypi.org/project/pypdf/), splits it into chunks, and embeds
each chunk with the OpenAI API — the first three stages of a RAG pipeline. You
bring your own `OPENAI_API_KEY`. It defaults to two arXiv papers and takes your
own with `--url`.

It runs on a **fanout** exchange (every worker self-selects), like the
quickstart — what's new is that the work is real: the parser **downloads** each
PDF over HTTP, and a real embedding API whose transient errors flow through
Warren's retry path.

**1. Install the example extras** (real PDF + OpenAI clients, not needed by the
framework itself) and start the same infrastructure as the quickstart:

```bash
pip install -e .[examples]      # or: pip install 'warren[examples]'
export OPENAI_API_KEY=sk-...
```

**2. Start the three workers** (one terminal each) plus the support workers so
you can watch progress:

```bash
python -m runtime_scripts.start_worker --pipeline-spec ./examples/rag --worker-type pdf_parser --config-file examples/rag/config.yaml
python -m runtime_scripts.start_worker --pipeline-spec ./examples/rag --worker-type text_chunker --config-file examples/rag/config.yaml
python -m runtime_scripts.start_worker --pipeline-spec ./examples/rag --worker-type embedding_generator --config-file examples/rag/config.yaml
python -m runtime_scripts.start_job_status_worker --pipeline-spec ./examples/rag --config-file examples/rag/config.yaml
```

`OPENAI_API_KEY` only needs to be set for the **embedding** worker's terminal —
the parser and chunker don't call OpenAI.

**3. Publish the PDFs** (defaults to two arXiv papers; add your own with `--url`):

```bash
python -m examples.rag.publish_jobs --job-name rag-001 --config-file examples/rag/config.yaml
```

The publisher sends one small message per PDF carrying just its *URL* — the
parser worker downloads and parses each one. Results land in the
`parsed_documents`, `chunks`, and `embeddings` collections of the `warren_rag`
database.

**4. Watch it run:**

```bash
python -m examples.inspect_job --job-name rag-001 --config-file examples/rag/config.yaml
```

To embed your own corpus, pass `--url` (repeatable). The chunk size and
embedding model are constants at the top of `examples/rag/workers/` — tune them
for your documents.

### Running on Kafka instead

A fanout pipeline runs on Kafka with zero code changes — just point every command at `examples/exchanges/fanout/config.kafka.yaml` instead of `config.yaml`, and start a Kafka broker (e.g. `localhost:9092`) in place of RabbitMQ. (Kafka supports fanout pipelines only; `topic`/`direct` routing is RabbitMQ-only for now.) The Kafka config has `backend: kafka`, a `jobs` topic with `create_if_missing: true`, and the same MongoDB/Redis/retry sections. See [`warren/docs/kafka.md`](warren/docs/kafka.md) for the full RabbitMQ→Kafka semantic mapping.

## Defining your own pipeline

A pipeline is a directory with a `pipeline_spec.py` (exporting a `PIPELINE: PipelineSpec`) and a `config.yaml` (a `RuntimeConfig`). Each worker module owns a `create(ctx: WorkerFactoryContext)` factory; the spec references factories via lazy-import wrappers so different deployment images only load the dependencies they need.

The `PipelineSpec` also defines the **exchange** (topology) and how each worker is wired to it: an optional `binding_key` and a `publish` route (`config.yaml` holds only per-environment infra — broker/Mongo/Redis hosts, credentials, prefetch). A pipeline uses exactly one exchange, and its type is the main routing decision you make.

### Choosing an exchange

Start with **fanout** — it's the simplest and covers most pipelines. Reach for `topic` or `direct` only when a concrete need below appears:

- **`fanout` — a pipeline where every worker self-selects.** Every worker receives every message and decides via `should_process` whether to act. Best when your stages form a straight line (or a fan-out where several *independent* workers should each react to the same event — embed *and* classify *and* extract entities). Adding a stage is purely additive: drop in a worker, change no routing. The cost is that every worker sees every message and discards what isn't for it — fine until that volume hurts.
  *Use it when:* "I just want a pipeline, and adding a worker shouldn't touch any routing."

- **`topic` — heterogeneous inputs routed by *kind*.** The broker routes each message by a key (`data_type` by convention) to only the workers that bind it. Best when inputs are mixed and different kinds need different workers: PDFs → a PDF parser, HTML → an OCR worker, scanned images → something else. The broker does the filtering, so a worker never wakes up for a message it would only discard.
  *Use it when:* "My documents aren't all the same, and routing by content type keeps each worker focused."

- **`direct` + a job-defined `RoutingPlan` — different jobs, different paths.** Workers declare what they `accepts`/`produces` (`CapabilityWorkerBase`) and bind their own id on a `direct` exchange. Each *job* ships a `RoutingPlan` in `job_parameters` that names the path through the **same** deployed workers — one job runs parse → chunk → embed, another runs parse → chunk → summarise — and the plan is validated against the workers' capabilities before publishing (`validate_routing_plan`).
  *Use it when:* "The set of workers is fixed, but each submission needs a different route through them."

Each has a runnable example. The three `examples/exchanges/` siblings are the **same pipeline wired three ways** (synthetic data, so the routing is what stands out); `examples/rag/` is the real, end-to-end one:

| Example | Exchange | The scenario it shows |
|---------|----------|-----------------------|
| `examples/rag/` | `fanout` | **Real** PDFs → chunks → embeddings (BYO OpenAI key) — the linear, additive case. |
| `examples/exchanges/fanout/` | `fanout` | The same shape on synthetic data — the zero-dependency quickstart. |
| `examples/exchanges/topic/` | `topic` | Broker routes by `data_type`; workers bind the type they consume. |
| `examples/exchanges/direct/` | `direct` | Capability workers + a per-job `RoutingPlan` choosing the path. |

(The `exchanges/` examples reuse synthetic workers to keep the routing mechanism front and centre; swap in the `examples/rag/` workers to make them do real work.)

See [`warren/docs/routing.md`](warren/docs/routing.md) for the full routing model and design decisions.

**Read [`warren/runtime/USAGE.md`](warren/runtime/USAGE.md)** — the full usage guide: core concepts (`PipelineSpec`, `WorkerSpec`, `WorkerFactoryContext`, `RuntimeConfig`, `DefaultWorkerRunner`), the launcher scripts, custom runners, and recommended project layout.

Deeper design docs live in [`warren/docs/`](warren/docs/): workers, storage and caching, document store, RabbitMQ and Kafka topology, results store, and the retry system.

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
pip install -e ".[dev,rmq,kafka]"
python -m pytest tests -q
ruff check . && ruff format --check .
```

## Contributing

Contributions are welcome — see [CONTRIBUTING.md](CONTRIBUTING.md). To report a security issue, follow [SECURITY.md](SECURITY.md) (never a public issue).

## License

Apache-2.0 — see [LICENSE](LICENSE).
