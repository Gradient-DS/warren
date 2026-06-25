# Warren Runtime — Usage Guide

## What is Warren?

Warren is a message-driven document processing framework. You define a pipeline as a set of worker types, each consuming messages from a shared RabbitMQ fanout exchange and self-selecting which messages to process. Workers run as independent processes — you scale by adding replicas of any worker type.

A typical flow: a job enters the pipeline as a message published to a fanout exchange. Every worker type receives a copy of every message in its own queue, but only processes the ones relevant to it — each worker's `should_process()` decides whether to act or discard. When a worker processes a message, it writes its results to a cached storage layer (MongoDB + Redis), then publishes a new message to the same exchange describing the *location* of those results. The cycle repeats: downstream workers pick up the new message, fetch the results they need from storage, process them, and publish their own result locations. This self-selection model means adding a new worker type is purely additive — no routing configuration changes, no upstream modifications.

More directed graph topologies (topic exchanges, per-stage routing) are possible with Warren's pubsub abstractions, but the default runtime and launcher scripts assume the fanout approach described above.

Warren separates the **framework** (worker base classes, storage interfaces, pubsub abstractions) from the **runtime** (concrete infrastructure wiring for RabbitMQ, MongoDB, Redis). The framework interfaces are transport-agnostic; the runtime binds them to a specific stack.

### Infrastructure assumptions

The runtime provided in `warren/runtime/` uses:

- **RabbitMQ** (via `aio_pika`) for message passing — fanout exchange, per-worker-type queues
- **MongoDB** (via `pymongo.AsyncMongoClient`) for document storage, job tracking, results, and publishing tracker
- **Redis** (via `redis.asyncio`) for caching (results, document bytes)

The warren library's abstract interfaces (`ConsumerManagerInterface`, `PublisherInterface`, `ResultsStoreInterface`, etc.) don't depend on these choices. A different runtime could bind them to Kafka, PostgreSQL, or any other stack. But the runtime shipped today — and all the launcher scripts in `runtime_scripts/` — assume this RabbitMQ + MongoDB + Redis combination.

## Core concepts

### PipelineSpec

A `PipelineSpec` declares the full pipeline composition: which worker types exist, what stores they use, and how completion is detected. It's a frozen dataclass — pure data, no behavior.

```python
PipelineSpec(
    exchange=RMQExchangeConfig(name="jobs", type="fanout"),
    workers={"parser": WorkerSpec(...), "chunker": WorkerSpec(...), ...},
    result_collections=["parsed_documents", "chunks", "embeddings"],
    reference_collection="chunks",
    completion_collection="embeddings",
    final_data_type="embedded_document",
)
```

- `exchange` is the single `RMQExchangeConfig` (`type` is `fanout`, `topic`, or `direct`) all workers consume from and publish to. It's pipeline topology, so it lives here in the spec — `config.yaml` holds only per-environment infra (hosts, credentials, prefetch). The support workers (job-status, retry, publication) observe this exchange; they require it to be `fanout` or `topic`. Multi-exchange deployments are deferred — see [`warren/docs/routing.md`](../docs/routing.md).
- `workers` maps a worker type name (string) to its `WorkerSpec`. The type name determines the RabbitMQ queue name (`{exchange}.{worker_type}`).
- `final_data_type` is the message type that signals a document is fully processed. The `JobStatusWorker` uses this to detect completion and mark jobs as done in the job store.
- `result_collections`, `reference_collection`, `completion_collection` — currently used only by the E2E test's `check_completion.py` polling script (count-comparison heuristic). Not used by any framework component. These should be moved out of `PipelineSpec` into E2E-specific config in a future cleanup (see TODOs).

### WorkerSpec

A `WorkerSpec` describes one worker type — what it needs and how to build it:

```python
WorkerSpec(
    collections={"read": "chunks", "write": "embeddings"},
    factory=create_embedder,
    binding_key=None,       # required for topic/direct, None for fanout
    publish=PublishSpec(),  # None = no downstream data (terminal)
    needs_document_fetcher=False,
    needs_document_store=False,
)
```

- `collections` maps roles to MongoDB collection names. Workers typically have a "read" collection (upstream results to consume) and a "write" collection (where this worker stores its own processing results). The runner creates a `DefaultResultsStore` per role and passes them to the factory via `ctx.stores["read"]`, `ctx.stores["write"]`, etc. Some workers have additional roles (e.g. the embedder reads from "chunks", "summaries", and "item_metadata").
- `factory` is an async callable `(WorkerFactoryContext) -> MessageConsumerInterface`. It creates and returns the worker instance. See [Defining a pipeline](#defining-a-pipeline).
- `binding_key` is the queue's binding pattern on the pipeline exchange. It must be `None` on a `fanout` exchange (which ignores keys) and is required on `topic`/`direct` exchanges (e.g. a `data_type` like `"markdown_document"`, or a `topic` wildcard like `"document.*"`).
- `publish` is a `PublishSpec(route=None, route_func=None)` describing how the worker publishes its result to the pipeline exchange, or `None` if it publishes nothing downstream (terminal — there is no separate `terminal` flag). On `fanout`, leave `route`/`route_func` unset; on `topic`/`direct`, set one (e.g. `route_func=MessageFieldRouter()` to route by `data_type`).
- `needs_document_fetcher` — if `True`, the runner builds a `CachedDocumentFetcher` (with path and GCS resolvers) and passes it as `ctx.get_document_func`. (Note: there is an open design question about whether this should be the factory's responsibility instead of the runner's — see TODOs.)
- `needs_document_store` — if `True`, the runner creates a `MongoDBDocumentStore` on the `documents` collection and passes it as `ctx.document_store`. (Same design note as above.)

### WorkerFactoryContext

The runner creates a `WorkerFactoryContext` and passes it to each factory function. It bundles everything the factory might need:

```python
@dataclass(frozen=True)
class WorkerFactoryContext:
    worker_name: str                              # unique instance name
    stores: dict[str, ResultsStoreInterface]      # pre-built stores per role
    mongo_client: AsyncMongoClient                # for creating additional stores
    redis_client: Redis                           # for creating additional stores
    database_name: str                            # MongoDB database name
    get_document_func: GetDocumentFunc | None      # when needs_document_fetcher=True
    document_store: DocumentStoreInterface | None  # when needs_document_store=True
```

The `mongo_client`, `redis_client`, and `database_name` fields are passed through so factories can construct specialised stores (e.g. `BinaryResultsStore` for raw HTML bytes) beyond the standard `DefaultResultsStore` instances that the runner creates from `collections`.

### RuntimeConfig

Infrastructure settings loaded from YAML:

```yaml
rabbitmq:
  # Exchange definitions live in the pipeline spec (topology), not here.
  connection:
    host: localhost
    port: 5672
    login: guest
    password: guest
  consumer:
    prefetch_count: 4
    on_shutdown_timeout: 30.0
  retry:
    enabled: true
    collection_name: retries

mongodb:
  host: localhost
  port: 27017
  database: my_pipeline

redis:
  host: localhost
  port: 6379
```

All fields have sensible defaults. Load via `RuntimeConfig.from_yaml("config.yaml")`.

Note: MongoDB and Redis currently only accept `host`/`port` pairs. Connection string support (`mongodb://...`, `redis://...`) is planned but not yet implemented.

**Three reuse modes:**

1. **Defaults + YAML override** (most common) — start from defaults, override what you need in the YAML file. Fields you omit keep their defaults.
2. **Programmatic override** — construct `RuntimeConfig(rabbitmq=..., mongodb=...)` directly in Python. Useful for tests or embedded use.
3. **Subclass** — extend `RuntimeConfig` with additional fields for your deployment. The YAML loader (`model_validate`) ignores unknown fields by default.

### DefaultWorkerRunner

The runner that wires everything together. Given a `RuntimeConfig` and a `WorkerSpec`, it:

1. Creates infrastructure connections (MongoDB, Redis, RabbitMQ)
2. Builds `ResultsStoreInterface` instances from `collections`
3. Optionally creates a `CachedDocumentFetcher` and/or `DocumentStoreInterface`
4. Calls the factory function with a `WorkerFactoryContext`
5. Creates the worker's RMQ publisher (none if `publish` is `None`) and a consumer manager bound to the pipeline exchange with `binding_key`
6. Runs the consumer until `SIGINT`/`SIGTERM`
7. Tears down everything on shutdown

You rarely instantiate `DefaultWorkerRunner` directly — the launcher scripts handle that. But understanding what it does helps when debugging or writing custom runners.

## Defining a pipeline

### Convention

A pipeline lives in a directory with at least two files:

```
my_pipeline/
    pipeline_spec.py    # PipelineSpec definition
    config.yaml         # RuntimeConfig for this deployment
```

`pipeline_spec.py` exports a `PIPELINE` variable of type `PipelineSpec`. The launcher scripts find it by convention (the variable name `PIPELINE` and the filename `pipeline_spec.py` are both defaults that can be overridden via CLI flags).

### Factory functions

Each worker module owns a `create(ctx: WorkerFactoryContext)` factory function. This keeps construction logic next to the worker class and avoids a monolithic spec file:

```python
# my_pipeline/workers/parser_worker.py

class ParserWorker(FilteringWorkerBase):
    """The worker class — processing logic."""
    ...


async def create(ctx: WorkerFactoryContext) -> ParserWorker:
    """Factory for the pipeline spec."""
    from my_pipeline.processors import HtmlProcessor, PdfProcessor

    html_processor = HtmlProcessor(default_backend="trafilatura")
    pdf_processor = PdfProcessor()

    return ParserWorker(
        worker_name=ctx.worker_name,
        processors=[html_processor, pdf_processor],
        get_document_func=ctx.get_document_func,
        write_store=ctx.stores["write"],
    )
```

The pipeline spec then references these factories via lazy-import wrappers:

```python
# my_pipeline/pipeline_spec.py

from warren.runtime import PipelineSpec, PublishSpec, WorkerFactoryContext, WorkerSpec
from warren.common import MessageConsumerInterface
from warren.pubsub.rabbitmq.config import RMQExchangeConfig

async def _create_parser(ctx: WorkerFactoryContext) -> MessageConsumerInterface:
    from my_pipeline.workers.parser_worker import create
    return await create(ctx)

PIPELINE = PipelineSpec(
    exchange=RMQExchangeConfig(name="jobs", type="fanout"),
    workers={
        "parser": WorkerSpec(
            collections={"write": "parsed_documents"},
            factory=_create_parser,
            publish=PublishSpec(),
            needs_document_fetcher=True,
        ),
        ...
    },
    ...
)
```

**Why lazy wrappers?** Different Docker images install different pip packages. A parser image has `pymupdf` but not `kubernetes`; a vectordb-provisioning image has `kubernetes` but not `pymupdf`. If the spec imported all worker modules at module level, every image would need every dependency. Lazy wrappers ensure a worker module only loads when that worker type actually starts.

### Environment-driven configuration

Workers that need external services (LLM endpoints, API keys, K8s parameters) read from environment variables inside their factory functions — not from `RuntimeConfig`. This keeps `RuntimeConfig` focused on infrastructure and lets Helm/Docker inject per-pod configuration:

```python
async def create(ctx: WorkerFactoryContext) -> EmbedderWorker:
    import os
    api_key = os.environ["OPENAI_API_KEY"]
    ...
```

## Running workers

Warren provides launcher scripts in `runtime_scripts/`. These handle config loading, worker name generation, error handling, and the setup/run/teardown lifecycle.

### Standard workers: `start_worker.py`

Starts any worker type defined in a `PipelineSpec`:

```bash
# From the project root:
python -m runtime_scripts.start_worker \
    --pipeline-spec ./my_pipeline \
    --worker-type parser \
    --config-file ./my_pipeline/config.yaml \
    --worker-name parser-1
```

`--pipeline-spec` resolution is flexible:
- **Directory** — looks for `pipeline_spec.py` inside, loads `PIPELINE`
- **File** — loads the file, uses `PIPELINE` (or append `:VAR_NAME`)
- **Dotted module** — `my.module` or `my.module:CUSTOM_VAR`
- **Omitted** — defaults to `./pipeline/pipeline_spec.py:PIPELINE`

`--worker-type` must match a key in `PipelineSpec.workers`.

`--worker-name` is optional — auto-generates `{worker_type}-{uuid8}` if omitted.

### Publication worker: `start_job_publication_worker.py`

The publication worker is special: it consumes job-submission messages that describe which documents to process (metadata, URLs, file paths). For each document in the submission, the publication worker loads and caches the document bytes, registers it in the document store (assigning a stable `doc_id` and recording its storage location), and publishes a message to the exchange with the document ID so that downstream workers can find and load the document for processing. Because the loading, registration, and message format are application-specific (file vs. URL, metadata schemas, adapter logic), it takes a `--publisher-factory` flag:

```bash
python -m runtime_scripts.start_job_publication_worker \
    --publisher-factory my_pipeline.publishers.factory:create_multi_type_publisher \
    --config-file ./my_pipeline/config.yaml
```

The factory function must match the `DocumentsPublisherFactoryFunc` protocol — it receives `(publisher, infra, config, worker_name)` and returns a `JobDocumentsPublisher`. See `warren/jobs/publishing/job_publication_worker_runner.py` for details.

### Support workers

These don't process documents — they support the pipeline:

```bash
# Job status tracking (completion detection, progress bars)
python -m runtime_scripts.start_job_status_worker \
    --config-file ./my_pipeline/config.yaml

# Retry management (soft-failure re-delivery with backoff)
python -m runtime_scripts.start_retry_worker \
    --config-file ./my_pipeline/config.yaml
```

Both use the same `--config-file` and `--worker-name` flags as `start_worker.py`.

### All launchers share

- `--config-file` — path to `RuntimeConfig` YAML
- `--worker-name` — unique instance name (auto-generated if omitted)
- `--debug` — enables DEBUG logging (default: INFO)
- Three-phase error handling with `summarize_exception_chain`
- Graceful shutdown on `SIGINT`/`SIGTERM`

## Custom runners

`DefaultWorkerRunner` handles the common case. When it doesn't fit, you have two options:

### Subclass with hooks

`DefaultWorkerRunner` exposes two override hooks:

```python
class MyRunner(DefaultWorkerRunner):
    def _create_resolvers(self):
        """Add custom document resolvers (e.g. S3)."""
        resolvers = super()._create_resolvers()
        resolvers["s3"] = my_s3_resolver
        return resolvers

    def _wrap_worker(self, worker, spec):
        """Intercept the worker after factory creation (e.g. for instrumentation)."""
        return MyInstrumentationWrapper(worker)
```

Use via the `--runner` flag:

```bash
python -m runtime_scripts.start_worker \
    --pipeline-spec ./my_pipeline \
    --worker-type parser \
    --runner my_pipeline.runners:MyRunner
```

### Fully custom runner

For workers that don't fit the `DefaultWorkerRunner` model (different lifecycle, different infrastructure), subclass `WorkerRunnerBase` directly and write your own launcher script. The `JobPublicationWorkerRunner` is an example — it manages its own publisher creation lifecycle.

## Project layout

```
my_project/
├── warren/                     # Framework library
│   ├── common.py               # Shared types, exceptions
│   ├── workers/                # Worker base classes, runners
│   ├── pubsub/                 # Transport abstractions + RabbitMQ impl
│   ├── storage/                # Storage abstractions + MongoDB/Redis impl
│   ├── jobs/                   # Job publication, status tracking
│   ├── retry_management/       # Retry worker + runner
│   ├── runtime/                # RuntimeConfig, DefaultWorkerRunner, PipelineSpec
│   └── exceptions.py           # WarrenError hierarchy
│
├── runtime_scripts/            # Launcher scripts (NOT in the warren package)
│   ├── lib/                    # Shared launcher utilities
│   ├── start_worker.py         # Generic worker launcher
│   ├── start_job_publication_worker.py
│   ├── start_job_status_worker.py
│   ├── start_retry_worker.py
│   └── purge_queues.py         # Queue cleanup utility
│
├── my_pipeline/                # Your application pipeline
│   ├── pipeline_spec.py        # PipelineSpec definition
│   ├── config.yaml             # RuntimeConfig for this deployment
│   ├── workers/                # Worker classes + create() factories
│   ├── processors/             # Processing logic (parsing, chunking, etc.)
│   └── publishers/             # Publication worker factory
│
└── e2e_test/                   # End-to-end test scenarios
    ├── fake/                   # Synthetic data, fast iteration
    └── real/                   # Real documents, GCS integration
```

**Why are `runtime_scripts/` separate from `warren/`?** The scripts are deployment artifacts, not library code. They depend on warren but aren't part of its API. When warren becomes its own pip package, consumers clone the scripts and adapt them — they're templates, not imports.

**Why is the pipeline separate from warren?** Warren is the framework; a pipeline is an application of it. Warren provides `FilteringWorkerBase`, `DefaultWorkerRunner`, `PipelineSpec` and the infrastructure wiring. Your pipeline provides the workers, processors, and factories that do the actual document processing. Multiple independent pipelines can use the same warren library, each with their own worker types, stores, and deployment configuration.
