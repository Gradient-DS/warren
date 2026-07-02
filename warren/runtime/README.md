# warren/runtime/

Concrete runtime layer for the distributed processing framework. Turns a
`PipelineSpec` into running worker processes.

## Technology assumptions

This runtime is built on a specific infrastructure stack:

- **RabbitMQ** (via `aio_pika`) or **Kafka** (via `aiokafka`) for message
  passing, selected by `backend:` in the config. RabbitMQ supports all
  exchange types; Kafka supports fanout pipelines only (see
  [`warren/docs/kafka.md`](../docs/kafka.md)).
- **MongoDB** for document storage, job tracking, and results
- **Redis** for caching (results, document bytes)

The framework's abstract interfaces (`ConsumerManagerInterface`,
`PublisherInterface`, `ResultsStoreInterface`, etc.) are
transport-agnostic; `backends.py` is the single module that switches on
`config.backend`, so the runners never branch on transport. Pluggable
storage backends are under consideration for the future.

## Components

| Module | Purpose |
|---|---|
| `config.py` | `RuntimeConfig` — loads infra settings from YAML: `backend` selection plus RabbitMQ/Kafka connection, `MongoDBConfig`, `RedisConfig`, and `RuntimeRetryConfig`. The exchange is *not* here — it's pipeline topology and lives in the `PipelineSpec`. |
| `backends.py` | Pubsub backend factory — the single place that switches on `config.backend` to build connection managers, publishers, and consumer managers. |
| `infrastructure.py` | `RuntimeInfra` — creates and closes MongoDB, Redis, and pubsub connections from a `RuntimeConfig`. |
| `runner.py` | `DefaultWorkerRunner` — concrete `WorkerRunnerBase` that wires all infrastructure for a single worker type. Subclass hooks: `_wrap_worker()` (intercept worker after creation), `_create_resolvers()` (customize document resolution). |
| `spec.py` | `PipelineSpec`, `WorkerSpec`, `WorkerFactoryContext`, `WorkerFactory` — declarative pipeline composition. |

## Usage

### 1. Define a pipeline spec

```python
# my_pipeline/pipeline_spec.py
from warren.pubsub.rabbitmq.config import RMQExchangeConfig
from warren.runtime import (
    PipelineSpec,
    PublishSpec,
    WorkerSpec,
)

PIPELINE = PipelineSpec(
    exchange=RMQExchangeConfig(name="jobs", type="fanout"),
    workers={
        "parser": WorkerSpec(
            collections={"write": "parsed_documents"},
            factory=create_parser,
            publish=PublishSpec(),
            needs_document_fetcher=True,
        ),
        "chunker": WorkerSpec(
            collections={"read": "parsed_documents", "write": "chunks"},
            factory=create_chunker,
            publish=PublishSpec(),
        ),
        "embedder": WorkerSpec(
            collections={"read": "chunks", "write": "embeddings"},
            factory=create_embedder,
            publish=PublishSpec(),
        ),
    },
    result_collections=["parsed_documents", "chunks", "embeddings"],
    reference_collection="chunks",
    completion_collection="embeddings",
    final_data_type="embedded_document",
)
```

### 2. Write a config YAML

```yaml
rabbitmq:
  # The exchange lives in the pipeline spec (topology), not here —
  # config holds only per-environment infrastructure.
  connection:
    host: localhost
    port: 5672
    login: guest
    password: guest
  consumer:
    prefetch_count: 1
    on_shutdown_timeout: 30.0

mongodb:
  host: localhost
  port: 27017
  database: my_pipeline

redis:
  host: localhost
  port: 6379

retry:
  enabled: true
  collection_name: retries
```

### 3. Run a worker

```python
from warren.runtime import (
    DefaultWorkerRunner,
    RuntimeConfig,
)
from my_pipeline.pipeline_spec import PIPELINE

config = RuntimeConfig.from_yaml("config.yaml")
runner = DefaultWorkerRunner(
    config=config,
    worker_name="parser-1",
    worker_type="parser",
    worker_spec=PIPELINE.workers["parser"],
    exchange=PIPELINE.exchange,
)

await runner.setup()
await runner.run()      # blocks until SIGINT/SIGTERM
await runner.teardown()
```

See `USAGE.md` for the full guide including launcher scripts,
factory conventions, and custom runners.

### 4. Custom runner (optional)

Subclass `DefaultWorkerRunner` to customize behavior:

```python
class MyRunner(DefaultWorkerRunner):
    def _create_resolvers(self):
        # path, GCS, S3, and HTTP(S) resolvers ship built in
        resolvers = super()._create_resolvers()
        resolvers["azure"] = my_azure_blob_resolver
        return resolvers

    def _wrap_worker(self, worker, spec):
        return MyInstrumentationWrapper(worker)
```
