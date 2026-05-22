# warren/runtime/

Concrete runtime layer for the distributed processing framework. Turns a
`PipelineSpec` into running worker processes.

## Technology assumptions

This runtime is built on a specific infrastructure stack:

- **RabbitMQ** for message passing (via `aio_pika`)
- **MongoDB** for document storage, job tracking, and results
- **Redis** for caching (results, document bytes)

The framework's abstract interfaces (`ConsumerManagerInterface`,
`PublisherInterface`, `ResultsStoreInterface`, etc.) are
transport-agnostic, but this runtime layer binds them to the concrete
implementations above. Making the runtime itself infrastructure-agnostic
(pluggable transports and storage backends) is under consideration for
the future.

## Components

| Module | Purpose |
|---|---|
| `config.py` | `RuntimeConfig` — loads infra settings from YAML. Composes warren's own `RMQConnectionConfig`, `RMQExchangeConfig`, `RMQConsumerConfig` with `MongoDBConfig`, `RedisConfig`, and `RuntimeRetryConfig`. |
| `infrastructure.py` | `RuntimeInfra` — creates and closes MongoDB, Redis, and RMQ connections from a `RuntimeConfig`. |
| `runner.py` | `DefaultWorkerRunner` — concrete `WorkerRunnerBase` that wires all infrastructure for a single worker type. Subclass hooks: `_wrap_worker()` (intercept worker after creation), `_create_resolvers()` (customize document resolution). |
| `spec.py` | `PipelineSpec`, `WorkerSpec`, `WorkerFactoryContext`, `WorkerFactory` — declarative pipeline composition. |

## Usage

### 1. Define a pipeline spec

```python
# my_pipeline/pipeline_spec.py
from document_processing.distributed.warren.runtime import (
    PipelineSpec,
    WorkerSpec,
)

PIPELINE = PipelineSpec(
    workers={
        "parser": WorkerSpec(
            collections={"write": "parsed_documents"},
            factory=create_parser,
            needs_document_fetcher=True,
        ),
        "chunker": WorkerSpec(
            collections={"read": "parsed_documents", "write": "chunks"},
            factory=create_chunker,
        ),
        "embedder": WorkerSpec(
            collections={"read": "chunks", "write": "embeddings"},
            factory=create_embedder,
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
  connection:
    host: localhost
    port: 5672
    login: guest
    password: guest
  exchange:
    name: jobs
    type: fanout
    durable: true
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
from document_processing.distributed.warren.runtime import (
    DefaultWorkerRunner,
    RuntimeConfig,
)
from my_pipeline.pipeline_spec import PIPELINE

config = RuntimeConfig.from_yaml("config.yaml")
runner = DefaultWorkerRunner(
    worker_type="parser",
    worker_name="parser-1",
    config=config,
    pipeline=PIPELINE,
)

await runner.setup()
await runner.run()      # blocks until SIGINT/SIGTERM
await runner.teardown()
```

### 4. Custom runner (optional)

Subclass `DefaultWorkerRunner` to customize behavior:

```python
class MyRunner(DefaultWorkerRunner):
    def _create_resolvers(self):
        resolvers = super()._create_resolvers()
        resolvers["s3"] = my_s3_resolver
        return resolvers

    def _wrap_worker(self, worker, spec):
        return MyInstrumentationWrapper(worker)
```
