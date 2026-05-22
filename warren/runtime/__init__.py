"""
Runtime layer for the distributed processing framework.

Provides the concrete wiring that turns a ``PipelineSpec`` into running
worker processes. This runtime is built on **RabbitMQ** (via aio_pika),
**MongoDB**, and **Redis**.

Key components:

- ``RuntimeConfig`` — load infrastructure settings from YAML.
- ``RuntimeInfra`` — create / close MongoDB, Redis, and RMQ connections.
- ``DefaultWorkerRunner`` — concrete runner that wires everything for a
  single worker type.
- ``PipelineSpec`` / ``WorkerSpec`` — declarative pipeline composition.
"""

from document_processing.distributed.warren.runtime.config import (
    MongoDBConfig,
    RedisConfig,
    RuntimeConfig,
    RuntimeRetryConfig,
    RuntimeRMQConfig,
)
from document_processing.distributed.warren.runtime.infrastructure import (
    RuntimeInfra,
    close_runtime_infrastructure,
    create_runtime_infrastructure,
)
from document_processing.distributed.warren.runtime.runner import (
    DefaultWorkerRunner,
)
from document_processing.distributed.warren.runtime.spec import (
    PipelineSpec,
    WorkerFactory,
    WorkerFactoryContext,
    WorkerSpec,
)

__all__ = [
    "DefaultWorkerRunner",
    "MongoDBConfig",
    "PipelineSpec",
    "RedisConfig",
    "RuntimeConfig",
    "RuntimeInfra",
    "RuntimeRetryConfig",
    "RuntimeRMQConfig",
    "WorkerFactory",
    "WorkerFactoryContext",
    "WorkerSpec",
    "close_runtime_infrastructure",
    "create_runtime_infrastructure",
]
