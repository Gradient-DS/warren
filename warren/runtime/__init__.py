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

from warren.runtime.config import (
    MongoDBConfig,
    RedisConfig,
    RuntimeConfig,
    RuntimeRetryConfig,
    RuntimeRMQConfig,
)
from warren.runtime.infrastructure import (
    RuntimeInfra,
    close_runtime_infrastructure,
    create_runtime_infrastructure,
)
from warren.runtime.runner import (
    DefaultWorkerRunner,
)
from warren.runtime.spec import (
    PipelineSpec,
    PublishSpec,
    WorkerFactory,
    WorkerFactoryContext,
    WorkerSpec,
)
from warren.runtime.validation import (
    PipelineValidationError,
    RoutingPlanValidationError,
    build_capability_registry,
    validate_pipeline,
    validate_routing_plan,
)


__all__ = [
    "DefaultWorkerRunner",
    "MongoDBConfig",
    "PipelineSpec",
    "PipelineValidationError",
    "PublishSpec",
    "RedisConfig",
    "RoutingPlanValidationError",
    "RuntimeConfig",
    "RuntimeInfra",
    "RuntimeRMQConfig",
    "RuntimeRetryConfig",
    "WorkerFactory",
    "WorkerFactoryContext",
    "WorkerSpec",
    "build_capability_registry",
    "close_runtime_infrastructure",
    "create_runtime_infrastructure",
    "validate_pipeline",
    "validate_routing_plan",
]
