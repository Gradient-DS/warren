"""
Runtime layer for the distributed processing framework.

Provides the concrete wiring that turns a ``PipelineSpec`` into running
worker processes. The runtime runs a pipeline on RabbitMQ (aio-pika) or
Kafka (aiokafka), with MongoDB and Redis for storage. ``backend: memory``
runs all workers in one process, with no infrastructure.

## Pipeline spec

Declarative pipeline composition: the exchange, the worker types and how
each worker type is built.

- `PipelineSpec`
- `WorkerSpec`
- `PublishSpec`
- `WorkerFactory`
- `WorkerFactoryContext`

## Run

``DefaultWorkerRunner`` runs one worker. ``create_in_process_runners`` and
``run_in_process`` run a whole pipeline in one process on ``backend: memory``.

- `DefaultWorkerRunner`
- `create_in_process_runners`
- `run_in_process`

## Configuration

``RuntimeConfig`` loads the settings of a deployment from YAML. The other
models are its sections.

- `RuntimeConfig`
- `RuntimeRMQConfig`
- `RuntimeKafkaConfig`
- `MongoDBConfig`
- `RedisConfig`
- `CacheConfig`
- `DocumentsConfig`
- `RetentionConfig`
- `RuntimeRetryConfig`
- `StartupConfig`
- `ScopingConfig`

## Infrastructure and backends

- `RuntimeInfra`
- `create_runtime_infrastructure`
- `close_runtime_infrastructure`
- `require_injected`
- `create_connection_manager`
- `create_publisher`
- `create_consumer_manager`
- `create_default_resolvers`

## Validation

- `validate_pipeline`
- `validate_routing_plan`
- `build_capability_registry`
- `CapabilityRegistry`
- `PipelineValidationError`
- `RoutingPlanValidationError`
"""

from warren.runtime.backends import (
    create_connection_manager,
    create_consumer_manager,
    create_publisher,
)
from warren.runtime.config import (
    CacheConfig,
    DocumentsConfig,
    MongoDBConfig,
    RedisConfig,
    RetentionConfig,
    RuntimeConfig,
    RuntimeKafkaConfig,
    RuntimeRetryConfig,
    RuntimeRMQConfig,
    ScopingConfig,
    StartupConfig,
)
from warren.runtime.in_process import (
    create_in_process_runners,
    run_in_process,
)
from warren.runtime.infrastructure import (
    RuntimeInfra,
    close_runtime_infrastructure,
    create_runtime_infrastructure,
    require_injected,
)
from warren.runtime.runner import (
    DefaultWorkerRunner,
    create_default_resolvers,
)
from warren.runtime.spec import (
    PipelineSpec,
    PublishSpec,
    WorkerFactory,
    WorkerFactoryContext,
    WorkerSpec,
)
from warren.runtime.validation import (
    CapabilityRegistry,
    PipelineValidationError,
    RoutingPlanValidationError,
    build_capability_registry,
    validate_pipeline,
    validate_routing_plan,
)


__all__ = [
    "CacheConfig",
    "CapabilityRegistry",
    "DefaultWorkerRunner",
    "DocumentsConfig",
    "MongoDBConfig",
    "PipelineSpec",
    "PipelineValidationError",
    "PublishSpec",
    "RedisConfig",
    "RetentionConfig",
    "RoutingPlanValidationError",
    "RuntimeConfig",
    "RuntimeInfra",
    "RuntimeKafkaConfig",
    "RuntimeRMQConfig",
    "RuntimeRetryConfig",
    "ScopingConfig",
    "StartupConfig",
    "WorkerFactory",
    "WorkerFactoryContext",
    "WorkerSpec",
    "build_capability_registry",
    "close_runtime_infrastructure",
    "create_connection_manager",
    "create_consumer_manager",
    "create_default_resolvers",
    "create_in_process_runners",
    "create_publisher",
    "create_runtime_infrastructure",
    "require_injected",
    "run_in_process",
    "validate_pipeline",
    "validate_routing_plan",
]
