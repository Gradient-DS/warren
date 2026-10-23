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
