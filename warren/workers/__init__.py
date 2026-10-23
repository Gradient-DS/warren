"""Workers: the worker base classes, the processing message and the worker runner.

The package also holds the health endpoint of a worker.
"""

from warren.workers.health import HealthConfig, HealthServer
from warren.workers.messages import (
    ExtractMessageIdentityFunc,
    MessageOrigin,
    ProcessingMessage,
    RetryInfo,
    build_message_key,
    build_message_key_prefix,
    extract_message_identity,
)
from warren.workers.runners import ConsumerManagerFactory, WorkerRunnerBase
from warren.workers.workers import (
    AsyncProcessingWorkerBase,
    CapabilityWorkerBase,
    FilteringWorkerBase,
    SyncProcessingWorkerBase,
)


__all__ = [
    "AsyncProcessingWorkerBase",
    "CapabilityWorkerBase",
    "ConsumerManagerFactory",
    "ExtractMessageIdentityFunc",
    "FilteringWorkerBase",
    "HealthConfig",
    "HealthServer",
    "MessageOrigin",
    "ProcessingMessage",
    "RetryInfo",
    "SyncProcessingWorkerBase",
    "WorkerRunnerBase",
    "build_message_key",
    "build_message_key_prefix",
    "extract_message_identity",
]
