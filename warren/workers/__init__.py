"""Workers: the worker base classes, the processing message and the worker runner.

To choose a base class, decide how the worker selects its messages and how
it runs. ``CapabilityWorkerBase`` selects by a declared set of ``accepts``
data types. ``FilteringWorkerBase`` selects with a hand-written
``should_process``. Both are async. For a worker without selection, use
``AsyncProcessingWorkerBase`` for async code, or ``SyncProcessingWorkerBase``
for sync code that runs in a thread.

## Worker base classes

- `CapabilityWorkerBase`
- `FilteringWorkerBase`
- `AsyncProcessingWorkerBase`
- `SyncProcessingWorkerBase`

## Messages

- `ProcessingMessage`
- `MessageOrigin`
- `RetryInfo`

## Runner

- `WorkerRunnerBase`
- `ConsumerManagerFactory`

## Health

- `HealthConfig`
- `HealthServer`

## Message keys and identity

- `build_message_key`
- `build_message_key_prefix`
- `extract_message_identity`
- `ExtractMessageIdentityFunc`
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
