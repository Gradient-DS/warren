"""
Runtime infrastructure: create and close connections from a ``RuntimeConfig``.
MongoDB and Redis connections are created for the RabbitMQ and Kafka backends;
``backend: memory`` creates neither.

The pubsub connection manager is backend-selectable (RabbitMQ, Kafka or memory):
it is built via :func:`warren.runtime.backends.create_connection_manager`,
which switches on ``config.backend`` and imports the transport lazily.
"""

from typing import Any, NamedTuple

import logging
from collections.abc import Awaitable, Callable

from basics.logging import get_logger
from basics.logging_utils import summarize_exception_chain
from pymongo import AsyncMongoClient
from redis.asyncio import Redis

from warren.exceptions import WarrenError
from warren.runtime import backends
from warren.runtime.config import RuntimeConfig


module_logger: logging.Logger = get_logger(__name__)


class RuntimeInfra(NamedTuple):
    """All infrastructure connections for a worker process.

    On ``backend: memory`` there is no MongoDB or Redis, so both clients are
    ``None`` and stores are injected into the runners instead.
    """

    mongo_client: AsyncMongoClient | None
    redis_client: Redis | None
    # RMQ, Kafka or Memory connection manager, per config.backend.
    pubsub_connection_manager: Any


async def create_runtime_infrastructure(config: RuntimeConfig) -> RuntimeInfra:
    """Create and initialize all connections from runtime config.

    The caller is responsible for closing via
    ``close_runtime_infrastructure``.

    :param config: Runtime infrastructure configuration.
    :return: Initialized connections ready for use.
    """
    mongo_client: AsyncMongoClient | None = None
    redis_client: Redis | None = None

    if config.backend == "memory":
        module_logger.warning(
            "backend 'memory': everything runs inside this process and nothing "
            "is persisted. For local runs and tests only."
        )
    else:
        mongo_client = AsyncMongoClient(
            host=config.mongodb.host,
            port=config.mongodb.port,
        )
        redis_client = Redis(
            host=config.redis.host,
            port=config.redis.port,
        )

    pubsub_connection_manager = backends.create_connection_manager(config)
    await pubsub_connection_manager.setup()

    return RuntimeInfra(
        mongo_client=mongo_client,
        redis_client=redis_client,
        pubsub_connection_manager=pubsub_connection_manager,
    )


async def close_runtime_infrastructure(infra: RuntimeInfra) -> None:
    """Close all connections. Best-effort — logs and continues on errors.

    :param infra: Infrastructure to close.
    """
    await _close_connection(
        infra.pubsub_connection_manager.teardown, "pubsub connection"
    )
    if infra.redis_client is not None:
        await _close_connection(infra.redis_client.aclose, "Redis client")
    if infra.mongo_client is not None:
        await _close_connection(infra.mongo_client.close, "MongoDB client")


async def _close_connection(
    close: Callable[[], Awaitable[None]],
    what: str,
) -> None:
    """Close a connection, logging the outcome — best-effort, never silent.

    A failure closing one connection is logged at warning (never raised)
    so it cannot prevent the others from being closed, and so cleanup is
    never silently swallowed.
    """
    module_logger.debug(f"Closing {what} ...")
    try:
        await close()
    except Exception as e:
        module_logger.warning(f"Error closing {what}: {summarize_exception_chain(e)}")
    else:
        module_logger.debug(f"Closed {what}")


def require_injected(config: RuntimeConfig, **components: object) -> None:
    """Fail clearly when the memory backend would need a default store.

    Default stores are built from the Mongo and Redis clients. The memory
    backend has neither, so every store a runner needs must be injected.
    Without this check the failure is an ``AttributeError`` on ``None``
    deep inside a store constructor.

    :param config: Runtime configuration.
    :param components: Name to injected value, for each component the
        runner needs. ``None`` means "not injected".
    :raises WarrenError: On the memory backend when any value is ``None``.
    """
    if config.backend != "memory":
        return
    missing = [name for name, value in components.items() if value is None]
    if missing:
        msg = (
            f"backend 'memory' has no MongoDB or Redis to build defaults from; "
            f"inject: {', '.join(missing)}. "
            f"warren.runtime.in_process.create_in_process_runners does this."
        )
        raise WarrenError(msg)
