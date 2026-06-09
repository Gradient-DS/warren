"""
Runtime infrastructure: create and close MongoDB, Redis, and RabbitMQ
connections from a ``RuntimeConfig``.
"""

from typing import NamedTuple

import logging
from collections.abc import Awaitable, Callable

from basics.logging import get_logger
from basics.logging_utils import summarize_exception_chain
from pymongo import AsyncMongoClient
from redis.asyncio import Redis

from document_processing.distributed.warren.pubsub.rabbitmq.aio_pika.connection import (
    RMQConnectionManager,
)
from document_processing.distributed.warren.runtime.config import RuntimeConfig


module_logger: logging.Logger = get_logger(__name__)


class RuntimeInfra(NamedTuple):
    """All infrastructure connections for a worker process."""

    mongo_client: AsyncMongoClient
    redis_client: Redis
    rmq_connection_manager: RMQConnectionManager


async def create_runtime_infrastructure(config: RuntimeConfig) -> RuntimeInfra:
    """Create and initialize all connections from runtime config.

    The caller is responsible for closing via
    ``close_runtime_infrastructure``.

    :param config: Runtime infrastructure configuration.
    :return: Initialized connections ready for use.
    """
    mongo_client = AsyncMongoClient(
        host=config.mongodb.host,
        port=config.mongodb.port,
    )

    redis_client = Redis(
        host=config.redis.host,
        port=config.redis.port,
    )

    rmq_connection_manager = RMQConnectionManager(config.rabbitmq.connection)
    await rmq_connection_manager.setup()

    return RuntimeInfra(
        mongo_client=mongo_client,
        redis_client=redis_client,
        rmq_connection_manager=rmq_connection_manager,
    )


async def close_runtime_infrastructure(infra: RuntimeInfra) -> None:
    """Close all connections. Best-effort — logs and continues on errors.

    :param infra: Infrastructure to close.
    """
    await _close_connection(
        infra.rmq_connection_manager.teardown, "RabbitMQ connection"
    )
    await _close_connection(infra.redis_client.aclose, "Redis client")
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
