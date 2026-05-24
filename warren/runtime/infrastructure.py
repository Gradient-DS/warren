"""
Runtime infrastructure: create and close MongoDB, Redis, and RabbitMQ
connections from a ``RuntimeConfig``.
"""

from typing import NamedTuple

from pymongo import AsyncMongoClient
from redis.asyncio import Redis

from document_processing.distributed.warren.pubsub.rabbitmq.aio_pika.connection import (
    RMQConnectionManager,
)
from document_processing.distributed.warren.runtime.config import RuntimeConfig


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
    """Close all connections. Best-effort — continues on errors.

    :param infra: Infrastructure to close.
    """
    try:
        await infra.rmq_connection_manager.teardown()
    except Exception:
        pass
    try:
        await infra.redis_client.aclose()
    except Exception:
        pass
    try:
        await infra.mongo_client.close()
    except Exception:
        pass
