"""
Runtime infrastructure: create and close connections from a ``RuntimeConfig``.
MongoDB and Redis connections are created for the RabbitMQ and Kafka backends;
``backend: memory`` creates neither.

The pubsub connection manager is backend-selectable (RabbitMQ, Kafka or memory):
it is built via :func:`warren.runtime.backends.create_connection_manager`,
which switches on ``config.backend`` and imports the transport lazily.
"""

from typing import Any, NamedTuple

import asyncio
import logging
import re
from collections.abc import Awaitable, Callable
from urllib.parse import unquote

from basics.logging import get_logger
from basics.logging_utils import summarize_exception_chain
from pymongo import AsyncMongoClient
from redis.asyncio import Redis
from redis.asyncio.connection import SSLConnection

from warren.exceptions import WarrenError
from warren.runtime import backends
from warren.runtime.config import MongoDBConfig, RedisConfig, RuntimeConfig


module_logger: logging.Logger = get_logger(__name__)


def _create_mongo_client(config: MongoDBConfig) -> AsyncMongoClient:
    kwargs: dict[str, Any] = {}
    for field, option in (
        ("username", "username"),
        ("auth_source", "authSource"),
        ("max_pool_size", "maxPoolSize"),
        ("server_selection_timeout_ms", "serverSelectionTimeoutMS"),
    ):
        value = getattr(config, field)
        if value is not None:
            kwargs[option] = value
    if config.password is not None:
        kwargs["password"] = config.password.get_secret_value()
    if config.uri is None or "tls" in config.model_fields_set:
        kwargs["tls"] = config.tls
    if config.uri is not None:
        return AsyncMongoClient(config.uri.get_secret_value(), **kwargs)
    return AsyncMongoClient(host=config.host, port=config.port, **kwargs)


def _create_redis_client(config: RedisConfig) -> Redis:
    kwargs: dict[str, Any] = {"db": config.db}
    for field in ("username", "max_connections", "socket_timeout"):
        value = getattr(config, field)
        if value is not None:
            kwargs[field] = value
    if config.password is not None:
        kwargs["password"] = config.password.get_secret_value()
    if config.url is not None:
        if config.ssl:
            kwargs["connection_class"] = SSLConnection
        return Redis.from_url(config.url.get_secret_value(), **kwargs)
    return Redis(host=config.host, port=config.port, ssl=config.ssl, **kwargs)


class RuntimeInfra(NamedTuple):
    """All infrastructure connections for a worker process.

    On ``backend: memory`` there is no MongoDB or Redis, so both clients are
    ``None`` and stores are injected into the runners instead.
    """

    mongo_client: AsyncMongoClient | None
    redis_client: Redis | None
    # RMQ, Kafka or Memory connection manager, per config.backend.
    pubsub_connection_manager: Any


async def create_runtime_infrastructure(
    config: RuntimeConfig,
    *,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> RuntimeInfra:
    """Create and initialize all connections from runtime config.

    The caller is responsible for closing via
    ``close_runtime_infrastructure``.

    :param config: Runtime infrastructure configuration.
    :param sleep: Delay between startup attempts.
    :return: Initialized connections ready for use.
    """
    if config.backend == "memory":
        module_logger.warning(
            "backend 'memory': everything runs inside this process and nothing "
            "is persisted. For local runs and tests only."
        )
        manager = backends.create_connection_manager(config)
        await manager.setup()
        return RuntimeInfra(None, None, manager)

    delay = min(config.startup.initial_delay_seconds, config.startup.max_delay_seconds)
    attempt = 1
    while True:
        manager = backends.create_connection_manager(config)
        mongo_client: AsyncMongoClient | None = None
        redis_client: Redis | None = None
        try:
            mongo_client = _create_mongo_client(config.mongodb)
            redis_client = _create_redis_client(config.redis)
            await manager.setup()
            await mongo_client.admin.command("ping")
            await redis_client.ping()
        except (Exception, asyncio.CancelledError) as e:
            await close_runtime_infrastructure(
                RuntimeInfra(mongo_client, redis_client, manager)
            )
            if isinstance(e, asyncio.CancelledError):
                raise
            module_logger.warning(
                f"Startup connection attempt {attempt}/{config.startup.attempts} "
                f"failed: {_startup_error_summary(e, config)}"
            )
            if attempt == config.startup.attempts:
                raise
        else:
            return RuntimeInfra(mongo_client, redis_client, manager)
        await sleep(delay)
        delay = min(delay * 2, config.startup.max_delay_seconds)
        attempt += 1


def _startup_error_summary(error: Exception, config: RuntimeConfig) -> str:
    summary = summarize_exception_chain(error)
    for secret in (
        config.mongodb.uri,
        config.mongodb.password,
        config.redis.url,
        config.redis.password,
        config.rabbitmq.connection.password,
    ):
        if secret is None:
            continue
        value = secret.get_secret_value()
        if value:
            summary = summary.replace(value, "**********")
        # Drivers may report just the decoded password from a connection URL.
        for password in re.findall(r"://[^/@]*:([^/@]+)@", value):
            summary = summary.replace(password, "**********")
            summary = summary.replace(unquote(password), "**********")
    return " ".join(summary.splitlines())


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
