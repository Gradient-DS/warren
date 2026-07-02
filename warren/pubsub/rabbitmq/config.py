"""
Pure-data configuration models for the RabbitMQ pubsub layer.

These models are intentionally free of any ``aio_pika`` dependency: they
describe *what* a connection, exchange, queue, consumer, or retry policy
should look like, and they can be loaded from YAML, serialized, compared,
or reused (e.g. embedded in ``RuntimeConfig``) without pulling in the
transport library. The behavior of acting on these configs — opening
connections, declaring exchanges and queues — lives in
``connection.py``, ``topology.py``, ``consumer.py``, and ``publisher.py``.

``RetryConfig`` is transport-agnostic (shared with the Kafka backend) and
lives in :mod:`warren.pubsub.common`; it is re-exported here for backwards
compatibility.
"""

from typing import Any, Literal

from pydantic import BaseModel, SecretStr

from warren.pubsub.common import RetryConfig


__all__ = [
    "RMQConnectionConfig",
    "RMQConsumerConfig",
    "RMQConsumerManagerConfig",
    "RMQExchangeConfig",
    "RMQQueueConfig",
    "RetryConfig",
]


class RMQConnectionConfig(BaseModel):
    """Connection-specific parameters for ``aio_pika.connect_robust()``."""

    host: str = "localhost"
    port: int = 5672
    login: str = "guest"
    password: SecretStr = SecretStr("guest")
    virtualhost: str = "/"
    ssl: bool = False
    ssl_options: dict[str, Any] | None = None
    ssl_context: Any | None = None  # ssl.SSLContext
    timeout: float | None = None
    client_properties: dict[str, Any] | None = None


class RMQExchangeConfig(BaseModel):
    name: str
    # "headers" is intentionally unsupported (routes on binding arguments,
    # a different code path from routing keys). See tasks/routing-design.md D1.
    type: Literal["fanout", "direct", "topic"] = "topic"
    durable: bool = True


class RMQQueueConfig(BaseModel):
    name: str
    durable: bool = True
    exclusive: bool = False
    auto_delete: bool = False
    routing_key: str | None = None


class RMQConsumerConfig(BaseModel):
    # TODO: prefetch count is influenced by the worker's concurrency level. How to handle this?
    prefetch_count: int = 1
    on_shutdown_timeout: float = 30.0


class RMQConsumerManagerConfig(BaseModel):
    exchange: RMQExchangeConfig
    queue: RMQQueueConfig
    consumer: RMQConsumerConfig
