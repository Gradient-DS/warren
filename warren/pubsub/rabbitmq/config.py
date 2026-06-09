"""
Pure-data configuration models for the RabbitMQ pubsub layer.

These models are intentionally free of any ``aio_pika`` dependency: they
describe *what* a connection, exchange, queue, consumer, or retry policy
should look like, and they can be loaded from YAML, serialized, compared,
or reused (e.g. embedded in ``RuntimeConfig``) without pulling in the
transport library. The behavior of acting on these configs — opening
connections, declaring exchanges and queues — lives in
``connection.py``, ``topology.py``, ``consumer.py``, and ``publisher.py``.
"""

from typing import Any, Literal

from pydantic import BaseModel, SecretStr


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
    type: Literal["topic", "fanout"] = "topic"
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


class RetryConfig(BaseModel):
    """Retry policy configuration for the consumer manager.

    Provides defaults when the worker's ``SoftFailureException`` does not
    specify values, and caps to enforce system-level limits.

    :param initial_delay: Initial delay in seconds before first retry
        when worker does not specify.
    :param max_retries: Max retry attempts when worker does not specify.
    :param backoff_base: Base for exponential backoff. Delay on
        attempt N = initial_delay * backoff_base^(N-1).
    :param jitter: Whether to add random jitter to delays.
    :param max_delay_cap: Maximum delay in seconds (caps backoff).
    :param max_retries_cap: Maximum retries allowed (overrides
        worker request if exceeded).
    :param fallback_requeue_delay: Delay in seconds before
        nack+requeue when no retry publisher is configured.
        Prevents tight retry loops.
    """

    initial_delay: int = 30
    max_retries: int = 5
    backoff_base: float = 2.0
    jitter: bool = True
    max_delay_cap: int = 300
    max_retries_cap: int = 10
    fallback_requeue_delay: float = 2.0


class RMQConsumerManagerConfig(BaseModel):
    exchange: RMQExchangeConfig
    queue: RMQQueueConfig
    consumer: RMQConsumerConfig
