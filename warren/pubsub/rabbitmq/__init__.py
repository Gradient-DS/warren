"""
RabbitMQ pubsub layer.

The top-level module exposes only the **protocol-level configs** —
pure-data Pydantic models that describe the RabbitMQ topology and
retry policy. They have no dependency on any specific Python client
library and can be loaded, serialized, and reused freely.

Concrete client implementations live in sub-packages:

- :mod:`...rabbitmq.aio_pika` — ``aio_pika``-based managers and helpers.

To use the ``aio_pika`` implementation, import explicitly from that
sub-package, e.g.::

    from document_processing.distributed.warren.pubsub.rabbitmq.aio_pika import (
        RMQConnectionManager,
        RMQConsumerManager,
        RMQPublisher,
    )
"""

from document_processing.distributed.warren.pubsub.rabbitmq.config import (
    RetryConfig,
    RMQConnectionConfig,
    RMQConsumerConfig,
    RMQConsumerManagerConfig,
    RMQExchangeConfig,
    RMQQueueConfig,
)


__all__ = [
    "RMQConnectionConfig",
    "RMQConsumerConfig",
    "RMQConsumerManagerConfig",
    "RMQExchangeConfig",
    "RMQQueueConfig",
    "RetryConfig",
]
