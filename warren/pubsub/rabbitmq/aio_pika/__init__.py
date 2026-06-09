"""
``aio_pika``-based implementation of the RabbitMQ pubsub layer.

The protocol-level configs in :mod:`...rabbitmq.config` describe *what*
to set up; the modules here use ``aio_pika`` to actually do it.
"""

from document_processing.distributed.warren.pubsub.rabbitmq.aio_pika.connection import (
    RMQConnectionManager,
)
from document_processing.distributed.warren.pubsub.rabbitmq.aio_pika.consumer import (
    RMQConsumerManager,
)
from document_processing.distributed.warren.pubsub.rabbitmq.aio_pika.publisher import (
    RMQPublisher,
)
from document_processing.distributed.warren.pubsub.rabbitmq.aio_pika.purge import (
    purge_queues,
)
from document_processing.distributed.warren.pubsub.rabbitmq.aio_pika.topology import (
    declare_exchange,
    declare_queue,
)


__all__ = [
    "RMQConnectionManager",
    "RMQConsumerManager",
    "RMQPublisher",
    "declare_exchange",
    "declare_queue",
    "purge_queues",
]
