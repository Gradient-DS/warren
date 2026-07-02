"""
``aio_pika``-based implementation of the RabbitMQ pubsub layer.

The protocol-level configs in :mod:`...rabbitmq.config` describe *what*
to set up; the modules here use ``aio_pika`` to actually do it.

``aio_pika`` is an optional dependency: importing this sub-package
without it installed raises
:class:`~warren.exceptions.OptionalDependencyError` pointing at the
``warren[rmq]`` extra.
"""

from warren.exceptions import OptionalDependencyError


try:
    import aio_pika  # noqa: F401
except ImportError as e:
    _PACKAGE = "aio-pika"
    raise OptionalDependencyError(
        _PACKAGE,
        install_hint='pip install "warren[rmq]"',
    ) from e

from warren.pubsub.rabbitmq.aio_pika.connection import (
    RMQConnectionManager,
)
from warren.pubsub.rabbitmq.aio_pika.consumer import (
    RMQConsumerManager,
)
from warren.pubsub.rabbitmq.aio_pika.publisher import (
    RMQPublisher,
)
from warren.pubsub.rabbitmq.aio_pika.purge import (
    purge_queues,
)
from warren.pubsub.rabbitmq.aio_pika.topology import (
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
