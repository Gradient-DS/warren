"""
Topology declaration helpers for RabbitMQ.

These functions perform the I/O of declaring exchanges and queues on a
channel, given pure-data ``RMQExchangeConfig`` / ``RMQQueueConfig`` values.
Keeping the behavior here (rather than as methods on the config models)
lets the configs stay decoupled from ``aio_pika``: they can be loaded,
serialized, and reused without pulling in the transport library.
"""

from aio_pika.abc import (
    AbstractChannel,
    AbstractExchange,
    AbstractQueue,
)

from document_processing.distributed.warren.pubsub.common import PubSubSetupError
from document_processing.distributed.warren.pubsub.rabbitmq.config import (
    RMQExchangeConfig,
    RMQQueueConfig,
)


async def declare_exchange(
    channel: AbstractChannel,
    config: RMQExchangeConfig,
) -> AbstractExchange:
    """Idempotently declare the exchange described by ``config`` on ``channel``.

    :raises PubSubSetupError: if the broker rejects the declaration.
    """
    try:
        return await channel.declare_exchange(
            config.name,
            config.type,
            durable=config.durable,
        )
    except Exception as e:
        raise PubSubSetupError(
            f"Failed to declare exchange '{config.name}' (type={config.type})"
        ) from e


async def declare_queue(
    channel: AbstractChannel,
    exchange: AbstractExchange,
    config: RMQQueueConfig,
    *,
    exchange_type: str,
) -> AbstractQueue:
    """Idempotently declare the queue described by ``config`` and bind it to ``exchange``.

    :raises PubSubSetupError: if the broker rejects the declaration or bind.
    """
    if config.routing_key and exchange_type == "fanout":
        raise ValueError("Non-empty routing key is not supported for fanout exchanges.")

    try:
        queue = await channel.declare_queue(
            config.name,
            durable=config.durable,
            exclusive=config.exclusive,
            auto_delete=config.auto_delete,
        )
        await queue.bind(exchange.name, routing_key=config.routing_key or "")
    except Exception as e:
        raise PubSubSetupError(
            f"Failed to declare queue '{config.name}' bound to exchange "
            f"'{exchange.name}' (routing_key='{config.routing_key or ''}')"
        ) from e

    return queue
