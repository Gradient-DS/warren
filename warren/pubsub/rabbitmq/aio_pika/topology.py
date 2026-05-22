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

from document_processing.distributed.warren.pubsub.rabbitmq.config import (
    RMQExchangeConfig,
    RMQQueueConfig,
)


async def declare_exchange(
    channel: AbstractChannel,
    config: RMQExchangeConfig,
) -> AbstractExchange:
    """Idempotently declare the exchange described by ``config`` on ``channel``."""
    return await channel.declare_exchange(
        config.name,
        config.type,
        durable=config.durable,
    )


async def declare_queue(
    channel: AbstractChannel,
    exchange: AbstractExchange,
    config: RMQQueueConfig,
    *,
    exchange_type: str,
) -> AbstractQueue:
    """Idempotently declare the queue described by ``config`` and bind it to ``exchange``."""
    if config.routing_key and exchange_type == "fanout":
        raise ValueError(
            "Non-empty routing key is not supported for fanout exchanges."
        )

    queue = await channel.declare_queue(
        config.name,
        durable=config.durable,
        exclusive=config.exclusive,
        auto_delete=config.auto_delete,
    )

    await queue.bind(exchange.name, routing_key=config.routing_key or "")

    return queue
