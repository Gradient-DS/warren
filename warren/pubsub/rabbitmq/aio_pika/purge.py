"""
Purge RabbitMQ queues and optionally delete an exchange.

Framework-level utility for cleaning up RabbitMQ state between runs.
Queue/exchange names are caller-provided — no hardcoded topology knowledge.
"""

import logging
from collections.abc import Sequence

from aio_pika.abc import AbstractChannel
from aio_pika.exceptions import ChannelNotFoundEntity

from basics.logging import get_logger

from document_processing.distributed.warren.pubsub.rabbitmq.aio_pika.connection import (
    RMQConnectionManager,
)

module_logger: logging.Logger = get_logger(__name__)


async def purge_queues(
    *,
    connection_manager: RMQConnectionManager,
    queue_names: Sequence[str],
    exchange_name: str = "",
) -> None:
    """Delete the listed queues and optionally the exchange.

    :param connection_manager: an already-setup ``RMQConnectionManager``.
    :param queue_names: queues to delete.
    :param exchange_name: exchange to delete. Skipped when empty.
    """
    channel: AbstractChannel = await connection_manager.create_channel()

    for queue_name in queue_names:
        try:
            await channel.queue_delete(queue_name)
            module_logger.info("Deleted queue: %s", queue_name)
        except ChannelNotFoundEntity:
            module_logger.info("Queue does not exist, skipping: %s", queue_name)
        except Exception:
            channel = await connection_manager.create_channel()

    if exchange_name:
        try:
            await channel.exchange_delete(exchange_name)
            module_logger.info("Deleted exchange: %s", exchange_name)
        except ChannelNotFoundEntity:
            module_logger.info(
                "Exchange does not exist, skipping: %s", exchange_name
            )
        except Exception:
            pass
