"""
Purge RabbitMQ queues and optionally delete an exchange.

Framework-level utility for cleaning up RabbitMQ state between runs.
Queue/exchange names are caller-provided — no hardcoded topology knowledge.
"""

import logging
from collections.abc import Awaitable, Callable, Sequence

from aio_pika.abc import AbstractChannel
from aio_pika.exceptions import ChannelNotFoundEntity
from basics.logging import get_logger
from basics.logging_utils import summarize_exception_chain

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

    Best-effort: each deletion runs on its own channel, so failing to
    open a channel or to delete one entity is logged and the remaining
    entities are still attempted.

    :param connection_manager: an already-setup ``RMQConnectionManager``.
    :param queue_names: queues to delete.
    :param exchange_name: exchange to delete. Skipped when empty.
    """
    for queue_name in queue_names:
        await _delete_entity(
            connection_manager,
            "queue",
            queue_name,
            lambda channel, q=queue_name: channel.queue_delete(q),
        )

    if exchange_name:
        await _delete_entity(
            connection_manager,
            "exchange",
            exchange_name,
            lambda channel: channel.exchange_delete(exchange_name),
        )


async def _delete_entity(
    connection_manager: RMQConnectionManager,
    kind: str,
    name: str,
    delete: Callable[[AbstractChannel], Awaitable[object]],
) -> None:
    """Delete one queue/exchange on its own channel, best-effort.

    Failing to open the channel, to delete the entity, or to close the
    channel afterwards is logged and swallowed, so a single failure
    never aborts the wider purge.
    """
    try:
        channel = await connection_manager.create_channel()
    except Exception as e:
        module_logger.warning(
            "Could not open a channel to delete %s '%s': %s",
            kind,
            name,
            summarize_exception_chain(e),
        )
        return

    try:
        await delete(channel)
        module_logger.info("Deleted %s: %s", kind, name)
    except ChannelNotFoundEntity:
        module_logger.info("%s does not exist, skipping: %s", kind, name)
    except Exception as e:
        module_logger.warning(
            "Failed to delete %s '%s': %s",
            kind,
            name,
            summarize_exception_chain(e),
        )
    finally:
        try:
            await channel.close()
        except Exception as e:
            module_logger.debug(
                "Error closing purge channel for %s '%s': %s",
                kind,
                name,
                summarize_exception_chain(e),
            )
