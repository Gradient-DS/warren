"""
Topology helpers for Kafka.

These functions perform the I/O of verifying (and optionally creating)
topics, given pure-data ``KafkaTopicConfig`` values. Keeping the behavior
here (rather than as methods on the config models) lets the configs stay
decoupled from ``aiokafka`` — the same pattern as the RabbitMQ topology
helpers.

There is no queue or binding to declare: one topic plays the role of a
fanout exchange, and consumer groups (configured on the consumer
manager) play the role of queues.
"""

from typing import TYPE_CHECKING

from aiokafka.admin import NewTopic
from aiokafka.errors import TopicAlreadyExistsError

from warren.pubsub.common import PubSubSetupError
from warren.pubsub.kafka.aiokafka.connection import KafkaConnectionManager
from warren.pubsub.kafka.config import KafkaTopicConfig


if TYPE_CHECKING:
    from aiokafka.protocol.api import Response


async def ensure_topic(
    connection_manager: KafkaConnectionManager,
    config: KafkaTopicConfig,
) -> None:
    """Ensure the topic described by ``config`` exists on the cluster.

    If the topic is missing and ``config.create_if_missing`` is enabled,
    it is created — idempotently, so concurrent creation by another
    worker is tolerated. If it is missing and creation is disabled
    (platforms where topics are provisioned out-of-band), setup fails
    loud.

    :raises PubSubSetupError: if the topic is missing while
        ``create_if_missing`` is disabled, or if the existence check or
        the creation fails.
    """
    admin = connection_manager.admin

    try:
        existing = await admin.list_topics()
    except Exception as e:
        msg = f"Failed to list topics while ensuring topic '{config.name}'"
        raise PubSubSetupError(msg) from e

    if config.name in existing:
        return

    if not config.create_if_missing:
        msg = (
            f"Topic '{config.name}' does not exist and create_if_missing "
            f"is disabled — create it out-of-band or enable creation."
        )
        raise PubSubSetupError(msg)

    new_topic = NewTopic(
        name=config.name,
        num_partitions=config.num_partitions,
        replication_factor=config.replication_factor,
    )

    try:
        response = await admin.create_topics([new_topic])
    except TopicAlreadyExistsError:
        return  # Concurrent creation — idempotent.
    except Exception as e:
        msg = f"Failed to create topic '{config.name}'"
        raise PubSubSetupError(msg) from e

    _raise_on_topic_errors(response)


def _raise_on_topic_errors(response: "Response") -> None:
    """Surface per-topic errors from a ``CreateTopicsResponse``.

    ``AIOKafkaAdminClient.create_topics`` reports failures in the
    response body rather than raising, so the error codes are checked
    here. "Topic already exists" is tolerated (idempotent create).

    :raises PubSubSetupError: for any other per-topic error.
    """
    for entry in getattr(response, "topic_errors", []):
        topic_name, error_code, *rest = entry
        if error_code in (0, TopicAlreadyExistsError.errno):
            continue
        error_message = rest[0] if rest else None
        msg = (
            f"Failed to create topic '{topic_name}': "
            f"error code {error_code} ({error_message})"
        )
        raise PubSubSetupError(msg)
