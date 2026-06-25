"""
Kafka publisher implementation.
"""

from typing import TYPE_CHECKING, Any

import json

from basics.logging_utils import summarize_exception_chain

from warren.pubsub.base import BasePublisher
from warren.pubsub.common import (
    PublishFailureException,
    PubSubSetupError,
    Route,
    RouteFunc,
)
from warren.pubsub.kafka.aiokafka.connection import (
    KafkaConnectionManager,
)
from warren.pubsub.kafka.aiokafka.topology import (
    ensure_topic,
)
from warren.pubsub.kafka.config import (
    KafkaTopicConfig,
)


if TYPE_CHECKING:
    from aiokafka import AIOKafkaProducer


class KafkaPublisher(BasePublisher):
    """
    Kafka publisher for a single topic.

    A topic plays the role of a fanout exchange in the Kafka backend, so
    the publisher is fanout-only: routes are rejected with ``ValueError``
    (the parity of ``RMQPublisher``'s fanout mode). Messages are sent
    without a key, so the cluster distributes them round-robin over the
    topic's partitions.
    """

    def __init__(
        self,
        connection_manager: KafkaConnectionManager,
        topic_config: KafkaTopicConfig,
        *,
        route: Route | None = None,
        route_func: RouteFunc | None = None,
        content_type: str = "application/json",
        name: str | None = None,
    ) -> None:
        if route is not None or route_func is not None:
            msg = (
                "Kafka topics are fanout-only: "
                "'route' and 'route_func' are not supported."
            )
            raise ValueError(msg)

        super().__init__(name=name)

        self._connection_manager = connection_manager
        self._topic_config = topic_config
        self._content_type = content_type

        # Instantiated after setup is called.
        self._producer: AIOKafkaProducer | None = None

    async def setup(self) -> None:
        """Ensure the publisher's topic exists and start the producer.

        The producer is idempotent and waits for full-ISR acknowledgment
        (``acks="all"``) — the durability counterpart of RabbitMQ's
        persistent delivery mode.

        On partial failure the caller is responsible for cleanup via
        ``teardown()`` (idempotent, best-effort).

        :raises PubSubSetupError: if the topic or producer cannot be
            set up.
        """
        try:
            await ensure_topic(self._connection_manager, self._topic_config)
            producer = self._connection_manager.create_producer(
                acks="all",
                enable_idempotence=True,
            )
            # Published onto the instance the moment it exists, so a
            # partial setup leaves truthful state for teardown().
            self._producer = producer
            await producer.start()
        except Exception as e:
            msg = (
                f"{self}: Publisher setup failed for topic '{self._topic_config.name}'"
            )
            raise PubSubSetupError(msg) from e

    async def teardown(self) -> None:
        if self._producer is not None:
            try:
                await self._producer.stop()
            except Exception as e:
                self._log.warning(
                    f"Error stopping publisher producer: {summarize_exception_chain(e)}"
                )

    async def __call__(self, message: dict[str, Any]) -> None:
        if self._producer is None:
            msg = "Must call setup() before publishing."
            raise RuntimeError(msg)

        body = json.dumps(message).encode()

        try:
            # No key — round-robin partition assignment (fanout parity).
            await self._producer.send_and_wait(
                self._topic_config.name,
                value=body,
                headers=[("content_type", self._content_type.encode())],
            )
        except Exception as e:
            msg = f"Failed to publish to topic '{self._topic_config.name}': {e}"
            raise PublishFailureException(msg) from e
