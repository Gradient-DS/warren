"""
Pubsub backend factory: select RabbitMQ or Kafka from ``RuntimeConfig``.

This module is the single place that switches on ``config.backend``.
Every runner builds its connection manager, publishers, and consumer
managers through these three functions, so adding or swapping a backend
touches only this file — the runners stay transport-agnostic.

**Lazy backend imports.** The transport implementations
(``warren.pubsub.rabbitmq.aio_pika.*`` / ``warren.pubsub.kafka.aiokafka.*``)
are imported *inside* the function bodies, not at module load. A
``warren[rmq]``-only install therefore never imports ``aiokafka`` (and
vice versa); a missing extra surfaces as the ``OptionalDependencyError``
raised by the implementation sub-package's import guard, the moment that
backend is actually selected.

**Naming convention lives here.** ``create_consumer_manager`` owns the
queue/group naming for all four call sites:

- RabbitMQ: queue ``f"{exchange.name}.{worker_type}"``.
- Kafka: consumer group ``config.kafka.consumer.group_id or
  f"{topic.name}.{worker_type}"``.

For Kafka the group is *pre-resolved here* and passed down via a
per-manager ``KafkaConsumerManagerConfig``, so the convention is owned in
one place (this factory). ``KafkaConsumerManager`` keeps its own
``group_id or "<topic>.<type>"`` fallback for direct construction outside
the runtime, but in the runtime path it always receives a concrete
``group_id`` and never reaches that fallback.
"""

from typing import Any

from warren.common import MessageConsumerInterface
from warren.pubsub.common import (
    ConsumerManagerInterface,
    PublisherInterface,
    RetryConfig,
)
from warren.runtime.config import RuntimeConfig
from warren.workers.messages import ExtractMessageIdentityFunc


def create_connection_manager(config: RuntimeConfig) -> Any:
    """Create the (unset-up) pubsub connection manager for the backend.

    The caller is responsible for ``setup()`` / ``teardown()``.

    :param config: Runtime configuration; ``config.backend`` selects the
        backend.
    :return: ``RMQConnectionManager`` or ``KafkaConnectionManager``.
    :raises OptionalDependencyError: if the selected backend's transport
        extra is not installed.
    """
    if config.backend == "kafka":
        from warren.pubsub.kafka.aiokafka.connection import (
            KafkaConnectionManager,
        )

        return KafkaConnectionManager(config.kafka.connection)

    from warren.pubsub.rabbitmq.aio_pika.connection import (
        RMQConnectionManager,
    )

    return RMQConnectionManager(config.rabbitmq.connection)


def create_publisher(
    config: RuntimeConfig,
    connection_manager: Any,
    *,
    name: str | None = None,
) -> PublisherInterface:
    """Create a fanout publisher onto the backend's job topic/exchange.

    The publisher targets the configured ``jobs`` fanout exchange (RMQ) or
    ``jobs`` topic (Kafka) — both broadcast to every worker type.

    :param config: Runtime configuration; ``config.backend`` selects the
        backend.
    :param connection_manager: Connection manager from
        ``create_connection_manager`` (must match the backend).
    :param name: Optional publisher name for logging.
    :return: An unset-up publisher implementing ``PublisherInterface``.
    :raises OptionalDependencyError: if the selected backend's transport
        extra is not installed.
    """
    if config.backend == "kafka":
        from warren.pubsub.kafka.aiokafka.publisher import KafkaPublisher

        return KafkaPublisher(
            connection_manager,
            config.kafka.topic,
            name=name,
        )

    from warren.pubsub.rabbitmq.aio_pika.publisher import RMQPublisher

    return RMQPublisher(
        connection_manager=connection_manager,
        exchange_config=config.rabbitmq.exchange,
        name=name,
    )


def create_consumer_manager(
    config: RuntimeConfig,
    connection_manager: Any,
    *,
    worker_type: str,
    consumer: MessageConsumerInterface,
    publishers: list[PublisherInterface] | None = None,
    retry_config: RetryConfig | None = None,
    extract_identity_func: ExtractMessageIdentityFunc | None = None,
    publish_hard_failures: bool = True,
) -> ConsumerManagerInterface:
    """Create the consumer manager for ``worker_type`` on the backend.

    Owns the queue/group naming convention for all call sites:

    - RabbitMQ: queue ``f"{exchange.name}.{worker_type}"``.
    - Kafka: consumer group ``config.kafka.consumer.group_id or
      f"{topic.name}.{worker_type}"`` (pre-resolved here).

    The remaining keyword arguments are forwarded verbatim to the backend
    consumer manager (both backends share the same constructor shape), so
    each call site keeps its own ``publishers`` / ``publish_hard_failures``
    / retry behaviour without the factory having to know why.

    :param config: Runtime configuration; ``config.backend`` selects the
        backend.
    :param connection_manager: Connection manager from
        ``create_connection_manager`` (must match the backend).
    :param worker_type: Worker type name — the queue suffix (RMQ) or the
        group suffix (Kafka).
    :param consumer: The worker that processes each message.
    :param publishers: Downstream publishers, or ``None`` for none.
    :param retry_config: Retry policy override (defaults applied by the
        consumer manager when ``None``).
    :param extract_identity_func: Message-identity extractor override.
    :param publish_hard_failures: Whether to publish a hard-failure
        envelope on terminal failure.
    :return: An unset-up consumer manager implementing
        ``ConsumerManagerInterface``.
    :raises OptionalDependencyError: if the selected backend's transport
        extra is not installed.
    """
    if config.backend == "kafka":
        from warren.pubsub.kafka.aiokafka.consumer import KafkaConsumerManager
        from warren.pubsub.kafka.config import KafkaConsumerManagerConfig

        # Pre-resolve the group here so the naming convention is owned by
        # the factory, not split with the manager's internal fallback.
        group_id = (
            config.kafka.consumer.group_id or f"{config.kafka.topic.name}.{worker_type}"
        )
        consumer_config = config.kafka.consumer.model_copy(
            update={"group_id": group_id}
        )
        manager_config = KafkaConsumerManagerConfig(
            topic=config.kafka.topic,
            consumer=consumer_config,
        )

        return KafkaConsumerManager(
            config=manager_config,
            connection_manager=connection_manager,
            consumer=consumer,
            publishers=publishers,
            retry_config=retry_config,
            extract_identity_func=extract_identity_func,
            publish_hard_failures=publish_hard_failures,
        )

    from warren.pubsub.rabbitmq.aio_pika.consumer import RMQConsumerManager
    from warren.pubsub.rabbitmq.config import (
        RMQConsumerManagerConfig,
        RMQQueueConfig,
    )

    exchange_config = config.rabbitmq.exchange
    queue_config = RMQQueueConfig(
        name=f"{exchange_config.name}.{worker_type}",
        durable=True,
    )
    manager_config = RMQConsumerManagerConfig(
        exchange=exchange_config,
        queue=queue_config,
        consumer=config.rabbitmq.consumer,
    )

    return RMQConsumerManager(
        config=manager_config,
        connection_manager=connection_manager,
        consumer=consumer,
        publishers=publishers,
        retry_config=retry_config,
        extract_identity_func=extract_identity_func,
        publish_hard_failures=publish_hard_failures,
    )
