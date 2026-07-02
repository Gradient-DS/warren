"""
Pubsub backend factory: select RabbitMQ or Kafka from ``RuntimeConfig``.

This module is the single place that switches on ``config.backend``.
Every runner builds its connection manager, publishers, and consumer
managers through these three functions, so adding or swapping a backend
touches only this file — the runners stay transport-agnostic.

**Kafka is fanout-only (for now).** The flexible-routing model
(``topic``/``direct`` exchanges, binding keys, route functions — see
``warren/docs/routing.md``) maps onto RabbitMQ exchanges. A Kafka topic
with one consumer group per worker type gives exactly Warren's *fanout*
semantics (every worker type sees every message and self-selects), so a
fanout pipeline runs on either backend unchanged. A pipeline whose
exchange is ``topic`` or ``direct`` on ``backend: kafka`` fails fast here
with a clear error; broker-side routing on Kafka is deferred.

On a fanout pipeline route functions compute keys the exchange ignores
(``observer_route_func`` is ``None`` on fanout; ``ReplayRouter`` replays
``""``), so the Kafka paths drop ``route``/``route_func`` — semantically
identical, and ``KafkaPublisher`` would reject them.

**Lazy backend imports.** The transport implementations
(``warren.pubsub.rabbitmq.aio_pika.*`` / ``warren.pubsub.kafka.aiokafka.*``)
are imported *inside* the function bodies, not at module load. A
``warren[rmq]``-only install therefore never imports ``aiokafka`` (and
vice versa); a missing extra surfaces as the ``OptionalDependencyError``
raised by the implementation sub-package's import guard, the moment that
backend is actually selected.

**Naming convention lives here.** ``create_consumer_manager`` owns the
queue/group naming for all call sites:

- RabbitMQ: queue ``f"{exchange.name}.{worker_type}"``.
- Kafka: consumer group ``config.kafka.consumer.group_id or
  f"{topic.name}.{worker_type}"``.

For Kafka the group is *pre-resolved here* and passed down via a
per-manager ``KafkaConsumerManagerConfig``, so the convention is owned in
one place (this factory). ``KafkaConsumerManager`` keeps its own
``group_id or "<topic>.<type>"`` fallback for direct construction outside
the runtime, but in the runtime path it always receives a concrete
``group_id`` and never reaches that fallback.

Note the suffix sources differ: the factory derives the group from its
``worker_type`` argument, whereas the manager's fallback derives it from
``consumer.type``. These are the same value in the runtime path (callers
pass ``worker_type=consumer.type``) but are not guaranteed interchangeable
for direct construction.
"""

from typing import Any

from warren.common import MessageConsumerInterface
from warren.exceptions import WarrenError
from warren.pubsub.common import (
    ConsumerManagerInterface,
    PublisherInterface,
    RetryConfig,
    Route,
    RouteFunc,
)
from warren.pubsub.rabbitmq.config import RMQExchangeConfig
from warren.runtime.config import RuntimeConfig
from warren.workers.messages import ExtractMessageIdentityFunc


def _require_kafka_fanout(exchange: RMQExchangeConfig) -> None:
    """Fail fast when a routed pipeline selects the Kafka backend."""
    if exchange.type != "fanout":
        msg = (
            f"Kafka supports fanout pipelines only: exchange "
            f"'{exchange.name}' has type '{exchange.type}'. Topic/direct "
            f"routing is RabbitMQ-only for now — see warren/docs/routing.md."
        )
        raise WarrenError(msg)


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
    exchange: RMQExchangeConfig,
    route: Route | None = None,
    route_func: RouteFunc | None = None,
    name: str | None = None,
) -> PublisherInterface:
    """Create a publisher onto ``exchange`` for the configured backend.

    :param config: Runtime configuration; ``config.backend`` selects the
        backend.
    :param connection_manager: Connection manager from
        ``create_connection_manager`` (must match the backend).
    :param exchange: The pipeline exchange to publish to (from the
        ``PipelineSpec``, or a framework-derived observer exchange).
    :param route: Static routing key (RabbitMQ topic/direct only).
    :param route_func: Per-message routing function (RabbitMQ
        topic/direct; a no-op on fanout, dropped on Kafka — see module
        docstring).
    :param name: Optional publisher name for logging.
    :return: An unset-up publisher implementing ``PublisherInterface``.
    :raises WarrenError: if ``exchange`` is not fanout on Kafka.
    :raises OptionalDependencyError: if the selected backend's transport
        extra is not installed.
    """
    if config.backend == "kafka":
        from warren.pubsub.kafka.aiokafka.publisher import KafkaPublisher

        _require_kafka_fanout(exchange)
        # Fanout ignores routing keys, so route/route_func are inert here
        # and Kafka has no per-message keys to compute — dropped.
        return KafkaPublisher(
            connection_manager,
            config.kafka.topic,
            name=name,
        )

    from warren.pubsub.rabbitmq.aio_pika.publisher import RMQPublisher

    return RMQPublisher(
        connection_manager=connection_manager,
        exchange_config=exchange,
        route=route,
        route_func=route_func,
        name=name,
    )


def create_consumer_manager(
    config: RuntimeConfig,
    connection_manager: Any,
    *,
    exchange: RMQExchangeConfig,
    worker_type: str,
    consumer: MessageConsumerInterface,
    binding_key: str | None = None,
    data_publisher: PublisherInterface | None = None,
    control_publisher: PublisherInterface | None = None,
    observer_publisher: PublisherInterface | None = None,
    retry_config: RetryConfig | None = None,
    extract_identity_func: ExtractMessageIdentityFunc | None = None,
    publish_hard_failures: bool = True,
) -> ConsumerManagerInterface:
    """Create the consumer manager for ``worker_type`` on the backend.

    Owns the queue/group naming convention for all call sites:

    - RabbitMQ: queue ``f"{exchange.name}.{worker_type}"``, bound with
      ``binding_key``.
    - Kafka: consumer group ``config.kafka.consumer.group_id or
      f"{topic.name}.{worker_type}"`` (pre-resolved here); ``binding_key``
      is ``None`` on a fanout pipeline and unused.

    The publisher split (data / control / observer — see
    ``warren/docs/routing.md``) is forwarded verbatim to the backend
    consumer manager; both backends share the same constructor shape.

    :param config: Runtime configuration; ``config.backend`` selects the
        backend.
    :param connection_manager: Connection manager from
        ``create_connection_manager`` (must match the backend).
    :param exchange: The exchange this worker consumes from.
    :param worker_type: Worker type name — the queue suffix (RMQ) or the
        group suffix (Kafka).
    :param consumer: The worker that processes each message.
    :param binding_key: Queue binding pattern (RabbitMQ topic/direct
        only; ``None`` on fanout).
    :param data_publisher: Downstream publisher for successful results,
        or ``None`` if the worker is terminal.
    :param control_publisher: Publisher for lifecycle envelopes
        (soft/hard-failure), or ``None`` for seek-back/requeue fallback.
    :param observer_publisher: Publisher echoing successful results to
        the observer exchange (RabbitMQ direct pipelines only).
    :param retry_config: Retry policy override (defaults applied by the
        consumer manager when ``None``).
    :param extract_identity_func: Message-identity extractor override.
    :param publish_hard_failures: Whether to publish a hard-failure
        envelope on terminal failure.
    :return: An unset-up consumer manager implementing
        ``ConsumerManagerInterface``.
    :raises WarrenError: if ``exchange`` is not fanout on Kafka.
    :raises OptionalDependencyError: if the selected backend's transport
        extra is not installed.
    """
    if config.backend == "kafka":
        from warren.pubsub.kafka.aiokafka.consumer import KafkaConsumerManager
        from warren.pubsub.kafka.config import KafkaConsumerManagerConfig

        _require_kafka_fanout(exchange)

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
            data_publisher=data_publisher,
            control_publisher=control_publisher,
            observer_publisher=observer_publisher,
            retry_config=retry_config,
            extract_identity_func=extract_identity_func,
            publish_hard_failures=publish_hard_failures,
        )

    from warren.pubsub.rabbitmq.aio_pika.consumer import RMQConsumerManager
    from warren.pubsub.rabbitmq.config import (
        RMQConsumerManagerConfig,
        RMQQueueConfig,
    )

    queue_config = RMQQueueConfig(
        name=f"{exchange.name}.{worker_type}",
        durable=True,
        routing_key=binding_key,
    )
    manager_config = RMQConsumerManagerConfig(
        exchange=exchange,
        queue=queue_config,
        consumer=config.rabbitmq.consumer,
    )

    return RMQConsumerManager(
        config=manager_config,
        connection_manager=connection_manager,
        consumer=consumer,
        data_publisher=data_publisher,
        control_publisher=control_publisher,
        observer_publisher=observer_publisher,
        retry_config=retry_config,
        extract_identity_func=extract_identity_func,
        publish_hard_failures=publish_hard_failures,
    )
