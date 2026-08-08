"""Unit tests for the pubsub backend factory (``warren.runtime.backends``).

No broker: the factory only *constructs* connection managers, publishers,
and consumer managers (it never calls ``setup()``), so a sentinel object
stands in for the connection manager. The tests assert:

- backend selection returns the right concrete classes,
- the queue/group naming convention the factory owns:
  RMQ queue ``jobs.<worker_type>``, Kafka group ``<topic>.<worker_type>``,
- an explicit ``kafka.consumer.group_id`` overrides the derived group,
- Kafka is fanout-only: a topic/direct exchange raises ``WarrenError``.

The naming convention is the load-bearing behavior here — these tests pin
it so it stays owned by the factory in one place.
"""

import pytest

from warren.exceptions import WarrenError
from warren.pubsub.kafka.aiokafka.connection import KafkaConnectionManager
from warren.pubsub.kafka.aiokafka.consumer import KafkaConsumerManager
from warren.pubsub.kafka.aiokafka.publisher import KafkaPublisher
from warren.pubsub.kafka.config import KafkaConsumerConfig
from warren.pubsub.rabbitmq.aio_pika.connection import RMQConnectionManager
from warren.pubsub.rabbitmq.aio_pika.consumer import RMQConsumerManager
from warren.pubsub.rabbitmq.aio_pika.publisher import RMQPublisher
from warren.pubsub.rabbitmq.config import RMQExchangeConfig
from warren.runtime import backends
from warren.runtime.config import RuntimeConfig, RuntimeKafkaConfig


WORKER_TYPE = "document_parser"

FANOUT = RMQExchangeConfig(name="jobs", type="fanout")
TOPIC = RMQExchangeConfig(name="jobs", type="topic")
DIRECT = RMQExchangeConfig(name="jobs", type="direct")


class _FakeConsumer:
    """MessageConsumerInterface stand-in: only ``type``/``name`` are read."""

    @property
    def type(self) -> str:
        return WORKER_TYPE

    @property
    def name(self) -> str:
        return "parser-1"

    async def __call__(self, message: dict) -> dict | None:
        return None


# ---------------------------------------------------------------------------
# Connection manager
# ---------------------------------------------------------------------------


def test_connection_manager_rabbitmq() -> None:
    config = RuntimeConfig(backend="rabbitmq")
    manager = backends.create_connection_manager(config)
    assert isinstance(manager, RMQConnectionManager)


def test_connection_manager_kafka() -> None:
    config = RuntimeConfig(backend="kafka")
    manager = backends.create_connection_manager(config)
    assert isinstance(manager, KafkaConnectionManager)


# ---------------------------------------------------------------------------
# Publisher
# ---------------------------------------------------------------------------


def test_publisher_rabbitmq() -> None:
    config = RuntimeConfig(backend="rabbitmq")
    publisher = backends.create_publisher(config, object(), exchange=FANOUT, name="x")
    assert isinstance(publisher, RMQPublisher)
    # The name is forwarded to the publisher (woven into its logger name).
    assert publisher._pybase_logger_name == "[RMQPublisher] x"


def test_publisher_kafka() -> None:
    config = RuntimeConfig(backend="kafka")
    publisher = backends.create_publisher(config, object(), exchange=FANOUT, name="x")
    assert isinstance(publisher, KafkaPublisher)
    # The name is forwarded to the publisher (woven into its logger name).
    assert publisher._pybase_logger_name == "[KafkaPublisher] x"


def test_publisher_kafka_rejects_non_fanout() -> None:
    config = RuntimeConfig(backend="kafka")
    with pytest.raises(WarrenError, match="fanout pipelines only"):
        backends.create_publisher(config, object(), exchange=TOPIC)


# ---------------------------------------------------------------------------
# Consumer manager — class selection + naming convention
# ---------------------------------------------------------------------------


def test_consumer_manager_rabbitmq_queue_name() -> None:
    config = RuntimeConfig(backend="rabbitmq")
    manager = backends.create_consumer_manager(
        config,
        object(),
        exchange=FANOUT,
        worker_type=WORKER_TYPE,
        consumer=_FakeConsumer(),
    )

    assert isinstance(manager, RMQConsumerManager)
    # RMQ queue name: f"{exchange.name}.{worker_type}".
    assert manager._config.queue.name == f"jobs.{WORKER_TYPE}"


def test_consumer_manager_rabbitmq_binding_key_threaded() -> None:
    config = RuntimeConfig(backend="rabbitmq")
    manager = backends.create_consumer_manager(
        config,
        object(),
        exchange=TOPIC,
        worker_type=WORKER_TYPE,
        binding_key="pdf_document",
        consumer=_FakeConsumer(),
    )

    assert manager._config.queue.routing_key == "pdf_document"


def test_consumer_manager_kafka_resolved_group_id() -> None:
    config = RuntimeConfig(backend="kafka")
    manager = backends.create_consumer_manager(
        config,
        object(),
        exchange=FANOUT,
        worker_type=WORKER_TYPE,
        consumer=_FakeConsumer(),
    )

    assert isinstance(manager, KafkaConsumerManager)
    # Kafka group id derived by the factory: f"{topic.name}.{worker_type}".
    assert manager._config.consumer.group_id == f"jobs.{WORKER_TYPE}"


def test_consumer_manager_kafka_explicit_group_id_wins() -> None:
    config = RuntimeConfig(
        backend="kafka",
        kafka=RuntimeKafkaConfig(
            consumer=KafkaConsumerConfig(group_id="preassigned-group"),
        ),
    )
    manager = backends.create_consumer_manager(
        config,
        object(),
        exchange=FANOUT,
        worker_type=WORKER_TYPE,
        consumer=_FakeConsumer(),
    )

    # Explicit group id overrides the derived convention.
    assert manager._config.consumer.group_id == "preassigned-group"


def test_consumer_manager_kafka_rejects_non_fanout() -> None:
    config = RuntimeConfig(backend="kafka")
    with pytest.raises(WarrenError, match="fanout pipelines only"):
        backends.create_consumer_manager(
            config,
            object(),
            exchange=DIRECT,
            worker_type=WORKER_TYPE,
            consumer=_FakeConsumer(),
        )


def test_consumer_manager_forwards_publish_hard_failures() -> None:
    config = RuntimeConfig(backend="rabbitmq")
    manager = backends.create_consumer_manager(
        config,
        object(),
        exchange=FANOUT,
        worker_type=WORKER_TYPE,
        consumer=_FakeConsumer(),
        publish_hard_failures=False,
    )

    assert manager._publish_hard_failures is False
