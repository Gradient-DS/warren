"""
Pure-data configuration models for the Kafka pubsub layer.

These models are intentionally free of any ``aiokafka`` dependency: they
describe *what* a connection, topic, or consumer should look like, and
they can be loaded from YAML, serialized, compared, or reused (e.g.
embedded in ``RuntimeConfig``) without pulling in the transport library.
The behavior of acting on these configs — starting clients, ensuring
topics — lives in ``connection.py``, ``topology.py``, ``consumer.py``,
and ``publisher.py`` under the ``aiokafka`` sub-package.

The transport-agnostic retry policy (``RetryConfig``) lives in
:mod:`warren.pubsub.common` and is shared with the RabbitMQ backend.
"""

from typing import Literal

from pydantic import BaseModel, Field


class KafkaConnectionConfig(BaseModel):
    """Cluster-level parameters shared by all Kafka clients.

    The SSL fields are file *paths* only — the ``ssl.SSLContext`` is
    built in the ``aiokafka`` implementation sub-package, keeping this
    model pure-data.
    """

    bootstrap_servers: list[str] = Field(default_factory=lambda: ["localhost:9092"])
    security_protocol: Literal["PLAINTEXT", "SSL"] = "PLAINTEXT"
    ssl_cafile: str | None = None
    ssl_certfile: str | None = None
    ssl_keyfile: str | None = None
    client_id: str | None = None


class KafkaTopicConfig(BaseModel):
    """A Kafka topic — plays the role of a fanout exchange.

    :param name: Topic name.
    :param num_partitions: Partition count, used only when creating.
    :param replication_factor: Replication factor, used only when creating.
    :param create_if_missing: Create the topic if it does not exist.
        Enable locally; disable on platforms where topics are
        provisioned out-of-band (e.g. via a console).
    """

    name: str
    num_partitions: int = 6
    replication_factor: int = 1
    create_if_missing: bool = False


class KafkaConsumerConfig(BaseModel):
    """Consumer-group parameters for a single consumer manager.

    :param group_id: Consumer group name. When None, the consumer
        manager derives ``"<topic>.<worker_type>"`` — one group per
        worker type on the topic (fanout parity). Set explicitly on
        platforms with pre-assigned group names.
    :param auto_offset_reset: Where a *new* group starts reading.
    :param max_poll_interval_ms: Max time between polls before the
        broker evicts the consumer from the group.
    :param session_timeout_ms: Heartbeat session timeout.
    :param on_shutdown_timeout: Seconds to wait for the in-flight
        message during shutdown (mirrors ``RMQConsumerConfig``).
    """

    group_id: str | None = None
    auto_offset_reset: Literal["earliest", "latest"] = "earliest"
    max_poll_interval_ms: int = 600_000
    session_timeout_ms: int = 45_000
    on_shutdown_timeout: float = 30.0


class KafkaConsumerManagerConfig(BaseModel):
    topic: KafkaTopicConfig
    consumer: KafkaConsumerConfig
