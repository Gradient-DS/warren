"""
Runtime configuration for the distributed processing framework.

``RuntimeConfig`` composes the framework's own RabbitMQ and Kafka config
models with MongoDB, Redis, and retry settings. It is the single config
object that ``DefaultWorkerRunner`` and ``create_runtime_infrastructure``
consume — no separate E2E / prod config classes needed.

The ``backend`` field selects which pubsub backend the runtime wires up.
Both backends' config models are pure-data (no ``aio_pika`` / ``aiokafka``
import), so importing them here is safe regardless of which transport
extra is installed — the transport library is only imported lazily by the
backend factory in :mod:`warren.runtime.backends`.

Load from YAML via ``RuntimeConfig.from_yaml(path)``.
"""

from typing import Literal

from pathlib import Path

import yaml
from pydantic import BaseModel

from warren.pubsub.kafka.config import (
    KafkaConnectionConfig,
    KafkaConsumerConfig,
    KafkaTopicConfig,
)
from warren.pubsub.rabbitmq.config import (
    RMQConnectionConfig,
    RMQConsumerConfig,
)


class RuntimeRMQConfig(BaseModel):
    """RabbitMQ connection + consumer settings (environment-specific).

    Exchange *definitions* (topology) live on the ``PipelineSpec``, not here —
    see tasks/routing-design.md D3. Config holds only what varies per
    deployment: where the broker is and how to consume.
    """

    connection: RMQConnectionConfig = RMQConnectionConfig()
    consumer: RMQConsumerConfig = RMQConsumerConfig()


class RuntimeKafkaConfig(BaseModel):
    """Kafka settings composed from framework config models.

    The default topic is ``jobs`` — the Kafka counterpart of the RMQ
    ``jobs`` fanout exchange.
    """

    connection: KafkaConnectionConfig = KafkaConnectionConfig()
    topic: KafkaTopicConfig = KafkaTopicConfig(name="jobs")
    consumer: KafkaConsumerConfig = KafkaConsumerConfig()


class MongoDBConfig(BaseModel):
    host: str = "localhost"
    port: int = 27017
    database: str = "distributed_processing"


class RedisConfig(BaseModel):
    host: str = "localhost"
    port: int = 6379


class RuntimeRetryConfig(BaseModel):
    enabled: bool = False
    collection_name: str = "retries"


class RuntimeConfig(BaseModel):
    """Top-level runtime configuration.

    :param backend: Pubsub backend the runtime wires up. Explicit (not
        presence-based): defaults to ``"rabbitmq"`` so every existing
        YAML stays valid, and only the matching backend section is used.
    :param rabbitmq: RabbitMQ connection, exchange, and consumer settings.
    :param kafka: Kafka connection, topic, and consumer settings.
    :param mongodb: MongoDB connection settings.
    :param redis: Redis connection settings.
    :param retry: Retry worker toggle and collection name.
    """

    backend: Literal["rabbitmq", "kafka"] = "rabbitmq"
    rabbitmq: RuntimeRMQConfig = RuntimeRMQConfig()
    kafka: RuntimeKafkaConfig = RuntimeKafkaConfig()
    mongodb: MongoDBConfig = MongoDBConfig()
    redis: RedisConfig = RedisConfig()
    retry: RuntimeRetryConfig = RuntimeRetryConfig()

    @classmethod
    def from_yaml(cls, path: Path) -> "RuntimeConfig":
        """Load configuration from a YAML file.

        :param path: Path to the YAML config file.
        :return: Populated RuntimeConfig instance.
        :raises FileNotFoundError: If the config file doesn't exist.
        """
        with Path(path).open() as f:
            data = yaml.safe_load(f)
        return cls.model_validate(data)
