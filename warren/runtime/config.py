"""
Runtime configuration for the distributed processing framework.

``RuntimeConfig`` composes the framework's own RabbitMQ config models
with MongoDB, Redis, and retry settings. It is the single config
object that ``DefaultWorkerRunner`` and ``create_runtime_infrastructure``
consume — no separate E2E / prod config classes needed.

Load from YAML via ``RuntimeConfig.from_yaml(path)``.
"""

from pathlib import Path

import yaml
from pydantic import BaseModel

from warren.pubsub.rabbitmq.config import (
    RMQConnectionConfig,
    RMQConsumerConfig,
    RMQExchangeConfig,
)


class RuntimeRMQConfig(BaseModel):
    """RabbitMQ settings composed from framework config models."""

    connection: RMQConnectionConfig = RMQConnectionConfig()
    exchange: RMQExchangeConfig = RMQExchangeConfig(name="jobs", type="fanout")
    consumer: RMQConsumerConfig = RMQConsumerConfig()


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

    :param rabbitmq: RabbitMQ connection, exchange, and consumer settings.
    :param mongodb: MongoDB connection settings.
    :param redis: Redis connection settings.
    :param retry: Retry worker toggle and collection name.
    """

    rabbitmq: RuntimeRMQConfig = RuntimeRMQConfig()
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
