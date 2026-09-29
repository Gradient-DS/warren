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

from typing import Any, Literal, Self

import os
import re
from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, Field, SecretStr, model_validator

from warren.pubsub.common import RetryConfig
from warren.pubsub.kafka.config import (
    KafkaConnectionConfig,
    KafkaConsumerConfig,
    KafkaTopicConfig,
)
from warren.pubsub.memory.config import MemoryConsumerConfig
from warren.pubsub.rabbitmq.config import (
    RMQConnectionConfig,
    RMQConsumerConfig,
)
from warren.workers.health import HealthConfig


class RuntimeRMQConfig(BaseModel):
    """RabbitMQ connection + consumer settings (environment-specific).

    Exchange *definitions* (topology) live on the ``PipelineSpec``, not here —
    see tasks/routing-design.md D3. Config holds only what varies per
    deployment: where the broker is and how to consume.
    """

    model_config = ConfigDict(extra="forbid")

    connection: RMQConnectionConfig = RMQConnectionConfig()
    consumer: RMQConsumerConfig = RMQConsumerConfig()


class RuntimeKafkaConfig(BaseModel):
    """Kafka settings composed from framework config models.

    The default topic is ``jobs`` — the Kafka counterpart of the RMQ
    ``jobs`` fanout exchange.
    """

    model_config = ConfigDict(extra="forbid")

    connection: KafkaConnectionConfig = KafkaConnectionConfig()
    topic: KafkaTopicConfig = KafkaTopicConfig(name="jobs")
    consumer: KafkaConsumerConfig = KafkaConsumerConfig()


class MongoDBConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    uri: SecretStr | None = None
    host: str = "localhost"
    port: int = 27017
    database: str = "distributed_processing"
    username: str | None = None
    password: SecretStr | None = None
    auth_source: str | None = None
    tls: bool = False
    max_pool_size: int | None = None
    server_selection_timeout_ms: int | None = None


class RedisConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    url: SecretStr | None = None
    host: str = "localhost"
    port: int = 6379
    username: str | None = None
    password: SecretStr | None = None
    db: int = 0
    ssl: bool = False
    max_connections: int | None = None
    socket_timeout: float | None = None


class CacheConfig(BaseModel):
    """Expiry for cached payloads, in seconds."""

    model_config = ConfigDict(extra="forbid")

    cache_ttl_seconds: int = Field(default=3600, gt=0)


class RetentionConfig(BaseModel):
    """MongoDB job record expiry, in seconds."""

    model_config = ConfigDict(extra="forbid")

    job_records_ttl_seconds: int | None = Field(default=None, ge=0)
    job_records_max_age_seconds: int | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def validate_max_age(self) -> Self:
        ttl = self.job_records_ttl_seconds
        max_age = self.job_records_max_age_seconds
        if ttl is not None and max_age is not None and max_age < ttl:
            msg = "job_records_max_age_seconds must be >= job_records_ttl_seconds"
            raise ValueError(msg)
        return self


class RuntimeRetryConfig(BaseModel):
    """Retry worker toggle plus the retry policy every consumer manager applies.

    :param enabled: Whether the retry worker runs.
    :param collection_name: MongoDB collection for messages awaiting retry.
    :param policy: Defaults and caps for soft-failure retries. Passed to
        every consumer manager the runner builds, so ``max_delay_cap`` and
        friends are deployment settings rather than library constants.
    """

    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    collection_name: str = "retries"
    policy: RetryConfig = RetryConfig()


class StartupConfig(BaseModel):
    """Connection attempts and bounded exponential backoff at startup."""

    model_config = ConfigDict(extra="forbid")

    attempts: int = Field(default=1, ge=1)
    initial_delay_seconds: float = Field(default=1.0, ge=0, allow_inf_nan=False)
    max_delay_seconds: float = Field(default=30.0, ge=0, allow_inf_nan=False)


class ScopingConfig(BaseModel):
    """Content namespace settings."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    required: bool = True
    database_prefix: str = "wr_"


class RuntimeConfig(BaseModel):
    """Top-level runtime configuration.

    :param backend: Pubsub backend the runtime wires up. Explicit (not
        presence-based): defaults to ``"rabbitmq"`` so every existing
        YAML stays valid, and only the matching backend section is used.
        ``"memory"`` runs everything in one process with no infrastructure
        and needs no external connection settings (see ``warren/docs/memory.md``).
    :param rabbitmq: RabbitMQ connection, exchange, and consumer settings.
    :param kafka: Kafka connection, topic, and consumer settings.
    :param memory: In-process consumer settings.
    :param mongodb: MongoDB connection settings.
    :param redis: Redis connection settings.
    :param documents: Fetched payload cache expiry.
    :param results: Processing result cache expiry.
    :param retention: MongoDB job record expiry.
    :param retry: Retry worker toggle and collection name.
    :param health: Watchdog and readiness endpoint settings.
    :param startup: Connection attempts and backoff settings.
    """

    model_config = ConfigDict(extra="forbid")

    backend: Literal["rabbitmq", "kafka", "memory"] = "rabbitmq"
    rabbitmq: RuntimeRMQConfig = RuntimeRMQConfig()
    kafka: RuntimeKafkaConfig = RuntimeKafkaConfig()
    memory: MemoryConsumerConfig = MemoryConsumerConfig()
    mongodb: MongoDBConfig = MongoDBConfig()
    redis: RedisConfig = RedisConfig()
    documents: CacheConfig = CacheConfig(cache_ttl_seconds=86400)
    results: CacheConfig = CacheConfig()
    retention: RetentionConfig = RetentionConfig()
    retry: RuntimeRetryConfig = RuntimeRetryConfig()
    health: HealthConfig = HealthConfig()
    startup: StartupConfig = StartupConfig()
    scoping: ScopingConfig = ScopingConfig()

    @classmethod
    def from_yaml(cls, path: Path) -> "RuntimeConfig":
        """Load configuration from a YAML file.

        :param path: Path to the YAML config file.
        :return: Populated RuntimeConfig instance.
        :raises FileNotFoundError: If the config file doesn't exist.
        """
        with Path(path).open() as f:
            data = yaml.safe_load(f)
        return cls.model_validate(_expand_env(data))


def _expand_env(value: Any) -> Any:
    """Expand environment references in YAML string values."""
    if isinstance(value, str):

        def replace(match: re.Match[str]) -> str:
            name = match.group(1)
            try:
                return os.environ[name]
            except KeyError:
                msg = f"Environment variable {name!r} is not set"
                raise ValueError(msg) from None

        return re.sub(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}", replace, value)
    if isinstance(value, dict):
        return {key: _expand_env(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_expand_env(item) for item in value]
    return value
