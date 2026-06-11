"""
Kafka pubsub layer.

The top-level module exposes only the **protocol-level configs** —
pure-data Pydantic models that describe the Kafka topology and consumer
settings. They have no dependency on any specific Python client library
and can be loaded, serialized, and reused freely (importing this module
does not require ``aiokafka``).

Concrete client implementations live in sub-packages:

- :mod:`...kafka.aiokafka` — ``aiokafka``-based managers and helpers
  (requires the ``warren[kafka]`` extra).

To use the ``aiokafka`` implementation, import explicitly from that
sub-package, e.g.::

    from warren.pubsub.kafka.aiokafka import (
        KafkaConnectionManager,
        KafkaConsumerManager,
        KafkaPublisher,
    )
"""

from warren.pubsub.kafka.config import (
    KafkaConnectionConfig,
    KafkaConsumerConfig,
    KafkaConsumerManagerConfig,
    KafkaTopicConfig,
)


__all__ = [
    "KafkaConnectionConfig",
    "KafkaConsumerConfig",
    "KafkaConsumerManagerConfig",
    "KafkaTopicConfig",
]
