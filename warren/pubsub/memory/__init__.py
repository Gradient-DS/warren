"""In-process pubsub backend: one broker, one event loop, no infrastructure.

Selected with ``backend: memory`` in ``RuntimeConfig``. Single-process and
non-durable by construction. See ``warren/docs/memory.md``.
"""

from warren.pubsub.memory.broker import Delivery, MemoryBroker, topic_matches
from warren.pubsub.memory.config import MemoryConsumerConfig
from warren.pubsub.memory.connection import MemoryConnectionManager
from warren.pubsub.memory.consumer import MemoryConsumerManager
from warren.pubsub.memory.publisher import MemoryPublisher


__all__ = [
    "Delivery",
    "MemoryBroker",
    "MemoryConnectionManager",
    "MemoryConsumerConfig",
    "MemoryConsumerManager",
    "MemoryPublisher",
    "topic_matches",
]
