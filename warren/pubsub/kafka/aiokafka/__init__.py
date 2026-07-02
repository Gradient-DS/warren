"""
``aiokafka``-based implementation of the Kafka pubsub layer.

The protocol-level configs in :mod:`...kafka.config` describe *what*
to set up; the modules here use ``aiokafka`` to actually do it.

``aiokafka`` is an optional dependency: importing this sub-package
without it installed raises
:class:`~warren.exceptions.OptionalDependencyError` pointing at the
``warren[kafka]`` extra.
"""

from warren.exceptions import OptionalDependencyError


try:
    import aiokafka  # noqa: F401
except ImportError as e:
    _PACKAGE = "aiokafka"
    raise OptionalDependencyError(
        _PACKAGE,
        install_hint='pip install "warren[kafka]"',
    ) from e

from warren.pubsub.kafka.aiokafka.connection import KafkaConnectionManager
from warren.pubsub.kafka.aiokafka.consumer import KafkaConsumerManager
from warren.pubsub.kafka.aiokafka.publisher import KafkaPublisher
from warren.pubsub.kafka.aiokafka.topology import ensure_topic


__all__ = [
    "KafkaConnectionManager",
    "KafkaConsumerManager",
    "KafkaPublisher",
    "ensure_topic",
]
