"""Warren: a message-driven framework for distributed document processing.

The top-level package holds the contracts that pubsub and workers share: the
message consumer protocols, the failure exceptions and the base errors.
"""

from warren.common import (
    AsyncMessageConsumerInterface,
    HardFailureException,
    MessageConsumerIdentity,
    MessageConsumerInterface,
    SoftFailureException,
    SyncMessageConsumerInterface,
)
from warren.constants import PUBLISHER_ORIGIN_TYPE
from warren.exceptions import OptionalDependencyError, WarrenError


__all__ = [
    "PUBLISHER_ORIGIN_TYPE",
    "AsyncMessageConsumerInterface",
    "HardFailureException",
    "MessageConsumerIdentity",
    "MessageConsumerInterface",
    "OptionalDependencyError",
    "SoftFailureException",
    "SyncMessageConsumerInterface",
    "WarrenError",
]
