"""
Pubsub-specific interfaces and exceptions.

Defines the protocols and types used within the pubsub package hierarchy.
For shared contracts between pubsub and workers, see distributed/common.py.
"""

from dataclasses import dataclass
from typing import Protocol, Dict, List

from document_processing.distributed.warren.exceptions import WarrenError


class PublishFailureException(WarrenError):
    """Failed to publish message to downstream queue."""

    pass


class PubSubSetupError(WarrenError):
    """Failed to set up a pubsub resource (channel, exchange, queue, publisher).

    Raised by the topology helpers and the publisher/consumer ``setup()``
    paths to give a transport-layer failure warren-specific context
    (which exchange/queue/publisher) before it reaches the runner's
    higher-level setup-phase wrapping.
    """

    pass


@dataclass(frozen=True)
class Route:
    """
    A transport-agnostic route to a destination, specified by the key.
    Can be extended to include other routing information if necessary.

    :param key: The routing key.
    """

    key: str


class RouteFunc(Protocol):
    async def __call__(self, message: Dict) -> List[Route]:
        """
        Resolve the routes for a message. Can route to multiple
        destinations, for example, a processing destination and a
        logging destination.

        :param message: Message body as a dictionary.

        :return: List of routes.
        """
        ...


class PublisherInterface(Protocol):
    """Interface for publishers. Each publisher owns its own routing logic."""

    async def setup(self) -> None:
        """Set up the publisher."""
        ...

    async def __call__(self, message: Dict) -> None:
        """
        Publish a message.

        :param message: Message body as a dictionary.
        """
        ...

    async def teardown(self) -> None:
        """Tear down the publisher."""
        ...


class ConsumerManagerInterface(Protocol):
    async def setup(self) -> None:
        """Set up consumption."""
        ...

    async def start_consuming(self) -> None:
        """Start consuming."""
        ...

    async def stop_consuming(self) -> None:
        """Stop the consumption."""
        ...
