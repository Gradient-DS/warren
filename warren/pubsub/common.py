"""
Pubsub-specific interfaces and exceptions.

Defines the protocols and types used within the pubsub package hierarchy.
For shared contracts between pubsub and workers, see distributed/common.py.
"""

from typing import Literal, Protocol

from dataclasses import dataclass

from pydantic import BaseModel

from warren.exceptions import WarrenError


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
class ConsumerHealth:
    """One observation of a consumer manager's transport state.

    Built for two readers: a watchdog deciding whether the process should
    exit, and a readiness endpoint. ``consumer_lost`` is the only condition
    a restart fixes; a blocked or reconnecting connection is left to the
    transport's own recovery.

    :param connected: The transport is up (not closed, not reconnecting).
    :param blocked: Connected, but the broker holds this connection in
        ``Connection.Blocked``; publishes and acks stall until it lifts.
    :param channel_open: The consume channel is initialised and open.
    :param consumer_registered: Our consumer tag is registered on the
        underlying channel; None when that could not be determined within
        the probe timeout (never treated as lost).
    :param detail: Why the state is what it is, for logs and the endpoint.
    """

    connected: bool
    blocked: bool
    channel_open: bool
    consumer_registered: bool | None
    detail: str = ""

    @property
    def ready(self) -> bool:
        return (
            self.connected
            and not self.blocked
            and self.channel_open
            and self.consumer_registered is True
        )

    @property
    def consumer_lost(self) -> bool:
        """Connection alive and unblocked, but no live consumer on it."""
        return (
            self.connected
            and not self.blocked
            and (not self.channel_open or self.consumer_registered is False)
        )

    @property
    def state(
        self,
    ) -> Literal["ready", "reconnecting", "blocked", "consumer_lost", "unknown"]:
        if self.ready:
            return "ready"
        if not self.connected:
            return "reconnecting"
        if self.blocked:
            return "blocked"
        if self.consumer_lost:
            return "consumer_lost"
        return "unknown"


class RetryConfig(BaseModel):
    """Retry policy configuration for the consumer manager.

    Transport-agnostic: provides defaults when the worker's
    ``SoftFailureException`` does not specify values, and caps to enforce
    system-level limits. Used by all pubsub backends (RabbitMQ, Kafka).

    :param initial_delay: Initial delay in seconds before first retry
        when worker does not specify.
    :param max_retries: Max retry attempts when worker does not specify.
    :param backoff_base: Base for exponential backoff. Delay on
        attempt N = initial_delay * backoff_base^(N-1).
    :param jitter: Whether to add random jitter to delays.
    :param max_delay_cap: Maximum delay in seconds (caps backoff).
    :param max_retries_cap: Maximum retries allowed (overrides
        worker request if exceeded).
    :param fallback_requeue_delay: Delay in seconds before
        nack+requeue when no retry publisher is configured.
        Prevents tight retry loops.
    """

    initial_delay: int = 30
    max_retries: int = 5
    backoff_base: float = 2.0
    jitter: bool = True
    max_delay_cap: int = 300
    max_retries_cap: int = 10
    fallback_requeue_delay: float = 2.0


@dataclass(frozen=True)
class Route:
    """
    A transport-agnostic route to a destination, specified by the key.
    Can be extended to include other routing information if necessary.

    :param key: The routing key.
    """

    key: str


class RouteFunc(Protocol):
    async def __call__(self, message: dict) -> list[Route]:
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

    async def __call__(self, message: dict) -> None:
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

    async def health(self, *, probe_timeout: float = 1.0) -> ConsumerHealth:
        """Observe transport state; must not raise for transport reasons.

        :param probe_timeout: Upper bound, in seconds, on any wait the
            observation needs (a blocked connection never becomes ready).
        """
        ...
