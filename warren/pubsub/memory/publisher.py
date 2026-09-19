"""Publisher for the in-process backend."""

import json

from warren.pubsub.base import BasePublisher
from warren.pubsub.common import PublishFailureException, Route, RouteFunc
from warren.pubsub.memory.connection import MemoryConnectionManager
from warren.pubsub.rabbitmq.config import RMQExchangeConfig


class MemoryPublisher(BasePublisher):
    """Publishes JSON-encoded messages onto a ``MemoryBroker`` exchange.

    Route resolution matches ``RMQPublisher``: ``route_func`` when set,
    otherwise the static ``route``, otherwise the empty key.

    Messages are encoded to JSON even though nothing leaves the process.
    That keeps two properties of the real backends: a payload that cannot
    be serialised fails at publish time, and every consumer decodes its own
    copy, which matters because consumer managers mutate the body when they
    stamp retry state.
    """

    def __init__(
        self,
        connection_manager: MemoryConnectionManager,
        exchange: RMQExchangeConfig,
        *,
        route: Route | None = None,
        route_func: RouteFunc | None = None,
        name: str | None = None,
    ) -> None:
        super().__init__(route, route_func, name=name)
        self._connection_manager = connection_manager
        self._exchange = exchange

    async def setup(self) -> None:
        """Nothing to open; the broker is owned by the connection manager."""

    async def teardown(self) -> None:
        """Nothing to close."""

    async def __call__(self, message: dict) -> None:
        try:
            body = json.dumps(message).encode()
        except (TypeError, ValueError) as e:
            msg = (
                f"Message for exchange '{self._exchange.name}' is not JSON-serialisable"
            )
            raise PublishFailureException(msg) from e

        routes = (
            await self._route_func(message)
            if self._route_func is not None
            else [self._route]
        )
        for route in routes:
            self._connection_manager.broker.publish(
                self._exchange,
                route.key if route else "",
                body,
            )
