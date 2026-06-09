"""
RabbitMQ publisher implementation.
"""

from typing import TYPE_CHECKING, Any

import json

import aio_pika
from basics.logging_utils import summarize_exception_chain

from document_processing.distributed.warren.pubsub.base import BasePublisher
from document_processing.distributed.warren.pubsub.common import (
    PublishFailureException,
    PubSubSetupError,
    Route,
    RouteFunc,
)
from document_processing.distributed.warren.pubsub.rabbitmq.aio_pika.connection import (
    RMQConnectionManager,
)
from document_processing.distributed.warren.pubsub.rabbitmq.aio_pika.topology import (
    declare_exchange,
)
from document_processing.distributed.warren.pubsub.rabbitmq.config import (
    RMQExchangeConfig,
)


if TYPE_CHECKING:
    from aio_pika.abc import AbstractChannel, AbstractExchange


class RMQPublisher(BasePublisher):
    """
    RabbitMQ publisher that owns its own routing decisions.

    Supports three modes:
    - Static exchange (+ optional routing key): publishes directly to the given exchange.
    - Router function: calls route_func(message) per message to resolve destinations dynamically.
    - Neither: raises ValueError at init time.

    Passing both a route_func and an explicit exchange/key is not allowed.
    """

    def __init__(
        self,
        connection_manager: RMQConnectionManager,
        exchange_config: RMQExchangeConfig,
        *,
        route: Route | None = None,
        route_func: RouteFunc | None = None,
        delivery_mode: int = 2,
        content_type: str = "application/json",
        name: str | None = None,
    ) -> None:
        super().__init__(route=route, route_func=route_func, name=name)

        has_static = route is not None
        has_route_func = route_func is not None

        if has_static and has_route_func:
            msg = "Cannot specify both 'route' and 'route_func'. Use one or the other."
            raise ValueError(msg)

        if exchange_config.type == "topic" and not has_static and not has_route_func:
            msg = "Topic exchanges require a route or route_func."
            raise ValueError(msg)

        if exchange_config.type == "fanout" and has_static:
            msg = "Fanout exchanges do not use a routing key."
            raise ValueError(msg)

        self._connection_manager = connection_manager
        self._exchange_config = exchange_config
        self._delivery_mode = delivery_mode
        self._content_type = content_type

        # Instantiate channel after setup is called.
        self._channel: AbstractChannel | None = None
        self._exchange: AbstractExchange | None = None

    async def setup(self) -> None:
        """Open a channel and declare the publisher's exchange.

        On partial failure the caller is responsible for cleanup via
        ``teardown()`` (idempotent, best-effort).

        :raises PubSubSetupError: if the channel or exchange cannot be
            set up.
        """
        try:
            self._channel = await self._connection_manager.create_channel()
            self._exchange = await declare_exchange(
                self._channel, self._exchange_config
            )
        except Exception as e:
            msg = (
                f"{self}: Publisher setup failed for exchange "
                f"'{self._exchange_config.name}'"
            )
            raise PubSubSetupError(msg) from e

    async def teardown(self) -> None:
        if self._channel is not None and not self._channel.is_closed:
            try:
                await self._channel.close()
            except Exception as e:
                self._log.warning(
                    f"Error closing publisher channel: {summarize_exception_chain(e)}"
                )

    async def __call__(self, message: dict[str, Any]) -> None:
        if self._channel is None:
            msg = "Must call setup() before publishing."
            raise RuntimeError(msg)

        body = json.dumps(message).encode()
        amqp_message = aio_pika.Message(
            body=body,
            delivery_mode=aio_pika.DeliveryMode(self._delivery_mode),
            content_type=self._content_type,
        )

        # If a route function is provided, use it to resolve the routes. Otherwise, use the routing key (which can be None).
        routes = (
            await self._route_func(message)
            if self._route_func is not None
            else [self._route]
        )

        for route in routes:
            await self._publish_to(
                amqp_message,
                routing_key=route.key if route else None,
            )

    async def _publish_to(
        self,
        amqp_message: aio_pika.Message,
        *,
        routing_key: str | None = None,
    ) -> None:
        """Publish to a specific exchange with a routing key. If no routing key is provided (fan-out), RMQ requires passing an empty string (which is ignored in fanout exchanges)."""
        if self._exchange is None:
            msg = "Must call setup() before publishing."
            raise RuntimeError(msg)

        try:
            if routing_key is None:
                await self._exchange.publish(amqp_message, routing_key="")
            else:
                await self._exchange.publish(amqp_message, routing_key=routing_key)
        except Exception as e:
            msg = (
                f"Failed to publish to exchange '{self._exchange}' "
                f"with routing key '{routing_key}': {e}"
            )
            raise PublishFailureException(msg) from e
