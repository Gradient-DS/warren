import asyncio
import inspect
import time
from abc import ABCMeta, abstractmethod

from basics.base import Base

from warren.common import MessageConsumerInterface, SoftFailureException
from warren.pubsub.common import (
    ConsumerHealth,
    ConsumerManagerInterface,
    PublisherInterface,
    Route,
    RouteFunc,
)


class BasePublisher(Base, PublisherInterface, metaclass=ABCMeta):
    """Base class for publishers. Each publisher owns its own routing logic."""

    def __init__(
        self,
        route: Route | None = None,
        route_func: RouteFunc | None = None,
        *,
        name: str | None = None,
    ) -> None:
        classname = type(self).__name__
        logger_name = f"[{classname}] {name}" if name else None
        super().__init__(pybase_logger_name=logger_name)

        has_static = route is not None
        has_route_func = route_func is not None

        if has_static and has_route_func:
            msg = "Cannot specify both 'route' and 'route_func'. Use one or the other."
            raise ValueError(msg)

        self._route = route
        self._route_func = route_func

    @abstractmethod
    async def setup(self) -> None:
        """Set up the publisher (channels, connections, etc.)."""
        ...

    @abstractmethod
    async def teardown(self) -> None:
        """Tear down the publisher."""
        ...

    @abstractmethod
    async def __call__(self, message: dict) -> None:
        """
        Publish a message. Routing is determined internally by the publisher.

        :param message: Message body as a dictionary.
        """
        ...


class ConsumerManagerBase(Base, ConsumerManagerInterface, metaclass=ABCMeta):
    def __init__(
        self,
        consumer: MessageConsumerInterface,
        *,
        publishers: list[PublisherInterface] | None = None,
        handler_timeout_seconds: float | None = None,
    ) -> None:
        classname = type(self).__name__
        logger_name = f"[{classname}] {consumer.name}" if consumer.name else None
        super().__init__(pybase_logger_name=logger_name)

        self._handler_timeout_seconds = handler_timeout_seconds
        self._handler_started_at: dict[object, float] = {}
        self._consumer = consumer
        self._publishers: list[PublisherInterface] = publishers or []

    async def _call_handler(self, body: dict) -> dict | None:
        token = object()
        self._handler_started_at[token] = time.monotonic()
        deadline = asyncio.timeout(self._handler_timeout_seconds)
        executor_running = False
        try:
            async with deadline:
                is_async = inspect.iscoroutinefunction(
                    self._consumer
                ) or inspect.iscoroutinefunction(
                    getattr(self._consumer, "__call__", None)
                )
                if is_async:
                    return await self._consumer(body)
                future = asyncio.get_running_loop().run_in_executor(
                    None, self._consumer, body
                )
                # A timeout cannot stop the executor thread; track it until it exits.
                executor_running = True
                future.add_done_callback(
                    lambda _: self._handler_started_at.pop(token, None)
                )
                return await asyncio.shield(future)
        except TimeoutError as e:
            if not deadline.expired():
                raise
            reason = f"handler timed out after {self._handler_timeout_seconds:g}s"
            raise SoftFailureException(reason) from e
        finally:
            if not executor_running:
                self._handler_started_at.pop(token, None)

    @abstractmethod
    async def setup(self) -> None:
        """Set up consumption for the given consumer and topic."""
        ...

    @abstractmethod
    async def start_consuming(self) -> None:
        """Allow the consumer to start consuming."""
        ...

    @abstractmethod
    async def stop_consuming(self) -> None:
        """Stop the consumption of messages by consumer."""
        ...

    @abstractmethod
    async def health(self, *, probe_timeout: float = 1.0) -> ConsumerHealth:
        """Observe transport state; must not raise for transport reasons.

        :param probe_timeout: Upper bound, in seconds, on any wait the
            observation needs (a blocked connection never becomes ready).
        """
        ...
