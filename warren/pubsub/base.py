from abc import ABCMeta, abstractmethod
from typing import Optional, List, Dict

from basics.base import Base

from document_processing.distributed.warren.common import MessageConsumerInterface
from document_processing.distributed.warren.pubsub.common import (
    ConsumerManagerInterface,
    PublisherInterface,
    Route,
    RouteFunc,
)


class BasePublisher(Base, PublisherInterface, metaclass=ABCMeta):
    """Base class for publishers. Each publisher owns its own routing logic."""

    def __init__(
        self,
        route: Optional[Route] = None,
        route_func: Optional[RouteFunc] = None,
        *,
        name: Optional[str] = None,
    ) -> None:
        classname = type(self).__name__
        logger_name = f"[{classname}] {name}" if name else None
        super().__init__(pybase_logger_name=logger_name)

        has_static = route is not None
        has_route_func = route_func is not None

        if has_static and has_route_func:
            raise ValueError(
                "Cannot specify both 'route' and 'route_func'. Use one or the other."
            )

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
    async def __call__(self, message: Dict) -> None:
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
        publishers: Optional[List[PublisherInterface]] = None,
    ) -> None:
        classname = type(self).__name__
        logger_name = f"[{classname}] {consumer.name}" if consumer.name else None
        super().__init__(pybase_logger_name=logger_name)

        self._consumer = consumer
        self._publishers: List[PublisherInterface] = publishers or []

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
