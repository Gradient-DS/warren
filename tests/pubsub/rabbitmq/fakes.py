"""Fakes for the RabbitMQ consumer-manager tests.

No broker: the aio-pika objects the manager touches are stood in by minimal
recorders. ``FakeIncomingMessage.closed`` reproduces what a reconnect does
to an in-flight delivery — its ``channel`` property raises
``ChannelInvalidStateError`` and so do ack/nack/reject, which all go through
it (aio_pika/message.py:389-391 in 9.5.8).
"""

import asyncio
import json
from collections.abc import Callable

from aio_pika.exceptions import ChannelInvalidStateError

from warren.pubsub.common import RetryConfig
from warren.pubsub.rabbitmq.aio_pika.consumer import RMQConsumerManager
from warren.pubsub.rabbitmq.config import (
    RMQConsumerConfig,
    RMQConsumerManagerConfig,
    RMQExchangeConfig,
    RMQQueueConfig,
)


BODY = {
    "data_type": "raw_document",
    "job_id": "job-1",
    "data": {"doc_id": "doc-1", "part_idx": 0},
    "origin": {"type": "api", "name": "api-1"},
}


class FakeIncomingMessage:
    """``AbstractIncomingMessage`` stand-in recording the settle it received."""

    def __init__(
        self,
        body: dict | bytes | None = None,
        *,
        redelivered: bool = False,
        routing_key: str = "",
        delivery_tag: int = 1,
        closed: bool = False,
        settle_error: BaseException | None = None,
    ) -> None:
        if body is None:
            body = BODY
        self.body = body if isinstance(body, bytes) else json.dumps(body).encode()
        self.redelivered = redelivered
        self.routing_key = routing_key
        self.delivery_tag = delivery_tag
        self.closed = closed
        self.settle_error = settle_error
        self.acked = False
        self.nacked: bool | None = None  # the requeue flag
        self.rejected: bool | None = None  # the requeue flag

    @property
    def channel(self) -> object:
        if self.closed:
            raise ChannelInvalidStateError
        return object()

    @property
    def settled(self) -> bool:
        return self.acked or self.nacked is not None or self.rejected is not None

    def _check(self) -> None:
        _ = self.channel
        if self.settle_error is not None:
            raise self.settle_error

    async def ack(self) -> None:
        self._check()
        self.acked = True

    async def nack(self, requeue: bool = True) -> None:
        self._check()
        self.nacked = requeue

    async def reject(self, requeue: bool = False) -> None:
        self._check()
        self.rejected = requeue


class FakeWorker:
    """Async MessageConsumerInterface stand-in with scripted behaviour."""

    def __init__(
        self, *, result: dict | None = None, error: Exception | None = None
    ) -> None:
        self.calls: list[dict] = []
        self.result = result
        self.error = error

    @property
    def name(self) -> str:
        return "worker-1"

    @property
    def type(self) -> str:
        return "test_worker"

    async def __call__(self, message: dict) -> dict | None:
        self.calls.append(message)
        if self.error is not None:
            raise self.error
        return self.result


class FakePublisher:
    """PublisherInterface stand-in; ``side_effect`` runs before recording."""

    def __init__(
        self,
        *,
        error: Exception | None = None,
        side_effect: Callable[[], None] | None = None,
    ) -> None:
        self.published: list[dict] = []
        self.error = error
        self.side_effect = side_effect

    async def setup(self) -> None:
        pass

    async def __call__(self, message: dict) -> None:
        if self.side_effect is not None:
            self.side_effect()
        if self.error is not None:
            raise self.error
        self.published.append(message)

    async def teardown(self) -> None:
        pass


def make_manager(
    worker: FakeWorker,
    *,
    data_publisher: FakePublisher | None = None,
    control_publisher: FakePublisher | None = None,
    retry_config: RetryConfig | None = None,
    consumer_config: RMQConsumerConfig | None = None,
    connection_manager: object | None = None,
) -> RMQConsumerManager:
    config = RMQConsumerManagerConfig(
        exchange=RMQExchangeConfig(name="jobs", type="fanout"),
        queue=RMQQueueConfig(name="jobs.test_worker"),
        consumer=consumer_config or RMQConsumerConfig(),
    )
    return RMQConsumerManager(
        config,
        connection_manager or object(),  # type: ignore[arg-type]
        worker,  # type: ignore[arg-type]
        data_publisher=data_publisher,  # type: ignore[arg-type]
        control_publisher=control_publisher,  # type: ignore[arg-type]
        # jitter off for determinism; no real sleeps
        retry_config=retry_config
        or RetryConfig(jitter=False, fallback_requeue_delay=0.0),
    )


def process(manager: RMQConsumerManager, message: FakeIncomingMessage) -> None:
    """Run one delivery through the manager's decision matrix."""
    asyncio.run(manager._process_message(message))  # type: ignore[arg-type]
