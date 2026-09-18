"""Pins that ``declare_queue`` forwards queue arguments verbatim."""

import asyncio

from warren.pubsub.rabbitmq.aio_pika.topology import declare_queue
from warren.pubsub.rabbitmq.config import RMQQueueConfig


class _FakeQueue:
    def __init__(self) -> None:
        self.bound: list[tuple] = []

    async def bind(self, exchange: str, routing_key: str = "") -> None:
        self.bound.append((exchange, routing_key))


class _FakeChannel:
    def __init__(self) -> None:
        self.declared: dict | None = None

    async def declare_queue(self, name: str, **kwargs) -> _FakeQueue:
        self.declared = {"name": name, **kwargs}
        return _FakeQueue()


class _FakeExchange:
    name = "jobs"


def test_declare_queue_forwards_arguments() -> None:
    channel = _FakeChannel()
    config = RMQQueueConfig(name="jobs.parser", arguments={"x-queue-type": "quorum"})

    asyncio.run(declare_queue(channel, _FakeExchange(), config, exchange_type="fanout"))

    assert channel.declared["arguments"] == {"x-queue-type": "quorum"}


def test_declare_queue_default_has_no_arguments() -> None:
    channel = _FakeChannel()

    asyncio.run(
        declare_queue(
            channel, _FakeExchange(), RMQQueueConfig(name="q"), exchange_type="fanout"
        )
    )

    assert channel.declared["arguments"] is None
