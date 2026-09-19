"""Unit tests for ``MemoryPublisher`` and ``MemoryConnectionManager``."""

import asyncio
import json

import pytest

from warren.pubsub.common import PublishFailureException, Route
from warren.pubsub.memory.connection import MemoryConnectionManager
from warren.pubsub.memory.publisher import MemoryPublisher
from warren.pubsub.rabbitmq.config import RMQExchangeConfig


DIRECT = RMQExchangeConfig(name="jobs", type="direct")
FANOUT = RMQExchangeConfig(name="jobs", type="fanout")


def test_broker_requires_setup() -> None:
    with pytest.raises(RuntimeError):
        _ = MemoryConnectionManager().broker


def test_publishes_json_with_static_route() -> None:
    async def scenario() -> tuple[bytes, str]:
        conn = MemoryConnectionManager()
        await conn.setup()
        queue = conn.broker.bind(DIRECT, "jobs.a", "alpha")
        publisher = MemoryPublisher(conn, DIRECT, route=Route("alpha"))
        await publisher.setup()
        await publisher({"data_type": "x"})
        delivery = queue.get_nowait()
        return delivery.body, delivery.routing_key

    body, key = asyncio.run(scenario())
    assert json.loads(body) == {"data_type": "x"}
    assert key == "alpha"


def test_route_func_fans_a_message_out_to_several_keys() -> None:
    async def route_func(message: dict) -> list[Route]:
        return [Route("alpha"), Route("beta")]

    async def scenario() -> tuple[int, int]:
        conn = MemoryConnectionManager()
        await conn.setup()
        a = conn.broker.bind(DIRECT, "jobs.a", "alpha")
        b = conn.broker.bind(DIRECT, "jobs.b", "beta")
        publisher = MemoryPublisher(conn, DIRECT, route_func=route_func)
        await publisher({"data_type": "x"})
        return a.qsize(), b.qsize()

    assert asyncio.run(scenario()) == (1, 1)


def test_no_route_publishes_with_empty_key() -> None:
    async def scenario() -> str:
        conn = MemoryConnectionManager()
        await conn.setup()
        queue = conn.broker.bind(FANOUT, "jobs.a")
        await MemoryPublisher(conn, FANOUT)({"data_type": "x"})
        return queue.get_nowait().routing_key

    assert asyncio.run(scenario()) == ""


def test_unserialisable_message_is_a_publish_failure() -> None:
    async def scenario() -> None:
        conn = MemoryConnectionManager()
        await conn.setup()
        await MemoryPublisher(conn, FANOUT)({"payload": object()})

    with pytest.raises(PublishFailureException):
        asyncio.run(scenario())


def test_teardown_drops_the_broker() -> None:
    async def scenario() -> None:
        conn = MemoryConnectionManager()
        await conn.setup()
        await conn.teardown()
        _ = conn.broker

    with pytest.raises(RuntimeError):
        asyncio.run(scenario())
