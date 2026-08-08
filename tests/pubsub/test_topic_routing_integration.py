"""
Integration test: a topic exchange routes by binding key.

Requires a running RabbitMQ on localhost:5672 (e.g. the docker container from
the README). Skips cleanly when no broker is reachable, so it is safe in CI
without a broker.
"""

import asyncio
import json
import uuid

import aio_pika
import pytest

from warren.pubsub.rabbitmq.aio_pika.connection import RMQConnectionManager
from warren.pubsub.rabbitmq.aio_pika.topology import declare_exchange, declare_queue
from warren.pubsub.rabbitmq.config import (
    RMQConnectionConfig,
    RMQExchangeConfig,
    RMQQueueConfig,
)


async def _broker_reachable() -> bool:
    mgr = RMQConnectionManager(RMQConnectionConfig())
    try:
        await mgr.setup()
        await mgr.teardown()
    except Exception:  # noqa: BLE001 - any failure means "no broker", skip the test
        return False
    return True


async def _run_topic_routing() -> tuple[str | None, str | None]:
    """Bind two queues to a topic exchange with distinct keys, publish to one,
    and return (message on queue 'a', message on queue 'b')."""
    suffix = uuid.uuid4().hex[:8]
    exchange_name = f"test-topic-{suffix}"
    mgr = RMQConnectionManager(RMQConnectionConfig())
    await mgr.setup()
    try:
        channel = await mgr.create_channel()
        exchange = await declare_exchange(
            channel, RMQExchangeConfig(name=exchange_name, type="topic", durable=False)
        )
        # Exclusive queues are connection-scoped and auto-removed on close
        # (RabbitMQ 4 blocks transient non-exclusive queues by default).
        queue_a = await declare_queue(
            channel,
            exchange,
            RMQQueueConfig(
                name=f"q-a-{suffix}",
                durable=False,
                exclusive=True,
                routing_key="alpha",
            ),
            exchange_type="topic",
        )
        queue_b = await declare_queue(
            channel,
            exchange,
            RMQQueueConfig(
                name=f"q-b-{suffix}",
                durable=False,
                exclusive=True,
                routing_key="beta",
            ),
            exchange_type="topic",
        )

        await exchange.publish(
            aio_pika.Message(body=json.dumps({"hello": "alpha"}).encode()),
            routing_key="alpha",
        )
        # Give the broker a moment to route.
        await asyncio.sleep(0.2)

        msg_a = await queue_a.get(no_ack=True, fail=False)
        msg_b = await queue_b.get(no_ack=True, fail=False)
        await exchange.delete()
        return (
            msg_a.body.decode() if msg_a else None,
            msg_b.body.decode() if msg_b else None,
        )
    finally:
        await mgr.teardown()


def test_topic_exchange_routes_only_matching_key():
    if not asyncio.run(_broker_reachable()):
        pytest.skip("No RabbitMQ broker reachable on localhost:5672")

    body_a, body_b = asyncio.run(_run_topic_routing())
    # matching key delivered, non-matching key NOT delivered
    assert body_a is not None
    assert "alpha" in body_a
    assert body_b is None
