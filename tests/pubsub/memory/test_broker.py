"""Unit tests for ``MemoryBroker``: exchange -> binding -> queue routing."""

import pytest

from warren.pubsub.memory.broker import Delivery, MemoryBroker, topic_matches
from warren.pubsub.rabbitmq.config import RMQExchangeConfig


FANOUT = RMQExchangeConfig(name="jobs", type="fanout")
DIRECT = RMQExchangeConfig(name="jobs", type="direct")
TOPIC = RMQExchangeConfig(name="jobs", type="topic")


def test_fanout_copies_to_every_bound_queue() -> None:
    broker = MemoryBroker()
    a = broker.bind(FANOUT, "jobs.a")
    b = broker.bind(FANOUT, "jobs.b")

    delivered = broker.publish(FANOUT, "ignored", b"x")

    assert delivered == 2
    assert a.get_nowait() == Delivery(body=b"x", routing_key="ignored")
    assert b.get_nowait() == Delivery(body=b"x", routing_key="ignored")


def test_direct_routes_on_exact_key() -> None:
    broker = MemoryBroker()
    a = broker.bind(DIRECT, "jobs.a", "alpha")
    b = broker.bind(DIRECT, "jobs.b", "beta")

    assert broker.publish(DIRECT, "alpha", b"x") == 1
    assert a.qsize() == 1
    assert b.qsize() == 0


def test_topic_routes_on_pattern() -> None:
    broker = MemoryBroker()
    a = broker.bind(TOPIC, "jobs.a", "doc.*.parsed")
    b = broker.bind(TOPIC, "jobs.b", "doc.#")

    assert broker.publish(TOPIC, "doc.pdf.parsed", b"x") == 2
    assert broker.publish(TOPIC, "doc.pdf.chunked", b"y") == 1
    assert a.qsize() == 1
    assert b.qsize() == 2


def test_publish_without_matching_binding_is_dropped() -> None:
    broker = MemoryBroker()
    broker.bind(DIRECT, "jobs.a", "alpha")

    assert broker.publish(DIRECT, "nobody", b"x") == 0
    assert broker.publish(RMQExchangeConfig(name="other", type="fanout"), "", b"x") == 0


def test_same_queue_name_is_one_queue_for_competing_consumers() -> None:
    broker = MemoryBroker()
    first = broker.bind(FANOUT, "jobs.a")
    second = broker.bind(FANOUT, "jobs.a")

    assert first is second
    assert broker.publish(FANOUT, "", b"x") == 1
    assert first.qsize() == 1


@pytest.mark.parametrize(
    ("pattern", "key", "expected"),
    [
        ("a.b", "a.b", True),
        ("a.b", "a.c", False),
        ("a.*", "a.b", True),
        ("a.*", "a", False),
        ("a.*", "a.b.c", False),
        ("a.#", "a", True),
        ("a.#", "a.b.c", True),
        ("#", "", True),
        ("#", "a.b", True),
        ("#.z", "a.b.z", True),
        ("*.b.#", "a.b", True),
        ("*.b.#", "b", False),
    ],
)
def test_topic_matches(pattern: str, key: str, expected: bool) -> None:
    assert topic_matches(pattern, key) is expected
