"""Pins that ``declare_queue`` forwards queue arguments verbatim."""

import asyncio

from warren.pubsub.rabbitmq.aio_pika.topology import declare_queue
from warren.pubsub.rabbitmq.config import RMQExchangeConfig, RMQQueueConfig
from warren.runtime import backends
from warren.runtime.config import RuntimeConfig

from .fakes import FakeWorker


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


def test_runtime_priority_argument_reaches_queue_declaration() -> None:
    config = RuntimeConfig.model_validate(
        {"rabbitmq": {"consumer": {"queue_arguments": {"x-max-priority": 2}}}}
    )
    manager = backends.create_consumer_manager(
        config,
        object(),
        exchange=RMQExchangeConfig(name="jobs", type="topic"),
        worker_type="parser",
        consumer=FakeWorker(),
        binding_key="interactive.raw_document",
    )
    channel = _FakeChannel()
    asyncio.run(
        declare_queue(
            channel, _FakeExchange(), manager._config.queue, exchange_type="topic"
        )
    )
    assert channel.declared["arguments"] == {"x-max-priority": 2}


def test_declare_queue_default_has_no_arguments() -> None:
    channel = _FakeChannel()

    asyncio.run(
        declare_queue(
            channel, _FakeExchange(), RMQQueueConfig(name="q"), exchange_type="fanout"
        )
    )

    assert channel.declared["arguments"] is None


def test_declare_queue_binds_each_key() -> None:
    queue = asyncio.run(
        declare_queue(
            _FakeChannel(),
            _FakeExchange(),
            RMQQueueConfig(name="q", binding_keys=("input.*", "retry.#")),
            exchange_type="topic",
        )
    )
    assert queue.bound == [("jobs", "input.*"), ("jobs", "retry.#")]


def test_declare_queue_keeps_legacy_routing_key() -> None:
    queue = asyncio.run(
        declare_queue(
            _FakeChannel(),
            _FakeExchange(),
            RMQQueueConfig(name="q", routing_key="input"),
            exchange_type="direct",
        )
    )
    assert queue.bound == [("jobs", "input")]


def test_fanout_ignores_binding_keys(caplog) -> None:
    with caplog.at_level("DEBUG"):
        queue = asyncio.run(
            declare_queue(
                _FakeChannel(),
                _FakeExchange(),
                RMQQueueConfig(name="q", binding_keys=("a", "b")),
                exchange_type="fanout",
            )
        )
    assert queue.bound == [("jobs", "")]
    assert "Ignoring binding keys" in caplog.text
