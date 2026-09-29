import asyncio

import pytest

from tests.pubsub.rabbitmq.fakes import FakeWorker
from tests.pubsub.rabbitmq.test_topology import _FakeChannel, _FakeExchange
from warren.exceptions import WarrenError
from warren.pubsub.memory.connection import MemoryConnectionManager
from warren.pubsub.rabbitmq.aio_pika.topology import declare_queue
from warren.pubsub.rabbitmq.config import RMQExchangeConfig
from warren.runtime.backends import create_consumer_manager
from warren.runtime.config import RuntimeConfig


@pytest.mark.parametrize("exchange_type", ["topic", "direct", "fanout"])
def test_runtime_bindings_match_rabbitmq_topology(exchange_type: str) -> None:
    async def run() -> None:
        exchange = RMQExchangeConfig(name="jobs", type=exchange_type)
        keys = ("input.*", "input.#") if exchange_type == "topic" else ("a", "b")
        kwargs = {
            "exchange": exchange,
            "worker_type": "worker",
            "consumer": FakeWorker(),
            "binding_keys": keys,
        }
        rabbit = create_consumer_manager(RuntimeConfig(), object(), **kwargs)
        queue = await declare_queue(
            _FakeChannel(),
            _FakeExchange(),
            rabbit._config.queue,
            exchange_type=exchange_type,
        )
        connection = MemoryConnectionManager()
        await connection.setup()
        memory = create_consumer_manager(
            RuntimeConfig(backend="memory"), connection, **kwargs
        )
        await memory.setup()
        actual_keys = tuple(key for _, key in queue.bound)
        assert actual_keys == (("",) if exchange_type == "fanout" else keys)
        published = ["input.one", "input.two.more", "other"]
        if exchange_type != "topic":
            published = ["a", "b", "other"]
        counts = [connection.broker.publish(exchange, key, b"{}") for key in published]
        assert counts == ([1, 1, 1] if exchange_type == "fanout" else [1, 1, 0])
        assert memory._queue.qsize() == sum(counts)

    asyncio.run(run())


@pytest.mark.parametrize("exchange_type", ["topic", "direct"])
def test_kafka_still_rejects_routed_exchanges(exchange_type: str) -> None:
    with pytest.raises(WarrenError, match="fanout pipelines only"):
        create_consumer_manager(
            RuntimeConfig(backend="kafka"),
            object(),
            exchange=RMQExchangeConfig(name="jobs", type=exchange_type),
            worker_type="worker",
            consumer=FakeWorker(),
            binding_keys=("a", "b"),
        )


def test_kafka_ignores_keys_on_fanout(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level("DEBUG"):
        manager = create_consumer_manager(
            RuntimeConfig(backend="kafka"),
            object(),
            exchange=RMQExchangeConfig(name="jobs", type="fanout"),
            worker_type="worker",
            consumer=FakeWorker(),
            binding_keys=("a", "b"),
        )
    assert manager._config.consumer.group_id == "jobs.worker"
    assert "Ignoring binding keys" in caplog.text
