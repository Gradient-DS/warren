import asyncio

import pytest
from pydantic import ValidationError

from tests.pubsub.memory.test_consumer_manager import _eventually, _manager
from tests.pubsub.rabbitmq.fakes import (
    FakeConnectionManager,
    FakeIncomingMessage,
    FakePublisher,
    FakeWorker,
    make_manager,
)
from tests.pubsub.rabbitmq.test_topology import _FakeChannel, _FakeExchange
from warren.pubsub.kafka.config import KafkaConsumerConfig
from warren.pubsub.memory.config import MemoryConsumerConfig
from warren.pubsub.rabbitmq.config import RMQConsumerConfig


class GatedWorker(FakeWorker):
    def __init__(self, concurrency: int) -> None:
        super().__init__()
        self.concurrency = concurrency
        self.at_capacity = asyncio.Event()
        self.release = asyncio.Event()
        self.running = 0
        self.peak = 0

    async def __call__(self, message: dict) -> dict:
        self.calls.append(message)
        self.running += 1
        self.peak = max(self.peak, self.running)
        if self.running == self.concurrency:
            self.at_capacity.set()
        try:
            await self.release.wait()
            return message
        finally:
            self.running -= 1


@pytest.mark.parametrize("backend", ["rabbitmq", "memory"])
@pytest.mark.parametrize("concurrency", [None, 1, 3])
def test_handlers_are_bounded_and_counted(
    backend: str, concurrency: int | None
) -> None:
    async def run() -> None:
        limit = concurrency or (4 if backend == "rabbitmq" else 1)
        worker, publisher = GatedWorker(limit), FakePublisher()
        if backend == "rabbitmq":
            manager = make_manager(
                worker,
                consumer_config=RMQConsumerConfig(
                    concurrency=concurrency, prefetch_count=4
                ),
                data_publisher=publisher,
                connection_manager=FakeConnectionManager(None),
            )
            for i in range(6):
                await manager._on_message(FakeIncomingMessage({"item": i}))
        else:
            manager = await _manager(
                worker, concurrency=concurrency, data_publisher=publisher
            )
            for _ in range(6):
                manager._connection_manager.broker.publish(manager._exchange, "", b"{}")
            await manager.start_consuming()

        try:
            async with asyncio.timeout(1):
                await worker.at_capacity.wait()
            await asyncio.sleep(0)
            assert len(worker.calls) == limit
            assert (await manager.health()).in_flight_handlers == limit
        finally:
            worker.release.set()
        await _eventually(lambda: len(publisher.published) == 6)
        await manager.stop_consuming()
        assert worker.peak == limit
        assert (await manager.health()).in_flight_handlers == 0

    asyncio.run(run())


def test_memory_shutdown_drains_all_handlers() -> None:
    async def run() -> None:
        worker, publisher = GatedWorker(3), FakePublisher()
        manager = await _manager(worker, concurrency=3, data_publisher=publisher)
        for _ in range(3):
            manager._connection_manager.broker.publish(manager._exchange, "", b"{}")
        await manager.start_consuming()
        async with asyncio.timeout(1):
            await worker.at_capacity.wait()
        stopping = asyncio.create_task(manager.stop_consuming())
        try:
            await asyncio.sleep(0)
            assert not stopping.done()
            assert (await manager.health()).in_flight_handlers == 3
        finally:
            worker.release.set()
            await stopping
        assert len(publisher.published) == 3
        assert not manager._in_flight_tasks

    asyncio.run(run())


@pytest.mark.parametrize(
    ("prefetch", "concurrency", "expected"),
    [(1, 3, 3), (8, 2, 8), (4, None, 4), (0, None, 0), (0, 2, 2)],
)
def test_rabbitmq_prefetch_is_at_least_concurrency(
    prefetch: int, concurrency: int | None, expected: int
) -> None:
    class Channel(_FakeChannel):
        async def set_qos(self, *, prefetch_count: int) -> None:
            self.prefetch_count = prefetch_count

        async def declare_exchange(
            self, name: str, exchange_type: str, *, durable: bool
        ) -> _FakeExchange:
            return _FakeExchange()

    class Connection:
        async def create_channel(self) -> Channel:
            return channel

    channel = Channel()
    manager = make_manager(
        FakeWorker(),
        consumer_config=RMQConsumerConfig(
            concurrency=concurrency, prefetch_count=prefetch
        ),
        connection_manager=Connection(),
    )
    asyncio.run(manager.setup())
    assert channel.prefetch_count == expected


@pytest.mark.parametrize(
    "config_type", [RMQConsumerConfig, KafkaConsumerConfig, MemoryConsumerConfig]
)
def test_concurrency_defaults_to_unset_and_rejects_zero(config_type: type) -> None:
    assert config_type().concurrency is None
    assert config_type(concurrency=None).concurrency is None
    assert config_type(concurrency=1).concurrency == 1
    with pytest.raises(ValidationError, match="greater than or equal to 1"):
        config_type(concurrency=0)


def test_kafka_rejects_parallel_handlers() -> None:
    with pytest.raises(ValidationError, match="preserve offset commit ordering"):
        KafkaConsumerConfig(concurrency=2)


def test_memory_health_detects_one_lost_consume_loop() -> None:
    async def run() -> None:
        manager = await _manager(FakeWorker(), concurrency=2)
        await manager.start_consuming()
        task = next(iter(manager._consume_tasks))
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        assert (await manager.health()).consumer_lost
        await manager.stop_consuming()

    asyncio.run(run())


def test_rabbitmq_unset_concurrency_with_unlimited_prefetch() -> None:
    async def run() -> None:
        worker = GatedWorker(6)
        manager = make_manager(
            worker,
            consumer_config=RMQConsumerConfig(prefetch_count=0),
            connection_manager=FakeConnectionManager(None),
        )
        for i in range(6):
            await manager._on_message(FakeIncomingMessage({"item": i}))
        try:
            async with asyncio.timeout(1):
                await worker.at_capacity.wait()
            assert (await manager.health()).in_flight_handlers == 6
        finally:
            worker.release.set()
            await manager.stop_consuming()
        assert worker.peak == 6

    asyncio.run(run())
