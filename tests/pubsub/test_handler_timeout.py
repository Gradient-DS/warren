import asyncio
import json
import threading
from collections.abc import Awaitable, Callable

import pytest

from tests.pubsub.kafka.test_consumer_manager import _FakeConnectionManager, _msg
from tests.pubsub.memory.test_consumer_manager import _eventually
from tests.pubsub.rabbitmq.fakes import (
    FakeConnectionManager,
    FakeIncomingMessage,
    FakePublisher,
    FakeWorker,
    make_manager,
)
from warren.pubsub.base import ConsumerManagerBase
from warren.pubsub.kafka.aiokafka.consumer import KafkaConsumerManager
from warren.pubsub.kafka.config import (
    KafkaConsumerConfig,
    KafkaConsumerManagerConfig,
    KafkaTopicConfig,
)
from warren.pubsub.memory.broker import Delivery
from warren.pubsub.memory.connection import MemoryConnectionManager
from warren.pubsub.memory.consumer import MemoryConsumerManager
from warren.pubsub.rabbitmq.config import RMQConsumerConfig, RMQExchangeConfig
from warren.workers.health import HealthServer


class WaitingWorker(FakeWorker):
    def __init__(self, *, ignore_cancellation: bool = False) -> None:
        super().__init__()
        self.started = asyncio.Event()
        self.cancelled = asyncio.Event()
        self.release = asyncio.Event()
        self.ignore_cancellation = ignore_cancellation

    async def __call__(self, message: dict) -> dict:
        self.started.set()
        try:
            await self.release.wait()
        except asyncio.CancelledError:
            self.cancelled.set()
            if not self.ignore_cancellation:
                raise
            await self.release.wait()
        return message


async def make_consumer(
    backend: str,
    worker: FakeWorker,
    publisher: FakePublisher,
    seconds: float | None,
) -> tuple[ConsumerManagerBase, Callable[[dict], Awaitable[None]]]:
    if backend == "rabbitmq":
        manager = make_manager(
            worker,
            consumer_config=RMQConsumerConfig(handler_timeout_seconds=seconds),
            control_publisher=publisher,
            data_publisher=publisher,
            connection_manager=FakeConnectionManager(None),
        )

        async def process(body: dict) -> None:
            await manager._process_message(FakeIncomingMessage(body))

    elif backend == "kafka":
        manager = KafkaConsumerManager(
            KafkaConsumerManagerConfig(
                topic=KafkaTopicConfig(name="jobs"),
                consumer=KafkaConsumerConfig(handler_timeout_seconds=seconds),
            ),
            _FakeConnectionManager(),
            worker,
            control_publisher=publisher,
            data_publisher=publisher,
        )
        await manager.setup()

        async def process(body: dict) -> None:
            await manager._process_message(_msg(body))

    else:
        connection = MemoryConnectionManager()
        await connection.setup()
        manager = MemoryConsumerManager(
            connection,
            worker,
            exchange=RMQExchangeConfig(name="jobs", type="fanout"),
            queue_name="jobs.worker",
            handler_timeout_seconds=seconds,
            control_publisher=publisher,
            data_publisher=publisher,
        )
        await manager.setup()

        async def process(body: dict) -> None:
            await manager._process_message(Delivery(json.dumps(body).encode(), ""))

    return manager, process


@pytest.mark.parametrize("backend", ["rabbitmq", "kafka", "memory"])
@pytest.mark.parametrize("retry_count", [0, 100])
def test_timeout_uses_retry_policy(backend: str, retry_count: int) -> None:
    async def run() -> None:
        publisher = FakePublisher()
        manager, process = await make_consumer(
            backend, WaitingWorker(), publisher, 0.001
        )
        await process({"data_type": "input", "retry": {"count": retry_count}})
        envelope = publisher.published[0]
        if retry_count == 0:
            assert envelope["data_type"] == "soft-failure"
            assert envelope["data"]["retry"]["count"] == 1
            assert (
                envelope["data"]["retry"]["reason"] == "handler timed out after 0.001s"
            )
        else:
            assert envelope["data_type"] == "hard-failure"
        assert (await manager.health()).in_flight_handlers == 0

    asyncio.run(run())


@pytest.mark.parametrize("backend", ["rabbitmq", "kafka", "memory"])
def test_no_timeout_waits_for_handler(backend: str) -> None:
    async def run() -> None:
        worker, publisher = WaitingWorker(), FakePublisher()
        manager, process = await make_consumer(backend, worker, publisher, None)
        task = asyncio.create_task(process({"data_type": "input"}))
        await worker.started.wait()
        assert not task.done()
        assert (await manager.health()).in_flight_handlers == 1
        worker.release.set()
        await task
        assert publisher.published == [{"data_type": "input"}]
        assert (await manager.health()).in_flight_handlers == 0

    asyncio.run(run())


@pytest.mark.parametrize("backend", ["rabbitmq", "kafka", "memory"])
def test_live_fails_for_handler_ignoring_cancellation(backend: str) -> None:
    async def run() -> None:
        worker = WaitingWorker(ignore_cancellation=True)
        manager, process = await make_consumer(backend, worker, FakePublisher(), 0.001)
        task = asyncio.create_task(process({}))
        try:
            await worker.cancelled.wait()
            await asyncio.sleep(0.003)
            health = await manager.health()
            server = HealthServer(lambda: health, host="127.0.0.1", port=0)
            assert server._respond("/live")[0] == "503 Service Unavailable"
            assert health.in_flight_handlers == 1
        finally:
            worker.release.set()
            await task
        health = await manager.health()
        assert server._respond("/live")[0] == "200 OK"

    asyncio.run(run())


def test_sync_handler_stays_in_health_until_thread_finishes() -> None:
    release = threading.Event()

    class SyncWorker(FakeWorker):
        def __call__(self, message: dict) -> dict:
            release.wait(2)
            return message

    async def run() -> None:
        manager, process = await make_consumer(
            "memory", SyncWorker(), FakePublisher(), 0.001
        )
        try:
            await process({})
            await asyncio.sleep(0.003)
            assert not (await manager.health()).live
        finally:
            release.set()
        await _eventually(lambda: not manager._handler_started_at)
        assert (await manager.health()).live

    asyncio.run(run())


def test_handler_timeout_error_is_not_a_deadline_expiry() -> None:
    async def run() -> None:
        publisher = FakePublisher()
        _, process = await make_consumer(
            "memory", FakeWorker(error=TimeoutError("upstream")), publisher, 1
        )
        await process({})
        assert publisher.published[0]["data_type"] == "hard-failure"

    asyncio.run(run())
