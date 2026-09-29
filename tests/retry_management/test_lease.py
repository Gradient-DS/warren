import asyncio
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError
from pymongo.errors import ConnectionFailure, DuplicateKeyError
from pymongo.results import UpdateResult
from pymongo.write_concern import WriteConcern

from tests.pubsub.rabbitmq.fakes import FakePublisher
from warren.common import MessageConsumerInterface
from warren.pubsub.memory.connection import MemoryConnectionManager
from warren.pubsub.memory.consumer import MemoryConsumerManager
from warren.pubsub.rabbitmq.config import RMQExchangeConfig
from warren.retry_management.lease import MongoRetryLease
from warren.retry_management.retry_worker import RetryWorker
from warren.retry_management.retry_worker_runner import RetryWorkerRunner
from warren.runtime.config import RuntimeConfig
from warren.runtime.infrastructure import RuntimeInfra
from warren.storage.document_store.memory import MemoryDocumentStore


class Collection:
    def __init__(self) -> None:
        self.row: dict | None = None
        self.error: Exception | None = None
        self.write_concern: WriteConcern | None = None

    def with_options(self, *, write_concern: WriteConcern) -> "Collection":
        self.write_concern = write_concern
        return self

    async def update_one(
        self, query: dict, update: dict, *, upsert: bool
    ) -> UpdateResult:
        await asyncio.sleep(0)
        if self.error is not None:
            raise self.error
        matches = (
            self.row is not None
            and self.row["_id"] == query["_id"]
            and any(
                all(
                    self.row[key] <= value["$lte"]
                    if isinstance(value, dict)
                    else self.row[key] == value
                    for key, value in clause.items()
                )
                for clause in query["$or"]
            )
        )
        if matches:
            self.row.update(update["$set"])
            return UpdateResult({"n": 1, "nModified": 1}, True)
        if upsert:
            if self.row is not None:
                msg = "duplicate _id"
                raise DuplicateKeyError(msg)
            self.row = {"_id": query["_id"], **update["$set"]}
            return UpdateResult({"n": 1, "upserted": query["_id"]}, True)
        return UpdateResult({"n": 0}, True)


class Database:
    def __init__(self) -> None:
        self.collections: dict[str, Collection] = {}

    def __getitem__(self, name: str) -> Collection:
        return self.collections.setdefault(name, Collection())


class MongoClient:
    def __init__(self) -> None:
        self.databases: dict[str, Database] = {}

    def __getitem__(self, name: str) -> Database:
        return self.databases.setdefault(name, Database())


class Clock:
    now = 1000.0

    def __call__(self) -> float:
        return self.now


class Sleep:
    def __init__(self) -> None:
        self.delays: list[float] = []
        self.ready = asyncio.Event()
        self.waiter: asyncio.Future[None] | None = None

    async def __call__(self, seconds: float) -> None:
        self.delays.append(seconds)
        self.waiter = asyncio.get_running_loop().create_future()
        self.ready.set()
        await self.waiter

    async def tick(self) -> None:
        await self.ready.wait()
        self.ready.clear()
        self.waiter.set_result(None)
        await self.ready.wait()


def store() -> MemoryDocumentStore:
    return MemoryDocumentStore(collection_name="retries", doc_id_field="retry_key")


def failed(key: str, delay: int = 0) -> dict:
    return {
        "data_type": "soft-failure",
        "data": {
            "job_id": "job",
            "data_type": "input",
            "data": {"doc_id": key},
            "retry": {"after": delay},
        },
    }


def make_worker(
    client: MongoClient,
    retries: MemoryDocumentStore,
    publisher: FakePublisher,
    clock: Clock,
    sleep: Sleep,
) -> RetryWorker:
    return RetryWorker(
        "retry",
        retry_store=retries,
        republish_publisher=publisher,
        lease=MongoRetryLease(
            client, database_name="control", collection_name="retries"
        ),
        clock=clock,
        sleep=sleep,
    )


async def finish_publishing(worker: RetryWorker) -> None:
    await asyncio.gather(*worker._republish_tasks)
    await asyncio.sleep(0)


def test_mongo_lease_is_exclusive_renewable_and_expires() -> None:
    async def run() -> None:
        client = MongoClient()
        lease = MongoRetryLease(
            client, database_name="control", collection_name="custom"
        )
        results = await asyncio.gather(
            lease.acquire_or_renew("a", 1000, 1030),
            lease.acquire_or_renew("b", 1000, 1030),
        )
        assert results == [True, False]
        collection = client["control"]["custom_lease"]
        assert collection.write_concern.document == {"w": "majority"}
        assert await lease.acquire_or_renew("a", 1010, 1040)
        assert not await lease.acquire_or_renew("b", 1030, 1060)
        assert await lease.acquire_or_renew("b", 1040, 1070)
        assert not await lease.acquire_or_renew("a", 1040, 1070)
        assert collection.row == {
            "_id": "retry_worker",
            "holder_id": "b",
            "expires_at": datetime.fromtimestamp(1070, UTC),
        }

    asyncio.run(run())


def test_only_holder_publishes_including_work_received_by_standby() -> None:
    async def run() -> None:
        client, retries, clock = MongoClient(), store(), Clock()
        publishers = [FakePublisher(), FakePublisher()]
        sleeps = [Sleep(), Sleep()]
        workers = [
            make_worker(client, retries, publishers[i], clock, sleeps[i])
            for i in range(2)
        ]
        await workers[0](failed("startup"))
        await asyncio.gather(*(worker.start() for worker in workers))
        await finish_publishing(workers[0])
        assert publishers[0].published == [failed("startup")["data"]]
        assert not publishers[1].published
        assert not workers[1]._pending_timers
        await workers[1](failed("standby"))
        await workers[1].schedule_pending()
        assert not workers[1]._republish_tasks
        clock.now += 10
        await sleeps[0].tick()
        await finish_publishing(workers[0])
        assert publishers[0].published == [
            failed("startup")["data"],
            failed("standby")["data"],
        ]
        assert not publishers[1].published
        assert sleeps[0].delays == [10, 10]
        assert not [row async for row in retries.query({})]
        await asyncio.gather(*(worker.shutdown() for worker in workers))

    asyncio.run(run())


def test_standby_recovers_once_after_expiry_and_stale_holder_cannot_publish() -> None:
    async def run() -> None:
        client, retries, clock = MongoClient(), store(), Clock()
        first_sleep, second_sleep = Sleep(), Sleep()
        first_publisher, second_publisher = FakePublisher(), FakePublisher()
        first = make_worker(client, retries, first_publisher, clock, first_sleep)
        second = make_worker(client, retries, second_publisher, clock, second_sleep)
        await first.start()
        await second.start()
        await first(failed("pending", 20))
        key, timer = next(iter(first._pending_timers.items()))
        clock.now += 29
        await second_sleep.tick()
        assert not second_publisher.published
        clock.now += 1
        await second_sleep.tick()
        await finish_publishing(second)
        assert second_publisher.published == [failed("pending", 20)["data"]]
        await first._republish(key)
        await first_sleep.tick()
        assert timer.cancelled()
        assert not first._pending_timers
        assert not first_publisher.published
        clock.now += 10
        await second_sleep.tick()
        await finish_publishing(second)
        assert len(second_publisher.published) == 1
        assert not [row async for row in retries.query({})]
        await first.shutdown()
        await second.shutdown()

    asyncio.run(run())


class BlockedPublisher(FakePublisher):
    def __init__(self) -> None:
        super().__init__()
        self.started = asyncio.Event()
        self.cancelled = asyncio.Event()

    async def __call__(self, message: dict) -> None:
        self.started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.cancelled.set()
            raise


@pytest.mark.parametrize("loss", ["failure", "ownership", "expiry", "shutdown"])
def test_lease_loss_cancels_timers_and_inflight_publish(loss: str) -> None:
    async def run() -> None:
        client, retries, clock, sleep = MongoClient(), store(), Clock(), Sleep()
        publisher = BlockedPublisher()
        worker = make_worker(client, retries, publisher, clock, sleep)
        await worker(failed("inflight"))
        await worker(failed("later", 300))
        await worker.start()
        await publisher.started.wait()
        timer = next(iter(worker._pending_timers.values()))
        collection = client["control"]["retries_lease"]
        if loss == "failure":
            collection.error = ConnectionFailure("unavailable")
            await sleep.tick()
        elif loss == "ownership":
            collection.row["holder_id"] = "other"
            await sleep.tick()
        elif loss == "expiry":
            clock.now += 30
            worker._lease_timer._run()
        else:
            await worker.shutdown()
        await publisher.cancelled.wait()
        assert timer.cancelled()
        assert not worker._pending_timers
        assert len([row async for row in retries.query({})]) == 2
        assert not publisher.published
        await worker.shutdown()

    asyncio.run(run())


@pytest.mark.parametrize("backend", ["rabbitmq", "kafka", "memory"])
def test_runner_wires_lease_and_ttl(backend: str) -> None:
    async def run() -> None:
        client = MongoClient()
        connection = MemoryConnectionManager()
        await connection.setup()
        exchange = RMQExchangeConfig(name="jobs")

        def factory(consumer: MessageConsumerInterface) -> MemoryConsumerManager:
            return MemoryConsumerManager(
                connection, consumer, exchange=exchange, queue_name="retry"
            )

        config = RuntimeConfig.model_validate(
            {
                "backend": backend,
                "retry": {"collection_name": "custom", "lease_ttl_seconds": 12},
                "mongodb": {"database": "control"},
            }
        )
        runner = RetryWorkerRunner(
            config,
            "retry",
            exchange=exchange,
            retry_store=store(),
            republish_publisher=FakePublisher(),
            consumer_manager_factory=factory,
            infra=RuntimeInfra(client, None, connection),
        )
        await runner.setup()
        if backend == "memory":
            assert not client.databases
            assert runner._retry_worker._lease is None
        else:
            row = client["control"]["custom_lease"].row
            assert row["holder_id"] == runner._retry_worker._holder_id
            assert runner._retry_worker._lease_ttl == 12
        await runner.teardown()
        assert not runner._retry_worker._republish_tasks
        await connection.teardown()

    asyncio.run(run())


@pytest.mark.parametrize("ttl", [0, -1, None, 1.5])
def test_lease_ttl_must_be_a_positive_integer(ttl: object) -> None:
    assert RuntimeConfig().retry.lease_ttl_seconds == 30
    with pytest.raises(ValidationError):
        RuntimeConfig.model_validate({"retry": {"lease_ttl_seconds": ttl}})


class PausedPublisher(FakePublisher):
    def __init__(self) -> None:
        super().__init__()
        self.started = asyncio.Event()
        self.resume = asyncio.Event()

    async def __call__(self, message: dict) -> None:
        self.started.set()
        await self.resume.wait()
        await super().__call__(message)


def test_standby_replacement_survives_inflight_publish_cleanup() -> None:
    async def run() -> None:
        client, retries, clock = MongoClient(), store(), Clock()
        publisher = PausedPublisher()
        sleep = Sleep()
        first = make_worker(client, retries, publisher, clock, sleep)
        second = make_worker(client, retries, FakePublisher(), clock, Sleep())
        await first(failed("same"))
        await first.start()
        await second.start()
        await publisher.started.wait()
        original = await anext(retries.query({}))
        replacement = failed("same", 1)
        await second(replacement)
        publisher.resume.set()
        await finish_publishing(first)
        current = await retries.get_document(original["retry_key"])
        assert current["generation"] != original["generation"]
        assert current["message"] == replacement["data"]
        clock.now += 10
        await sleep.tick()
        await finish_publishing(first)
        assert publisher.published == [failed("same")["data"], replacement["data"]]
        assert not [row async for row in retries.query({})]
        await first.shutdown()
        await second.shutdown()

    asyncio.run(run())
