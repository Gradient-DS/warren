import asyncio
from fnmatch import fnmatchcase

import pytest
from pymongo.errors import OperationFailure

from tests.pubsub.rabbitmq.fakes import FakePublisher
from tests.storage.scoping_doubles import MongoClient, RedisClient
from warren.common import HardFailureException
from warren.retry_management.retry_worker import RetryWorker
from warren.runtime.infrastructure import RuntimeInfra
from warren.storage.document_store.mongodb import MongoDBDocumentStore
from warren.storage.results.factories import create_default_results_store
from warren.storage.scoping import ScopedDatabase, current_scope, erase_scope


class EraseMongo(MongoClient):
    def __init__(self, error_code: int | None = None) -> None:
        super().__init__()
        self.error_code = error_code
        self.drop_calls: list[str] = []

    async def drop_database(self, name: str) -> None:
        self.drop_calls.append(name)
        if self.error_code is not None:
            reason = "database drop failed"
            raise OperationFailure(reason, code=self.error_code)
        await super().drop_database(name)


class EraseRedis(RedisClient):
    def __init__(self) -> None:
        super().__init__()
        self.snapshot: list[bytes] = []
        self.unlink_batches: list[int] = []

    async def scan(
        self, *, cursor: int, match: str, count: int = 10
    ) -> tuple[int, list[bytes]]:
        assert count == 1000
        self.scan_calls += 1
        if cursor == 0:
            self.snapshot = [
                key.encode() for key in self.entries if fnmatchcase(key, match)
            ]
        # COUNT is a hint; a page may exceed it.
        end = cursor + 1250
        return (end if end < len(self.snapshot) else 0), self.snapshot[cursor:end]

    async def unlink(self, *keys: bytes) -> int:
        assert 0 < len(keys) <= 1000
        self.unlink_batches.append(len(keys))
        return await self.delete(*(key.decode() for key in keys))


@pytest.mark.parametrize("fallback", [False, True])
@pytest.mark.parametrize("use_infra", [False, True])
def test_erase_removes_only_the_selected_scope(fallback: bool, use_infra: bool) -> None:
    async def run() -> None:
        mongo, redis = EraseMongo(13 if fallback else None), EraseRedis()
        for scope in ("a", "b"):
            for name in ("results", "binary", "documents"):
                await mongo[f"wr_{scope}"][name].insert_one({"value": scope})
        for name in (
            "jobs",
            "job_results",
            "job_publishing_results",
            "retries",
            "custom_retries",
        ):
            for scope in ("a", "b", None):
                await mongo["control"][name].insert_one({"scope": scope})
        redis.entries = {f"s:a:results:{i}": b"payload" for i in range(2305)}
        redis.entries.update(
            {"s:b:results:0": b"b", "s:ab:results:0": b"ab", "results:0": b"unscoped"}
        )
        clients = RuntimeInfra(mongo, redis, None) if use_infra else (mongo, redis)
        counts = await erase_scope(
            clients, "a", control_database="control", prefix="wr_"
        )
        assert counts == {"collections": 3, "redis_keys": 2305, "control_rows": 5}
        assert redis.scan_calls == 2
        assert redis.unlink_batches == [1000, 250, 1000, 55]
        assert mongo.drop_calls == ["wr_a"]
        assert not mongo["wr_a"].collections
        assert len(mongo["wr_b"].collections) == 3
        assert redis.entries == {
            "s:b:results:0": b"b",
            "s:ab:results:0": b"ab",
            "results:0": b"unscoped",
        }
        for collection in mongo["control"].collections.values():
            assert collection.rows == [{"scope": "b"}, {"scope": None}]
        assert await erase_scope(
            clients, "a", control_database="control", prefix="wr_"
        ) == {"collections": 0, "redis_keys": 0, "control_rows": 0}

    asyncio.run(run())


def test_drop_failure_other_than_permission_denied_propagates() -> None:
    async def run() -> None:
        mongo, redis = EraseMongo(91), EraseRedis()
        await mongo["wr_a"]["results"].insert_one({"value": 1})
        with pytest.raises(OperationFailure) as error:
            await erase_scope(
                (mongo, redis), "a", control_database="control", prefix="wr_"
            )
        assert error.value.code == 91
        assert mongo["wr_a"]["results"].rows == [{"value": 1}]
        assert redis.scan_calls == 0

    asyncio.run(run())


@pytest.mark.parametrize("scope", ["", "a*", "A", "a" * 41, "a\n"])
def test_invalid_erase_scope_does_no_io(scope: str) -> None:
    mongo, redis = EraseMongo(), EraseRedis()
    with pytest.raises(HardFailureException, match="malformed scope"):
        asyncio.run(
            erase_scope((mongo, redis), scope, control_database="control", prefix="wr_")
        )
    assert not mongo.databases
    assert redis.scan_calls == 0


def test_control_database_cannot_be_dropped() -> None:
    mongo, redis = EraseMongo(), EraseRedis()
    with pytest.raises(ValueError, match="must differ"):
        asyncio.run(
            erase_scope((mongo, redis), "a", control_database="wr_a", prefix="wr_")
        )
    assert not mongo.drop_calls


def test_erased_retry_is_not_republished_and_new_content_recreates_indexes() -> None:
    async def run() -> None:
        mongo, redis = EraseMongo(), EraseRedis()
        resolver = ScopedDatabase(mongo)
        results = await create_default_results_store(
            "results", mongo, redis, scoped_database=resolver
        )
        retry_store = MongoDBDocumentStore(
            mongo,
            database_name="control",
            collection_name="retries",
            doc_id_field="retry_key",
        )
        publisher = FakePublisher()
        worker = RetryWorker(
            "retry",
            retry_store=retry_store,
            republish_publisher=publisher,
            scoping_enabled=True,
        )
        token = current_scope.set("a")
        try:
            await results.store({"value": 1}, "item")
            failed = {
                "scope": "a",
                "job_id": "job",
                "data": {"doc_id": "item"},
                "retry": {"after": 3600},
            }
            await worker({"scope": "a", "data_type": "soft-failure", "data": failed})
            retry_key = next(iter(worker._pending_timers))
            await worker.shutdown()
            await erase_scope(
                (mongo, redis), "a", control_database="control", prefix="wr_"
            )
            await worker._republish(retry_key)
            assert not publisher.published
            assert not redis.entries
            await results.store({"value": 2}, "item")
            assert mongo["wr_a"]["results"].index_calls == 2
            assert (await results.get_result("item")).result == {"value": 2}
        finally:
            current_scope.reset(token)
            await worker.shutdown()

    asyncio.run(run())
