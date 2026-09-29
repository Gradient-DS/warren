import asyncio
from dataclasses import asdict
from datetime import UTC, datetime

import pytest

from tests.storage.results.doubles import MongoClient, RedisClient
from warren.storage.cache.redis import RedisDictCache
from warren.storage.document_store.memory import MemoryDocumentStore
from warren.storage.document_store.mongodb import MongoDBDocumentStore
from warren.storage.results import DefaultResultsStore, ResultItem


_ITEMS = [
    ResultItem({"value": 1}, "a", job_id="j", result_metadata={"source": "test"}),
    ResultItem({"value": 2}, "a", part_idx=1, job_id="j"),
    ResultItem({"value": 3}, "b"),
]


def make_store(
    client: MongoClient, redis: RedisClient, id_field: str = "result_id"
) -> DefaultResultsStore:
    return DefaultResultsStore(
        MongoDBDocumentStore(
            client,
            database_name="test",
            collection_name="results",
            doc_id_field=id_field,
            unique_indexes=[("doc_id", "job_id", "part_idx")],
        ),
        RedisDictCache(redis, base_key="results", default_ttl_seconds=3600),
    )


def test_batch_matches_single_writes_and_uses_one_round_trip() -> None:
    async def run() -> None:
        single, batch = MongoClient(), MongoClient()
        redis = RedisClient()
        single_store = make_store(single, RedisClient())
        batch_store = make_store(batch, redis)
        before = datetime.now(UTC)
        for item in _ITEMS:
            await single_store.store(**asdict(item))
        await batch_store.store_many(_ITEMS)
        after = datetime.now(UTC)
        assert batch.bulk_calls == 1
        assert batch.single_calls == batch.query_calls == 0
        assert single.single_calls == len(_ITEMS)
        assert redis.pipeline_calls == redis.execute_calls == 1
        assert len(redis.entries) == len(_ITEMS)
        for left, right in zip(single.rows, batch.rows, strict=True):
            assert left.keys() == right.keys()
            assert before <= left["created_at"] <= after
            assert before <= right["created_at"] <= after
            for key in left.keys() - {"_id", "result_id", "created_at"}:
                assert left[key] == right[key]
        await batch_store.store_many(_ITEMS)
        assert len(batch.rows) == len(_ITEMS)
        assert batch.bulk_calls == 2
        assert batch.single_calls == batch.query_calls == 0
        for item in _ITEMS:
            result = await batch_store.get_result(
                item.doc_id, item.part_idx, item.job_id
            )
            assert result.result == item.result
            assert result.result_id
        assert batch.query_calls == 0

    asyncio.run(run())


def test_empty_batch_does_no_io() -> None:
    mongo, redis = MongoClient(), RedisClient()
    asyncio.run(make_store(mongo, redis).store_many([]))
    assert mongo.bulk_calls == mongo.single_calls == mongo.query_calls == 0
    assert redis.pipeline_calls == 0


@pytest.mark.parametrize("backend", ["memory", "mongo"])
def test_repeated_keys_keep_the_last_item(backend: str) -> None:
    async def run() -> None:
        if backend == "memory":
            store = DefaultResultsStore(
                MemoryDocumentStore(
                    collection_name="results",
                    doc_id_field="result_id",
                    unique_indexes=[("doc_id", "job_id", "part_idx")],
                )
            )
        else:
            store = make_store(MongoClient(), RedisClient())
        items = [*_ITEMS, ResultItem({"value": 9}, "a", part_idx=0, job_id="j")]
        for _ in range(2):
            await store.store_many(items)
            rows = [row async for row in store._document_store.query({})]
            assert len(rows) == len(_ITEMS)
            assert (await store.get_result("a", job_id="j")).result == {"value": 9}

    asyncio.run(run())


def test_existing_mongodb_id_is_preserved_and_resolved_on_cache_read() -> None:
    async def run() -> None:
        mongo = MongoClient()
        store = make_store(mongo, RedisClient(), "_id")
        await store.store_many(_ITEMS)
        first = await store.get_result("a", job_id="j")
        assert mongo.query_calls == 0
        await store.store_many(_ITEMS)
        second = await store.get_result("a", job_id="j")
        assert second.result_id == first.result_id
        assert mongo.query_calls == 1

    asyncio.run(run())


class UnavailableRedis(RedisClient):
    def pipeline(self) -> None:
        from redis.exceptions import ConnectionError

        msg = "unavailable"
        raise ConnectionError(msg)


def test_cache_failure_does_not_undo_persistence() -> None:
    async def run() -> None:
        mongo = MongoClient()
        await make_store(mongo, UnavailableRedis()).store_many(_ITEMS)
        assert len(mongo.rows) == len(_ITEMS)
        assert mongo.bulk_calls == 1

    asyncio.run(run())
