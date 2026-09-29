import asyncio

import pytest

from tests.storage.results.doubles import MongoClient, RedisClient
from tests.storage.results.test_batch import make_store
from warren.storage.memory_registry import MemoryStoreRegistry
from warren.storage.results import DocumentProcessingResultsNotFound, ResultItem


@pytest.mark.parametrize("try_cache", [True, False])
@pytest.mark.parametrize("change", ["evict", "rewrite"])
def test_stream_reads_complete_sorted_results(change: str, try_cache: bool) -> None:
    async def run() -> None:
        mongo, redis = MongoClient(), RedisClient()
        store = make_store(mongo, redis)
        await store.store_many(
            [ResultItem({"value": index}, "a", index, "j") for index in [2, 0, 1]]
        )
        await store.store({"value": "other job"}, "a", job_id="other")
        await store.store({"value": "other item"}, "b", job_id="j")
        if change == "evict":
            redis.entries.pop(f"results:{store._build_cache_key('a', 1, 'j')}")
        else:
            redis.entries.clear()
            await store.store({"value": 9}, "a", part_idx=1, job_id="j")
        entries = dict(redis.entries)
        rows = [
            row
            async for row in store.stream_doc_processing_results(
                "a", "j", try_cache=try_cache
            )
        ]
        assert [row.part_idx for row in rows] == [0, 1, 2]
        assert [row.result["value"] for row in rows] == [
            0,
            9 if change == "rewrite" else 1,
            2,
        ]
        assert redis.scan_calls == 0
        assert mongo.query_calls == 1
        assert redis.entries == entries
        assert all(row.result_id for row in rows)

    asyncio.run(run())


def test_stream_ignores_a_fully_populated_cache() -> None:
    async def run() -> None:
        mongo, redis = MongoClient(), RedisClient()
        store = make_store(mongo, redis)
        await store.store({"value": 1}, "a")
        await store.get_result("a")
        assert mongo.query_calls == 0
        mongo.rows.clear()
        with pytest.raises(DocumentProcessingResultsNotFound):
            _ = [row async for row in store.stream_doc_processing_results("a")]
        assert redis.scan_calls == 0
        assert mongo.query_calls == 1

    asyncio.run(run())


def test_memory_stream_has_the_same_ordering() -> None:
    async def run() -> None:
        store = await MemoryStoreRegistry().results_store("results")
        await store.store_many([ResultItem({}, "a", index) for index in [2, 0, 1]])
        rows = [row async for row in store.stream_doc_processing_results("a")]
        assert [row.part_idx for row in rows] == [0, 1, 2]

    asyncio.run(run())
