import asyncio
from unittest.mock import AsyncMock

import pytest

from warren.retry_management.retry_worker import RetryWorker
from warren.storage.document_store.memory import MemoryDocumentStore


def failure(doc_id: str, count: int) -> dict:
    return {
        "data_type": "soft-failure",
        "job_id": "job",
        "data": {
            "job_id": "job",
            "data_type": "input",
            "data": {"doc_id": doc_id},
            "retry": {"count": count, "after": 60},
        },
    }


@pytest.mark.parametrize("legacy", [False, True])
def test_pending_generation_cannot_be_replaced(legacy: bool) -> None:
    async def run() -> None:
        store = MemoryDocumentStore(collection_name="retries", doc_id_field="retry_key")
        publisher = AsyncMock()
        first = RetryWorker("first", retry_store=store, republish_publisher=publisher)
        second = RetryWorker("second", retry_store=store, republish_publisher=publisher)
        try:
            await first(failure("doc", 2))
            original = await anext(store.query({}))
            if legacy:
                original.pop("generation")
                await store.insert(original, overwrite_existing=True)
            timer = first._pending_timers[original["retry_key"]]
            await first(failure("doc", 1))
            await second(failure("doc", 99))
            assert await store.get_document(original["retry_key"]) == original
            assert first._pending_timers[original["retry_key"]] is timer
            assert second._pending_timers == {}
            await first._republish(original["retry_key"], original.get("generation"))
            publisher.assert_awaited_once_with(failure("doc", 2)["data"])
            await second(failure("doc", 3))
            assert (await store.get_document(original["retry_key"]))["message"] == failure("doc", 3)["data"]
        finally:
            await first.shutdown()
            await second.shutdown()

    asyncio.run(run())


def test_concurrent_receipts_preserve_first_generation() -> None:
    async def run() -> None:
        store = MemoryDocumentStore(collection_name="retries", doc_id_field="retry_key")
        workers = [RetryWorker(str(i), retry_store=store, republish_publisher=AsyncMock()) for i in range(2)]
        try:
            await asyncio.gather(*(worker(failure("doc", i)) for i, worker in enumerate(workers)))
            rows = [row async for row in store.query({})]
            assert len(rows) == 1
            assert rows[0]["message"] == failure("doc", 0)["data"]
            assert sum(len(worker._pending_timers) for worker in workers) == 1
        finally:
            for worker in workers:
                await worker.shutdown()

    asyncio.run(run())
