import asyncio
from unittest.mock import AsyncMock

import pytest

from tests.pubsub.rabbitmq.fakes import FakePublisher, FakeWorker, make_manager
from warren.common import SoftFailureException
from warren.pubsub.common import RetryConfig
from warren.retry_management.retry_worker import RetryWorker
from warren.storage.document_store.memory import MemoryDocumentStore


@pytest.mark.parametrize("field", ["after", "count"])
@pytest.mark.parametrize("value", [float("nan"), float("inf"), -1, "1", True, None, [], {}])
def test_invalid_retry_is_not_persisted(field: str, value: object) -> None:
    async def run() -> None:
        store = MemoryDocumentStore(collection_name="retries", doc_id_field="retry_key")
        worker = RetryWorker("retry", retry_store=store, republish_publisher=AsyncMock())
        message = {"data_type": "soft-failure", "data": {"data": {}, "retry": {field: value}}}
        try:
            with pytest.raises(ValueError, match="retry"):
                await worker(message)
            assert [row async for row in store.query({})] == []
        finally:
            await worker.shutdown()

    asyncio.run(run())


@pytest.mark.parametrize("field", ["retry_after", "backoff_base", "retry_max"])
@pytest.mark.parametrize("value", [float("nan"), float("inf"), -1, "1", True])
def test_invalid_worker_retry_intent_is_rejected(field: str, value: object) -> None:
    manager = make_manager(FakeWorker())
    error = SoftFailureException("later", **{field: value})
    resolve = (
        manager._resolve_retry_max if field == "retry_max" else lambda error: manager._resolve_retry_after(error, 1)
    )
    with pytest.raises(ValueError, match=r"retry|backoff"):
        resolve(error)


def test_large_backoff_saturates_without_overflow() -> None:
    manager = make_manager(FakeWorker(), retry_config=RetryConfig(jitter=False))
    assert manager._resolve_retry_after(SoftFailureException("later", backoff_base=1e300), 10) == 300


def test_scheduler_caps_delay_with_policy() -> None:
    async def run() -> None:
        store = MemoryDocumentStore(collection_name="retries", doc_id_field="retry_key")
        worker = RetryWorker(
            "retry", retry_store=store, republish_publisher=FakePublisher(), retry_config=RetryConfig(max_delay_cap=42)
        )
        try:
            await worker({"data_type": "soft-failure", "data": {"retry": {"after": 1e300}, "data": {}}})
            row = await anext(store.query({}))
            assert row["retry_after_seconds"] == 42
        finally:
            await worker.shutdown()

    asyncio.run(run())
