import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest
from bson import BSON

from tests.storage.results.doubles import MongoClient, RedisClient
from warren.pubsub.rabbitmq.config import RMQExchangeConfig
from warren.retry_management.retry_worker import RetryWorker
from warren.retry_management.retry_worker_runner import RetryWorkerRunner
from warren.runtime.config import RuntimeConfig
from warren.runtime.infrastructure import RuntimeInfra
from warren.storage.document_store.memory import MemoryDocumentStore


@pytest.mark.parametrize("ttl", [None, 60, 604800])
def test_retry_runtime_configures_ttl_from_due_date(ttl: int | None) -> None:
    async def run() -> None:
        config = RuntimeConfig.model_validate({"retention": {"retry_records_ttl_seconds": ttl}})
        mongo = MongoClient()
        runner = RetryWorkerRunner(
            config, "retry", exchange=RMQExchangeConfig(name="jobs"), infra=RuntimeInfra(mongo, RedisClient(), None)
        )
        await runner._create_default_retry_store()
        indexes = mongo[config.mongodb.database]["retries"].indexes
        ttls = [spec for spec in indexes.values() if "expireAfterSeconds" in spec]
        assert ttls == ([] if ttl is None else [{"key": [("fire_at_date", 1)], "expireAfterSeconds": ttl}])

    asyncio.run(run())


def test_new_and_legacy_retries_have_bson_due_dates() -> None:
    async def run() -> None:
        store = MemoryDocumentStore(collection_name="retries", doc_id_field="retry_key")
        worker = RetryWorker("retry", retry_store=store, republish_publisher=AsyncMock(), clock=lambda: 1000)
        try:
            await worker({"data_type": "soft-failure", "data": {"data": {}, "retry": {"after": 300}}})
            row = await anext(store.query({}))
            assert row["fire_at_date"] == datetime.fromtimestamp(1300, UTC)
            decoded = BSON(BSON.encode(row)).decode()
            assert isinstance(decoded["fire_at_date"], datetime)
            await store.insert({"retry_key": "legacy", "message": {"data": {}}, "fire_at": 1600})
            await worker.schedule_pending()
            assert (await store.get_document("legacy"))["fire_at_date"] == datetime.fromtimestamp(1600, UTC)
        finally:
            await worker.shutdown()

    asyncio.run(run())


def test_retry_retention_default_is_one_week_after_due() -> None:
    assert RuntimeConfig().retention.retry_records_ttl_seconds == 604800


@pytest.mark.parametrize("fire_at", [float("nan"), float("inf"), -1, True, "soon", 1e300])
def test_invalid_persisted_date_does_not_block_recovery(fire_at: object) -> None:
    async def run() -> None:
        store = MemoryDocumentStore(collection_name="retries", doc_id_field="retry_key")
        worker = RetryWorker("retry", retry_store=store, republish_publisher=AsyncMock(), clock=lambda: 1000)
        await store.insert({"retry_key": "bad", "message": {"data": {}}, "fire_at": fire_at})
        await store.insert({"retry_key": "good", "message": {"data": {}}, "fire_at": 1600})
        try:
            await worker.schedule_pending()
            assert set(worker._pending_timers) == {"good"}
        finally:
            await worker.shutdown()

    asyncio.run(run())
