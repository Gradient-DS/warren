import asyncio

import pytest

from tests.pubsub.rabbitmq.fakes import FakePublisher, FakeWorker
from tests.storage.scoping_doubles import MongoClient
from warren.jobs.status.job_status_worker import JobStatusWorker
from warren.pubsub.memory.connection import MemoryConnectionManager
from warren.pubsub.memory.consumer import MemoryConsumerManager
from warren.pubsub.memory.publisher import MemoryPublisher
from warren.pubsub.rabbitmq.config import RMQExchangeConfig
from warren.retry_management.retry_worker import RetryWorker
from warren.storage.document_store.mongodb import MongoDBDocumentStore
from warren.storage.job_results.memory import MemoryJobResultsStore
from warren.storage.jobs.memory import MemoryJobStore
from warren.storage.results.factories import create_default_results_store
from warren.storage.scoping import ScopedDatabase, current_scope
from warren.workers.workers import FilteringWorkerBase


@pytest.mark.parametrize("scope_fields", [{}, {"scope": "INVALID"}, {"scope": {}}])
def test_invalid_scope_produces_one_failure_then_goes_quiet(scope_fields: dict) -> None:
    async def run() -> None:
        mongo = MongoClient()
        results = await create_default_results_store(
            "results", mongo, scoped_database=ScopedDatabase(mongo)
        )

        class StoringWorker(FilteringWorkerBase):
            def should_process(self, message: dict) -> bool:
                return message["data_type"] == "input"

            async def process(self, message: dict) -> None:
                await results.store({}, "item", job_id=message["job_id"])

        observed: list[dict] = []

        class Observer(FakeWorker):
            async def __call__(self, message: dict) -> None:
                observed.append(message)

        jobs, ledger = MemoryJobStore(), MemoryJobResultsStore()
        job_id = await jobs.create_job("output", num_documents=1)
        connection = MemoryConnectionManager()
        await connection.setup()
        exchange = RMQExchangeConfig(name="scopes", type="fanout")
        publisher = MemoryPublisher(connection, exchange)
        retry_store = MongoDBDocumentStore(
            mongo,
            database_name="control",
            collection_name="retries",
            doc_id_field="retry_key",
        )
        retry = RetryWorker(
            "retry",
            retry_store=retry_store,
            republish_publisher=publisher,
            scoping_enabled=True,
        )
        workers = [
            StoringWorker("store"),
            JobStatusWorker("status", job_store=jobs, job_results_store=ledger),
            retry,
            Observer(),
        ]
        managers = []
        for i, worker in enumerate(workers):
            manager = MemoryConsumerManager(
                connection,
                worker,
                exchange=exchange,
                queue_name=f"scopes.{i}",
                data_publisher=publisher,
                control_publisher=publisher,
            )
            await manager.setup()
            managers.append(manager)
        await publisher(
            {
                "data_type": "input",
                "data": {"doc_id": "item"},
                "job_id": job_id,
                "origin": {"type": "source", "name": "1"},
                **scope_fields,
            }
        )
        deliveries = 0
        while any(not manager._queue.empty() for manager in managers):
            for manager in managers:
                if not manager._queue.empty():
                    deliveries += 1
                    assert deliveries <= 12, "failure envelopes did not settle"
                    await manager._process_message(manager._queue.get_nowait())
        assert [message["data_type"] for message in observed] == [
            "input",
            "hard-failure",
            "job-completed",
        ]
        failure = observed[1]
        assert (
            "missing scope" in failure["error"]
            if not scope_fields
            else "malformed scope" in failure["error"]
        )
        for message in observed:
            assert (
                {"scope": message["scope"]} if "scope" in message else {}
            ) == scope_fields
        assert await ledger.count_hard_failed_docs(job_id) == 1
        assert not retry._pending_timers
        assert "wr_" not in mongo.databases
        assert current_scope.get() is None
        for manager in managers:
            await manager.stop_consuming()

    asyncio.run(run())


@pytest.mark.parametrize("scope", [None, "a", "INVALID", {}])
def test_retry_control_storage_and_replay_do_not_validate_scope(scope: object) -> None:
    async def run() -> None:
        mongo, publisher = MongoClient(), FakePublisher()
        store = MongoDBDocumentStore(
            mongo,
            database_name="control",
            collection_name="retries",
            doc_id_field="retry_key",
        )
        worker = RetryWorker(
            "retry",
            retry_store=store,
            republish_publisher=publisher,
            scoping_enabled=True,
        )
        failed = {
            "data_type": "input",
            "data": {"doc_id": "item"},
            "job_id": "job",
            "scope": scope,
            "retry": {"after": 3600},
        }
        token = current_scope.set(scope)
        try:
            await worker({"data_type": "soft-failure", "data": failed, "scope": scope})
        finally:
            current_scope.reset(token)
        row = mongo["control"]["retries"].rows[0]
        assert row.get("scope") == ("a" if scope == "a" else None)
        await worker.shutdown()
        await worker.schedule_pending()
        await worker._republish(row["retry_key"])
        assert publisher.published == [failed]
        assert not mongo["control"]["retries"].rows
        await worker.shutdown()

    asyncio.run(run())
