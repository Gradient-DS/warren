import asyncio
from unittest.mock import AsyncMock

import pytest

from tests.storage.job_store_doubles import JobCollection, MongoClient
from warren.common import MessageConsumerInterface
from warren.jobs.status.job_status_worker import JobStatusWorker
from warren.jobs.status.job_status_worker_runner import JobStatusWorkerRunner
from warren.pubsub.rabbitmq.config import RMQExchangeConfig
from warren.runtime.config import RuntimeConfig
from warren.runtime.infrastructure import RuntimeInfra
from warren.storage.job_results.memory import MemoryJobResultsStore
from warren.storage.jobs.memory import MemoryJobStore
from warren.storage.jobs.mongodb import MongoDBJobStore


CAPABILITIES = {
    "parser": (frozenset({"raw"}), "text"),
    "finisher": (frozenset({"text"}), "final"),
}


def observation(job_id: str, doc_id: str = "own", origin: str = "finisher") -> dict:
    return {
        "job_id": job_id,
        "data_type": "final",
        "data": {"doc_id": doc_id},
        "origin": {"type": origin, "name": "instance"},
    }


def failure(job_id: str, doc_id: str = "own", consumer: str = "finisher") -> dict:
    return {
        "job_id": job_id,
        "data_type": "hard-failure",
        "data": {**observation(job_id, doc_id, "parser"), "data_type": "text"},
        "origin": {"type": consumer, "name": "instance"},
    }


@pytest.mark.parametrize("backend", ["memory", "mongodb"])
@pytest.mark.parametrize("kind", ["success", "hard-failure"])
def test_other_jobs_document_cannot_resolve_job(backend: str, kind: str) -> None:
    async def run() -> None:
        if backend == "memory":
            jobs = MemoryJobStore()
        else:
            client = MongoClient()
            client["test"].collections["jobs"] = JobCollection()
            jobs = MongoDBJobStore(client, database_name="test")
        job_id = await jobs.create_job("final", num_documents=1, doc_ids=["own"])
        results = MemoryJobResultsStore()
        worker = JobStatusWorker("status", job_store=jobs, job_results_store=results, capabilities=CAPABILITIES)
        message = observation(job_id, "other") if kind == "success" else failure(job_id, "other")
        assert await worker(message) is None
        assert not (await jobs.get_status(job_id))["completed"]
        assert await results.get_doc_status(job_id, "other") == []
        assert await worker(observation(job_id)) is not None

    asyncio.run(run())


@pytest.mark.parametrize("origin", ["unknown", "parser", "publisher"])
def test_final_result_must_match_known_producer(origin: str) -> None:
    async def run() -> None:
        jobs, results = MemoryJobStore(), MemoryJobResultsStore()
        job_id = await jobs.create_job("final", num_documents=1)
        worker = JobStatusWorker("status", job_store=jobs, job_results_store=results, capabilities=CAPABILITIES)
        assert await worker(observation(job_id, origin=origin)) is None
        assert not (await jobs.get_status(job_id))["completed"]
        assert await results.get_doc_status(job_id, "own") == []

    asyncio.run(run())


@pytest.mark.parametrize("kind", ["soft-failure", "hard-failure"])
@pytest.mark.parametrize("change", ["consumer", "producer", "input", "job_id"])
def test_failure_envelope_must_match_job_and_capabilities(change: str, kind: str) -> None:
    async def run() -> None:
        jobs, results = MemoryJobStore(), MemoryJobResultsStore()
        job_id = await jobs.create_job("final", num_documents=1)
        worker = JobStatusWorker("status", job_store=jobs, job_results_store=results, capabilities=CAPABILITIES)
        message = failure(job_id)
        message["data_type"] = kind
        if change == "consumer":
            message["origin"]["type"] = "unknown"
        elif change == "producer":
            message["data"]["origin"]["type"] = "unknown"
        elif change == "input":
            message["origin"]["type"] = "parser"
        else:
            message["data"]["job_id"] = "other-job"
        assert await worker(message) is None
        assert not (await jobs.get_status(job_id))["completed"]
        assert await results.get_doc_status(job_id, "own") == []
        assert await worker(failure(job_id)) is not None

    asyncio.run(run())


@pytest.mark.parametrize("doc_ids", [None, ["late"]])
def test_late_document_count_keeps_working(doc_ids: list[str] | None) -> None:
    async def run() -> None:
        jobs, results = MemoryJobStore(), MemoryJobResultsStore()
        job_id = await jobs.create_job("final", doc_ids=doc_ids)
        worker = JobStatusWorker("status", job_store=jobs, job_results_store=results, capabilities=CAPABILITIES)
        assert await worker(observation(job_id, "late")) is None
        await jobs.increment_num_documents(job_id)
        assert await worker(observation(job_id, "late")) is not None

    asyncio.run(run())


def test_trusted_job_routing_restricts_origins() -> None:
    async def run() -> None:
        jobs, results = MemoryJobStore(), MemoryJobResultsStore()
        job_id = await jobs.create_job(
            "final", num_documents=1, parameters={"routing": {"entry": ["finisher"], "edges": {"finisher": []}}}
        )
        worker = JobStatusWorker("status", job_store=jobs, job_results_store=results)
        forged = observation(job_id, origin="unknown")
        forged["job_parameters"] = {"routing": {"entry": ["unknown"], "edges": {"unknown": []}}}
        assert await worker(forged) is None
        assert await worker(observation(job_id)) is not None

    asyncio.run(run())


def test_existing_unrelated_ledger_rows_do_not_count() -> None:
    async def run() -> None:
        jobs, results = MemoryJobStore(), MemoryJobResultsStore()
        job_id = await jobs.create_job("final", num_documents=2, doc_ids=["own", "later"])
        await results.record_success(job_id, "final", "other", "finisher", "instance")
        worker = JobStatusWorker("status", job_store=jobs, job_results_store=results, capabilities=CAPABILITIES)
        assert await worker(observation(job_id)) is None
        assert not (await jobs.get_status(job_id))["completed"]
        assert await worker(observation(job_id, "later")) is not None

    asyncio.run(run())


def test_rejected_observation_does_not_check_completion() -> None:
    jobs, results = AsyncMock(), AsyncMock()
    jobs.get_job.return_value = {"doc_ids": [], "num_documents": 0, "final_data_type": "final"}
    worker = JobStatusWorker("status", job_store=jobs, job_results_store=results)
    assert asyncio.run(worker(observation("job"))) is None
    jobs.update_completion.assert_not_awaited()
    results.count_completed_docs.assert_not_awaited()


def test_status_runner_passes_trusted_capabilities_to_worker() -> None:
    async def run() -> None:
        jobs, results, manager = MemoryJobStore(), MemoryJobResultsStore(), AsyncMock()
        job_id = await jobs.create_job("final", num_documents=1)
        workers: list[MessageConsumerInterface] = []

        def factory(worker: MessageConsumerInterface) -> AsyncMock:
            workers.append(worker)
            return manager

        runner = JobStatusWorkerRunner(
            RuntimeConfig(backend="memory"),
            "status",
            exchange=RMQExchangeConfig(name="jobs"),
            job_store=jobs,
            job_results_store=results,
            capabilities=CAPABILITIES,
            consumer_manager_factory=factory,
            infra=RuntimeInfra(None, None, None),
        )
        await runner.setup()
        try:
            assert await workers[0](observation(job_id, origin="unknown")) is None
            assert await results.get_doc_status(job_id, "own") == []
            assert await workers[0](observation(job_id)) is not None
        finally:
            await runner.teardown()

    asyncio.run(run())


def test_publisher_can_finish_a_job_without_a_declared_worker_output() -> None:
    async def run() -> None:
        jobs, results = MemoryJobStore(), MemoryJobResultsStore()
        job_id = await jobs.create_job("final", num_documents=1)
        worker = JobStatusWorker("status", job_store=jobs, job_results_store=results, capabilities={})
        assert await worker(observation(job_id, origin="publisher")) is not None

    asyncio.run(run())
