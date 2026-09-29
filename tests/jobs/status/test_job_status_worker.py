"""Unit tests for ``JobStatusWorker`` transient-store handling (B9.5 M6).

The status worker is a pure observer: a transient store blip must be
retried locally (not dropped, not re-fanned to the bus); a permanent
error or a malformed message must fail loud immediately (not retried);
and a sustained outage must drop the observation (ack) rather than
escape to the bus.
"""

import asyncio

import pytest

from tests.storage.job_store_doubles import JobCollection, MongoClient
from warren.jobs.status.job_status_worker import (
    JobStatusWorker,
)
from warren.storage.exceptions import (
    TransientStoreError,
)
from warren.storage.job_results.memory import MemoryJobResultsStore
from warren.storage.jobs.memory import MemoryJobStore
from warren.storage.jobs.mongodb import MongoDBJobStore


class _FakeJobResultsStore:
    """JobResultsStore stand-in whose ``record_success`` can be told to fail.

    Fails the first ``fail_times`` calls with ``error`` (default a
    transient error), then succeeds.
    """

    def __init__(
        self,
        *,
        fail_times: int = 0,
        error: Exception | None = None,
    ) -> None:
        self.record_calls = 0
        self._fail_times = fail_times
        self._error = error or TransientStoreError("transient boom")

    async def record_success(self, **kwargs: object) -> None:
        self.record_calls += 1
        if self.record_calls <= self._fail_times:
            raise self._error

    async def record_soft_failure(self, **kwargs: object) -> None:
        self.record_calls += 1

    async def record_hard_failure(self, **kwargs: object) -> None:
        self.record_calls += 1

    async def count_completed_docs(self, job_id: str, final_data_type: str) -> int:
        return 0

    async def count_hard_failed_docs(self, job_id: str) -> int:
        return 0


class _FakeJobStore:
    """JobStore stand-in; completion short-circuits via ``num_documents=None``."""

    def __init__(self) -> None:
        self.get_job_calls = 0

    async def get_job(self, job_id: str) -> dict:
        self.get_job_calls += 1
        return {"status": {}, "num_documents": None}

    async def update_completion(self, **kwargs: object) -> bool:
        return True


_SUCCESS_MSG = {
    "data_type": "chunked_document",
    "job_id": "job-1",
    "data": {"doc_id": "d1"},
    "origin": {"type": "chunker", "name": "c1"},
}


def _worker(
    results: _FakeJobResultsStore,
    jobs: _FakeJobStore,
) -> JobStatusWorker:
    return JobStatusWorker(
        "status-test",
        job_store=jobs,  # type: ignore[arg-type]
        job_results_store=results,  # type: ignore[arg-type]
        store_retry_attempts=5,
        store_retry_base_delay=0.0,  # no real sleeps in tests
    )


def test_transient_error_is_retried_until_it_heals() -> None:
    results = _FakeJobResultsStore(fail_times=2)  # fails twice, succeeds 3rd
    jobs = _FakeJobStore()
    worker = _worker(results, jobs)

    result = asyncio.run(worker.process(dict(_SUCCESS_MSG)))

    assert result is None  # completion short-circuits (num_documents None)
    assert results.record_calls == 3  # 2 transient failures + 1 success
    assert jobs.get_job_calls == 1  # completion checked once, after success


def test_sustained_transient_error_drops_observation() -> None:
    results = _FakeJobResultsStore(fail_times=99)  # always transient-fails
    jobs = _FakeJobStore()
    worker = _worker(results, jobs)

    result = asyncio.run(worker.process(dict(_SUCCESS_MSG)))

    assert result is None  # dropped (acked), not raised
    assert results.record_calls == 5  # all attempts used
    assert jobs.get_job_calls == 0  # never got past recording


def test_permanent_error_propagates_without_retry() -> None:
    results = _FakeJobResultsStore(fail_times=1, error=ValueError("permanent"))
    jobs = _FakeJobStore()
    worker = _worker(results, jobs)

    raised = False
    try:
        asyncio.run(worker.process(dict(_SUCCESS_MSG)))
    except ValueError:
        raised = True

    assert raised
    assert results.record_calls == 1  # not retried


def test_malformed_failure_envelope_fails_loud() -> None:
    results = _FakeJobResultsStore()
    jobs = _FakeJobStore()
    worker = _worker(results, jobs)
    malformed = {"data_type": "soft-failure", "job_id": "job-1"}  # no "data"

    raised = False
    try:
        asyncio.run(worker.process(malformed))
    except KeyError:
        raised = True

    assert raised
    assert results.record_calls == 0  # never recorded; not retried


class _RacingResultsStore(MemoryJobResultsStore):
    def __init__(self) -> None:
        super().__init__()
        self._barrier = asyncio.Barrier(2)

    async def count_hard_failed_docs(self, job_id: str) -> int:
        count = await super().count_hard_failed_docs(job_id)
        await self._barrier.wait()
        return count


@pytest.mark.parametrize("backend", ["memory", "mongodb"])
def test_concurrent_completions_emit_once(backend: str) -> None:
    async def scenario() -> None:
        if backend == "memory":
            jobs = MemoryJobStore()
        else:
            client = MongoClient()
            client["test"].collections["jobs"] = JobCollection()
            jobs = MongoDBJobStore(client, database_name="test")
        results = _RacingResultsStore()
        job_id = await jobs.create_job("final", num_documents=1)
        workers = [
            JobStatusWorker(
                f"status-{i}",
                job_store=jobs,
                job_results_store=results,
            )
            for i in range(2)
        ]
        message = {"job_id": job_id, "data_type": "final", "data": {"doc_id": "a"}}
        outputs = await asyncio.gather(*(worker.process(message) for worker in workers))
        signals = [output for output in outputs if output is not None]
        assert len(signals) == 1
        assert signals[0]["data_type"] == "job-completed"
        assert signals[0]["data"] == {
            "completed_count": 1,
            "hard_failed_count": 0,
            "with_failures": False,
        }
        assert (await jobs.get_status(job_id))["completed"] is True
        assert await workers[0].process(message) is None

    asyncio.run(scenario())


def test_failures_at_multiple_stages_count_as_one_item() -> None:
    async def scenario() -> None:
        jobs = MemoryJobStore()
        results = MemoryJobResultsStore()
        job_id = await jobs.create_job("final", num_documents=2)
        worker = JobStatusWorker("status", job_store=jobs, job_results_store=results)
        for stage in ("first", "second", "second"):
            assert (
                await worker.process(
                    {
                        "job_id": job_id,
                        "data_type": "hard-failure",
                        "data": {"data_type": stage, "data": {"doc_id": "a"}},
                        "error": "failed",
                    }
                )
                is None
            )
        signal = await worker.process(
            {"job_id": job_id, "data_type": "final", "data": {"doc_id": "b"}}
        )
        assert signal is not None
        assert signal["data"] == {
            "completed_count": 1,
            "hard_failed_count": 1,
            "with_failures": True,
        }

    asyncio.run(scenario())
