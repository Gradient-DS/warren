"""Unit tests for ``JobStatusWorker`` transient-store handling (B9.5 M6).

The status worker is a pure observer: a transient store blip must be
retried locally (not dropped, not re-fanned to the bus); a permanent
error or a malformed message must fail loud immediately (not retried);
and a sustained outage must drop the observation (ack) rather than
escape to the bus.
"""

import asyncio

from warren.jobs.status.job_status_worker import (
    JobStatusWorker,
)
from warren.storage.exceptions import (
    TransientStoreError,
)


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

    async def update_completion(self, **kwargs: object) -> None:
        pass


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
