"""Unit tests for ``JobDocumentsPublisher``.

The publisher's contract is that a failure on one document — *including a
failure of the tracking call itself* — must not abort publication of the
remaining documents, and a genuinely-published document must never be
reported as a failure just because its success-tracking write failed
(B9.5-ext HIGH finding). These tests pin that down by injecting a tracker
that raises.
"""

from typing import Any

import asyncio

from document_processing.distributed.warren.jobs.publishing.job_documents_publisher import (
    JobDocumentsPublisher,
)


class _FakePublisher:
    """Minimal PublisherInterface stand-in — records published messages."""

    def __init__(self) -> None:
        self.calls: list[dict] = []

    async def __call__(self, message: dict) -> None:
        self.calls.append(message)


class _FakeTracker:
    """PublishingTracker stand-in that can be told to raise."""

    def __init__(
        self,
        *,
        fail_success: bool = False,
        fail_failure_stages: tuple[str, ...] = (),
    ) -> None:
        self.successes: list[tuple] = []
        self.failures: list[tuple] = []
        self._fail_success = fail_success
        self._fail_failure_stages = set(fail_failure_stages)

    async def record_success(self, job_id: str, doc_id: str) -> None:
        if self._fail_success:
            msg = "tracker.record_success boom"
            raise RuntimeError(msg)
        self.successes.append((job_id, doc_id))

    async def record_failure(
        self,
        job_id: str,
        doc_id: str | None,
        source_id: str,
        error: str,
        stage: str,
    ) -> None:
        if stage in self._fail_failure_stages:
            msg = "tracker.record_failure boom"
            raise RuntimeError(msg)
        self.failures.append((job_id, doc_id, source_id, error, stage))


class _FakeJobStore:
    def __init__(self) -> None:
        self.num_documents: int | None = None
        self.completion: tuple[bool, bool] | None = None

    async def update_num_documents(self, job_id: str, num_documents: int) -> None:
        self.num_documents = num_documents

    async def update_completion(
        self, job_id: str, completed: bool, with_failures: bool
    ) -> None:
        self.completion = (completed, with_failures)


class _StubPublisher(JobDocumentsPublisher):
    """Concrete publisher whose load step can be told to fail per source."""

    def __init__(self, *, fail_load_for: tuple[str, ...] = (), **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._fail_load_for = set(fail_load_for)

    async def _load_document(self, source: Any) -> Any:
        if source in self._fail_load_for:
            msg = f"load boom for {source}"
            raise RuntimeError(msg)
        return {"src": source}

    async def _register_document(self, job_id: str, doc_data: Any) -> str:
        return f"doc-{doc_data['src']}"

    def _create_message(
        self, job_id: str, doc_id: str, doc_data: Any, job_parameters: dict
    ) -> dict:
        return {"job_id": job_id, "doc_id": doc_id}

    def _get_source_id(self, source: Any) -> str:
        return str(source)


async def _agen(items: list):
    for item in items:
        yield item


def test_record_success_failure_does_not_abort_or_misreport() -> None:
    """A failing ``record_success`` must not abort the loop, nor turn a
    genuinely-published document into a reported failure."""
    pub = _FakePublisher()
    tracker = _FakeTracker(fail_success=True)
    store = _FakeJobStore()
    publisher = _StubPublisher(publisher=pub, tracker=tracker, job_store=store)

    result = asyncio.run(publisher.publish_job("job-1", _agen(["a", "b", "c"])))

    # All three were published to the exchange...
    assert len(pub.calls) == 3
    # ...and all three count as published despite record_success raising.
    assert result == {"published": 3, "failed": 0, "total": 3}
    assert store.num_documents == 3


def test_record_failure_raising_does_not_abort_loop() -> None:
    """If recording a per-document failure itself raises, the loop must
    still process the remaining sources."""
    pub = _FakePublisher()
    tracker = _FakeTracker(fail_failure_stages=("load",))
    store = _FakeJobStore()
    publisher = _StubPublisher(
        publisher=pub, tracker=tracker, job_store=store, fail_load_for=("b",)
    )

    result = asyncio.run(publisher.publish_job("job-1", _agen(["a", "b", "c"])))

    # 'b' fails to load AND recording that failure raises — yet 'a' and 'c'
    # are still published.
    assert len(pub.calls) == 2
    assert result == {"published": 2, "failed": 1, "total": 3}
