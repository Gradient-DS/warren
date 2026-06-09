"""
Storage protocol for publishing outcomes.

Tracks what happened when documents were submitted for processing.
Independent from ``JobResultsStoreInterface``, which tracks what
happened *after* messages reached the exchange.
"""

from typing import Protocol


class PublishingTrackerInterface(Protocol):
    """Storage protocol for publishing outcomes."""

    async def setup(self) -> None:
        """Create indexes and prepare the store for use."""
        ...

    async def record_success(
        self,
        job_id: str,
        doc_id: str,
    ) -> None:
        """Record a successfully published document."""
        ...

    async def record_failure(
        self,
        job_id: str,
        doc_id: str | None,
        source: str,
        error: str,
        stage: str,
    ) -> None:
        """Record a failed publish attempt.

        :param doc_id: Document ID, or ``None`` if the failure
            occurred before a doc_id could be assigned.
        :param source: Human-readable source identifier for
            diagnostics.
        :param error: Error message describing what went wrong.
        :param stage: Which step failed: ``"load"``, ``"register"``,
            or ``"publish"``.
        """
        ...

    async def get_results(self, job_id: str) -> list[dict]:
        """Get all publishing results for a job.

        :return: List of result records, each containing at least
            ``{doc_id, time, success}``. Failure records additionally
            contain ``{source, error, stage}``.
        """
        ...

    async def get_failures(self, job_id: str) -> list[dict]:
        """Get only publishing failures for a job.

        :return: List of failure records (``success=False``).
        """
        ...
