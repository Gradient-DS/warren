"""
Storage protocol for per-document processing results.

Each record represents one document's outcome at one pipeline stage,
keyed by ``(job_id, data_type, doc_id)``. Separated from
``JobStoreInterface`` because it has different concerns: recording and
querying granular per-document outcomes, rather than managing job
definitions and high-level status.
"""

from typing import Protocol


class JobResultsStoreInterface(Protocol):
    """Storage protocol for per-document processing results.

    **Transient-error contract:** transient backend failures (connection
    blips, failover) are raised as ``TransientStoreError`` (see
    ``storage/exceptions.py``); permanent failures propagate as other
    exceptions. Callers that cannot tolerate a dropped operation — notably
    the ``JobStatusWorker`` observer, where a lost write strands a job —
    should retry on ``TransientStoreError``.
    """

    async def setup(self) -> None:
        """Create indexes and prepare the store for use."""
        ...

    # --- Recording results ---

    async def record_success(
        self,
        job_id: str,
        data_type: str,
        doc_id: str,
        origin_type: str,
        origin_name: str,
    ) -> None:
        """Record a successful processing result.

        Upserts the ``(job_id, data_type, doc_id)`` record. If the
        record previously held a soft-failure, it is overwritten with
        success and failure-specific fields are cleared.
        """
        ...

    async def record_soft_failure(
        self,
        job_id: str,
        data_type: str,
        doc_id: str,
        origin_type: str,
        origin_name: str,
        consumer_type: str,
        consumer_name: str,
        retry_count: int,
        error_reason: str,
    ) -> None:
        """Record a soft (retryable) failure.

        Upserts the ``(job_id, data_type, doc_id)`` record. Appends
        ``error_reason`` to the ``soft_failures`` list so the full
        retry history is preserved.

        :param origin_type: Who produced the message that failed.
        :param consumer_type: Who failed to consume it.
        """
        ...

    async def record_hard_failure(
        self,
        job_id: str,
        data_type: str,
        doc_id: str,
        origin_type: str,
        origin_name: str,
        consumer_type: str,
        consumer_name: str,
        error: str,
    ) -> None:
        """Record a hard (permanent) failure.

        Upserts the ``(job_id, data_type, doc_id)`` record. This
        document will not be retried.

        :param origin_type: Who produced the message that failed.
        :param consumer_type: Who failed to consume it.
        """
        ...

    # --- Counting ---

    async def count_completed_docs(
        self,
        job_id: str,
        final_data_type: str,
    ) -> int:
        """Count documents that reached the final stage successfully.

        :return: Number of documents with ``success=True`` for
            ``final_data_type``.
        """
        ...

    async def count_hard_failed_docs(self, job_id: str) -> int:
        """Count documents that hard-failed at any stage.

        :return: Number of distinct ``doc_id`` values with a
            ``hard_failure`` record.
        """
        ...

    # --- Queries ---

    async def get_stage_counts(self, job_id: str) -> list[dict]:
        """Get per-stage counts for progress display.

        :return: List of dicts, each containing:
            ``{data_type, total, succeeded, soft_failed, hard_failed}``.
        """
        ...

    async def get_doc_status(
        self,
        job_id: str,
        doc_id: str,
    ) -> list[dict]:
        """Get all stage records for a specific document.

        :return: List of status records across all stages.
        """
        ...

    async def get_failures(
        self,
        job_id: str,
        data_type: str | None = None,
    ) -> list[dict]:
        """Get all failure records for a job, optionally filtered by stage.

        :param data_type: If provided, only return failures for this
            stage.
        :return: List of failure records (``success=False``).
        """
        ...

    async def get_unique_errors(
        self,
        job_id: str,
        data_type: str | None = None,
    ) -> list[dict]:
        """Get deduplicated error messages with occurrence counts.

        Groups identical errors so operators see aggregated counts
        rather than thousands of identical records.

        :param data_type: If provided, only return errors for this
            stage.
        :return: List of ``{error, count, data_type, first_seen,
            last_seen}``.
        """
        ...
