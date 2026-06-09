"""
Framework-level worker that observes all messages on the fanout exchange
and records per-document processing results. Detects job completion and
signals it on the exchange.

Message classification:

- ``data_type == "job-completed"`` → skip (own output, self-consumption guard)
- ``data_type == "soft-failure"`` → extract inner message, record soft failure
- ``data_type == "hard-failure"`` → extract inner message, record hard failure
- Any other with valid ``job_id`` → record success
"""

from basics.logging_utils import summarize_exception_chain

from document_processing.distributed.warren.storage.exceptions import (
    TransientStoreError,
)
from document_processing.distributed.warren.storage.job_results.interface import (
    JobResultsStoreInterface,
)
from document_processing.distributed.warren.storage.jobs.interface import (
    JobStoreInterface,
)
from document_processing.distributed.warren.storage.retry import (
    run_with_transient_retry,
)
from document_processing.distributed.warren.workers.workers import (
    FilteringWorkerBase,
)


class JobStatusWorker(FilteringWorkerBase):
    """Observes messages on the fanout exchange and records results.

    Sits alongside processing workers on the same exchange. Classifies
    each message (success, soft-failure, hard-failure) and records the
    outcome via ``JobResultsStoreInterface``. After each update, checks
    whether the job is complete and returns a ``"job-completed"``
    message if so.

    :param worker_name: Unique instance identifier.
    :param job_store: Store for job definitions and completion status.
    :param job_results_store: Store for per-document processing results.
    :param worker_type: Optional override for the worker type
        (default: ``"job_status_worker"``).
    """

    def __init__(
        self,
        worker_name: str,
        *,
        job_store: JobStoreInterface,
        job_results_store: JobResultsStoreInterface,
        worker_type: str | None = None,
        store_retry_attempts: int = 5,
        store_retry_base_delay: float = 1.0,
    ) -> None:
        super().__init__(worker_name, worker_type=worker_type)
        self._job_store = job_store
        self._job_results_store = job_results_store
        self._store_retry_attempts = store_retry_attempts
        self._store_retry_base_delay = store_retry_base_delay

    def should_process(self, message: dict) -> bool:
        """Process everything with a job_id, except own completion
        signals and job publication triggers.

        ``"job-completed"`` is this worker's own output
        (self-consumption guard). ``"job"`` is a publication trigger
        for ``JobPublicationWorker``, not a document-level processing
        result. Publishing via the exchange is optional —
        ``JobDocumentsPublisher`` can be used directly without RMQ —
        so this worker must not depend on or react to it.
        """
        data_type = message.get("data_type")
        if data_type in ("job-completed", "job"):
            return False
        return message.get("job_id") is not None

    async def process(self, message: dict) -> dict | None:
        """Classify the message, record the result, and check completion.

        Store interactions run under a bounded local retry on
        ``TransientStoreError`` (``run_with_transient_retry``): this
        worker is a pure observer, so a transient store blip that escaped
        would drop the observation — losing a completion increment and
        potentially stranding the job. Permanent errors and malformed
        messages are not ``TransientStoreError``, so they propagate
        immediately (fail loud). A sustained outage exhausts the retries;
        the observation is then dropped (acked) rather than re-fanned to
        the bus.
        """
        data_type = message.get("data_type")
        job_id = message["job_id"]

        if not data_type:
            self._log.error(f"Message for job {job_id} has no data_type, skipping")
            return None

        try:
            return await run_with_transient_retry(
                lambda: self._record_and_check(data_type, job_id, message),
                attempts=self._store_retry_attempts,
                base_delay=self._store_retry_base_delay,
                on_retry=lambda attempt, error: self._log.warning(
                    f"Job {job_id}: transient store error "
                    f"(attempt {attempt}/{self._store_retry_attempts}), "
                    f"retrying: {summarize_exception_chain(error)}"
                ),
            )
        except TransientStoreError as e:
            self._log.error(
                f"Job {job_id}: transient store error persisted after "
                f"{self._store_retry_attempts} attempts; dropping observation "
                f"(likely a store outage): {summarize_exception_chain(e)}"
            )
            return None

    async def _record_and_check(
        self,
        data_type: str,
        job_id: str,
        message: dict,
    ) -> dict | None:
        """Record the observation for ``message`` and check job completion.

        Store operations are idempotent (deterministic upserts keyed on
        ``(job_id, data_type, doc_id)``; ``update_completion`` is a
        ``$set``), so re-running the whole record+check after a partial
        transient failure is safe. ``message["data"]`` stays unguarded: a
        missing field is malformed, not transient, so it fails loud
        immediately (not retried).
        """
        if data_type == "soft-failure":
            await self._handle_soft_failure(job_id, message["data"], message)
        elif data_type == "hard-failure":
            await self._handle_hard_failure(job_id, message["data"], message)
        else:
            await self._handle_success(job_id, message)

        return await self._check_completion(job_id)

    async def _handle_success(
        self,
        job_id: str,
        message: dict,
    ) -> None:
        origin = message.get("origin", {})
        doc_id = message.get("data", {}).get("doc_id")

        if doc_id is None:
            self._log.warning(
                f"Job {job_id}: success message with data_type="
                f"'{message.get('data_type')}' has no doc_id, skipping"
            )
            return

        await self._job_results_store.record_success(
            job_id=job_id,
            data_type=message["data_type"],
            doc_id=doc_id,
            origin_type=origin.get("type", ""),
            origin_name=origin.get("name", ""),
        )

    async def _handle_soft_failure(
        self,
        job_id: str,
        inner: dict,
        envelope: dict,
    ) -> None:
        """Record a soft failure from the inner message and envelope."""
        inner_origin = inner.get("origin", {})
        consumer = envelope.get("origin", {})
        retry = inner.get("retry", {})
        doc_id = inner.get("data", {}).get("doc_id")

        if doc_id is None:
            self._log.warning(
                f"Job {job_id}: soft-failure for data_type="
                f"'{inner.get('data_type')}' has no doc_id, skipping"
            )
            return

        await self._job_results_store.record_soft_failure(
            job_id=job_id,
            data_type=inner.get("data_type", ""),
            doc_id=doc_id,
            origin_type=inner_origin.get("type", ""),
            origin_name=inner_origin.get("name", ""),
            consumer_type=consumer.get("type", ""),
            consumer_name=consumer.get("name", ""),
            retry_count=retry.get("count", 0),
            error_reason=retry.get("reason", ""),
        )

    async def _handle_hard_failure(
        self,
        job_id: str,
        inner: dict,
        envelope: dict,
    ) -> None:
        """Record a hard failure from the inner message and envelope."""
        inner_origin = inner.get("origin", {})
        consumer = envelope.get("origin", {})
        doc_id = inner.get("data", {}).get("doc_id")

        if doc_id is None:
            self._log.warning(
                f"Job {job_id}: hard-failure for data_type="
                f"'{inner.get('data_type')}' has no doc_id, skipping"
            )
            return

        await self._job_results_store.record_hard_failure(
            job_id=job_id,
            data_type=inner.get("data_type", ""),
            doc_id=doc_id,
            origin_type=inner_origin.get("type", ""),
            origin_name=inner_origin.get("name", ""),
            consumer_type=consumer.get("type", ""),
            consumer_name=consumer.get("name", ""),
            error=envelope.get("error", ""),
        )

    async def _check_completion(self, job_id: str) -> dict | None:
        """Check if a job is complete and return a completion signal if so."""
        job = await self._job_store.get_job(job_id)

        status = job.get("status", {})
        if status.get("completed"):
            return None

        num_documents = job.get("num_documents")
        if num_documents is None:
            return None

        final_data_type = job["final_data_type"]
        completed_count = await self._job_results_store.count_completed_docs(
            job_id, final_data_type
        )
        hard_failed_count = await self._job_results_store.count_hard_failed_docs(job_id)
        total_resolved = completed_count + hard_failed_count

        if total_resolved < num_documents:
            return None

        with_failures = hard_failed_count > 0
        await self._job_store.update_completion(
            job_id=job_id,
            completed=True,
            with_failures=with_failures,
        )

        self._log.info(
            f"Job {job_id} completed: {completed_count} succeeded, "
            f"{hard_failed_count} failed"
        )

        # Forward job parameters so downstream consumers (e.g. the
        # vector-DB lifecycle worker, which filters on
        # job_parameters.vector_db.create) can opt in via should_process
        # without round-tripping back to the JobStore.
        job_parameters = job.get("parameters") or {}

        return {
            "data_type": "job-completed",
            "job_id": job_id,
            "origin": {
                "type": self.type,
                "name": self.name,
            },
            "data": {
                "completed_count": completed_count,
                "hard_failed_count": hard_failed_count,
                "with_failures": with_failures,
            },
            "job_parameters": job_parameters,
        }
