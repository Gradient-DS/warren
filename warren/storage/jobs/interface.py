"""
Storage protocol for job definitions and job-level status.

The ``jobs`` collection stores what a job *is* and its high-level state
(completed, with_failures). Per-document processing results are tracked
separately by ``JobResultsStoreInterface``.
"""

from typing import Protocol

from warren.exceptions import WarrenError
from warren.storage.exceptions import (
    ResourceNotFoundError,
)


class JobNotFoundError(ResourceNotFoundError):
    pass


class JobCreationFailedError(WarrenError):
    """The store failed to create the job."""

    pass


class JobStoreInterface(Protocol):
    """Storage protocol for job definitions and job-level status.

    **Transient-error contract:** transient backend failures (connection
    blips, failover) are raised as ``TransientStoreError`` (see
    ``storage/exceptions.py``); permanent failures propagate as other
    exceptions. Callers that cannot tolerate a dropped operation — notably
    the ``JobStatusWorker`` observer, where a lost completion update
    strands a job — should retry on ``TransientStoreError``.
    """

    async def setup(self) -> None:
        """Create indexes and prepare the store for use."""
        ...

    async def create_job(
        self,
        final_data_type: str,
        parameters: dict | None = None,
        num_documents: int | None = None,
        metadata: dict | None = None,
    ) -> str:
        """Create a new job entry with a store-generated ID.

        :param final_data_type: The data_type that marks a document as
            fully processed (e.g., ``"embedded_document"``).
        :param parameters: Processing parameters for workers.
        :param num_documents: Total document count, if known upfront.
            Can be set later via ``update_num_documents``.
        :param metadata: Optional application-defined metadata.

        :return: The generated job ID.
        """
        ...

    async def get_job(self, job_id: str) -> dict:
        """Get the full job document.

        :return: Job document as dict with all fields.
        :raises JobNotFoundError: If job_id does not exist.
        """
        ...

    async def update_num_documents(
        self,
        job_id: str,
        num_documents: int,
    ) -> None:
        """Set or update the number of documents for a job.

        A value of 0 means "no documents were successfully submitted"
        — different from ``None``, which means "not yet known."

        :raises JobNotFoundError: If job_id does not exist.
        """
        ...

    async def increment_num_documents(
        self,
        job_id: str,
        count: int = 1,
    ) -> int:
        """Atomically increment num_documents and return the new value.

        Used when documents are published in batches or streamed, and
        the total is not known upfront.

        :raises JobNotFoundError: If job_id does not exist.
        :return: New num_documents value after increment.
        """
        ...

    async def update_completion(
        self,
        job_id: str,
        completed: bool,
        with_failures: bool,
    ) -> None:
        """Atomically update the job's completion status.

        Always sets both fields together to prevent inconsistent state.

        :raises JobNotFoundError: If job_id does not exist.
        """
        ...

    async def get_status(self, job_id: str) -> dict:
        """Get the job's current status.

        :return: Dict with ``{completed, with_failures, completed_at}``.
        :raises JobNotFoundError: If job_id does not exist.
        """
        ...

    async def update_vector_db_state(
        self,
        job_id: str,
        fields: dict,
    ) -> None:
        """Partially update the job's ``vector_db`` sub-document.

        Used by the ``VectorDBProvisioningWorker`` to record state
        transitions for the per-job vector-DB lifecycle (creation,
        backup, teardown). Idempotent — repeated calls with the same
        fields are safe.

        :param job_id: Job to update.
        :param fields: Mapping of ``vector_db.<key>`` values to set.
            E.g. ``{"creation_status": "ready", "url": "..."}``.

        :raises JobNotFoundError: If job_id does not exist.
        """
        ...
