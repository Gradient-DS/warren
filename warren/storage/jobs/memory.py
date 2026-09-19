"""In-process ``JobStoreInterface`` implementation."""

import copy
import uuid
from datetime import UTC, datetime

from basics.base import Base

from warren.storage.jobs.interface import JobNotFoundError, JobStoreInterface


class MemoryJobStore(Base, JobStoreInterface):
    """Job definitions and high-level status held in a dict.

    Record shape, job id format and error types match ``MongoDBJobStore``
    so callers cannot tell the two apart. Single-process, gone at exit.
    """

    def __init__(self, *, name: str | None = None) -> None:
        super().__init__(pybase_logger_name=name)
        self._jobs: dict[str, dict] = {}

    async def setup(self) -> None:
        """Nothing to create; present because the Protocol requires it."""

    async def create_job(
        self,
        final_data_type: str,
        parameters: dict | None = None,
        num_documents: int | None = None,
        metadata: dict | None = None,
    ) -> str:
        now = datetime.now(UTC)
        job_id = f"job-{now.strftime('%Y-%m-%d')}-{uuid.uuid4().hex[:8]}"
        self._jobs[job_id] = {
            "job_id": job_id,
            "final_data_type": final_data_type,
            "parameters": copy.deepcopy(parameters) if parameters else {},
            "num_documents": num_documents,
            "status": {
                "completed": False,
                "with_failures": False,
                "completed_at": None,
            },
            "created_at": now,
            "updated_at": now,
            "metadata": copy.deepcopy(metadata),
        }
        return job_id

    async def get_job(self, job_id: str) -> dict:
        return copy.deepcopy(self._job(job_id))

    async def update_num_documents(self, job_id: str, num_documents: int) -> None:
        job = self._job(job_id)
        job["num_documents"] = num_documents
        job["updated_at"] = datetime.now(UTC)

    async def increment_num_documents(self, job_id: str, count: int = 1) -> int:
        job = self._job(job_id)
        job["num_documents"] = (job["num_documents"] or 0) + count
        job["updated_at"] = datetime.now(UTC)
        return job["num_documents"]

    async def update_completion(
        self,
        job_id: str,
        completed: bool,
        with_failures: bool,
    ) -> None:
        job = self._job(job_id)
        now = datetime.now(UTC)
        job["status"] = {
            "completed": completed,
            "with_failures": with_failures,
            "completed_at": now if completed else None,
        }
        job["updated_at"] = now

    async def get_status(self, job_id: str) -> dict:
        return copy.deepcopy(self._job(job_id)["status"])

    async def update_vector_db_state(self, job_id: str, fields: dict) -> None:
        if not fields:
            return
        job = self._job(job_id)
        job.setdefault("vector_db", {}).update(copy.deepcopy(fields))
        job["updated_at"] = datetime.now(UTC)

    def _job(self, job_id: str) -> dict:
        try:
            return self._jobs[job_id]
        except KeyError:
            msg = f"Job '{job_id}' not found"
            raise JobNotFoundError(msg) from None
