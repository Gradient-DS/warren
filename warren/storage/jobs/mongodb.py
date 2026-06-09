"""MongoDB implementation of JobStoreInterface."""

from typing import TYPE_CHECKING

import uuid
from datetime import UTC, datetime

from basics.base import Base
from pymongo import AsyncMongoClient
from pymongo.errors import DuplicateKeyError

from warren.storage.jobs.interface import (
    JobCreationFailedError,
    JobNotFoundError,
    JobStoreInterface,
)
from warren.storage.mongo_errors import (
    classify_transient_methods,
)


if TYPE_CHECKING:
    from pymongo.asynchronous.collection import AsyncCollection


@classify_transient_methods
class MongoDBJobStore(Base, JobStoreInterface):
    """Async MongoDB implementation of JobStoreInterface.

    Stores job definitions and high-level status in a ``jobs``
    collection. Uses ``job_id`` as the unique key.

    Requires calling ``await setup()`` after construction to create
    indexes.
    """

    def __init__(
        self,
        client: AsyncMongoClient,
        *,
        database_name: str,
        collection_name: str = "jobs",
        name: str | None = None,
    ) -> None:
        super().__init__(pybase_logger_name=name)
        self._client = client
        self._database_name = database_name
        self._collection_name = collection_name
        self._collection: AsyncCollection = client[database_name][collection_name]

    async def setup(self) -> None:
        """Create unique index on job_id."""
        await self._collection.create_index("job_id", unique=True)

    async def create_job(
        self,
        final_data_type: str,
        parameters: dict | None = None,
        num_documents: int | None = None,
        metadata: dict | None = None,
    ) -> str:
        job_id = self._generate_job_id()
        now = datetime.now(UTC)
        doc = {
            "job_id": job_id,
            "final_data_type": final_data_type,
            "parameters": parameters or {},
            "num_documents": num_documents,
            "status": {
                "completed": False,
                "with_failures": False,
                "completed_at": None,
            },
            "created_at": now,
            "updated_at": now,
            "metadata": metadata,
        }
        try:
            await self._collection.insert_one(doc)
        except DuplicateKeyError as e:
            msg = f"Failed to create job '{job_id}': duplicate key"
            raise JobCreationFailedError(msg) from e
        return job_id

    async def get_job(self, job_id: str) -> dict:
        doc = await self._collection.find_one({"job_id": job_id})
        if doc is None:
            msg = f"Job '{job_id}' not found"
            raise JobNotFoundError(msg)
        doc.pop("_id", None)
        return doc

    async def update_num_documents(
        self,
        job_id: str,
        num_documents: int,
    ) -> None:
        result = await self._collection.update_one(
            {"job_id": job_id},
            {
                "$set": {
                    "num_documents": num_documents,
                    "updated_at": datetime.now(UTC),
                },
            },
        )
        if result.matched_count == 0:
            msg = f"Job '{job_id}' not found"
            raise JobNotFoundError(msg)

    async def increment_num_documents(
        self,
        job_id: str,
        count: int = 1,
    ) -> int:
        result = await self._collection.find_one_and_update(
            {"job_id": job_id},
            {
                "$inc": {"num_documents": count},
                "$set": {"updated_at": datetime.now(UTC)},
            },
            return_document=True,
        )
        if result is None:
            msg = f"Job '{job_id}' not found"
            raise JobNotFoundError(msg)
        return result["num_documents"]

    async def update_completion(
        self,
        job_id: str,
        completed: bool,
        with_failures: bool,
    ) -> None:
        now = datetime.now(UTC)
        update: dict = {
            "$set": {
                "status.completed": completed,
                "status.with_failures": with_failures,
                "status.completed_at": now if completed else None,
                "updated_at": now,
            },
        }
        result = await self._collection.update_one(
            {"job_id": job_id},
            update,
        )
        if result.matched_count == 0:
            msg = f"Job '{job_id}' not found"
            raise JobNotFoundError(msg)

    async def get_status(self, job_id: str) -> dict:
        doc = await self._collection.find_one(
            {"job_id": job_id},
            projection={"status": 1, "_id": 0},
        )
        if doc is None:
            msg = f"Job '{job_id}' not found"
            raise JobNotFoundError(msg)
        return doc["status"]

    async def update_vector_db_state(
        self,
        job_id: str,
        fields: dict,
    ) -> None:
        if not fields:
            return
        now = datetime.now(UTC)
        set_fields: dict = {f"vector_db.{key}": value for key, value in fields.items()}
        set_fields["updated_at"] = now
        result = await self._collection.update_one(
            {"job_id": job_id},
            {"$set": set_fields},
        )
        if result.matched_count == 0:
            msg = f"Job '{job_id}' not found"
            raise JobNotFoundError(msg)

    def _generate_job_id(self) -> str:
        """Generate a unique job ID with date prefix.

        Format: ``job-{YYYY-MM-DD}-{8hex}``
        """
        date_str = datetime.now(UTC).strftime("%Y-%m-%d")
        short_uuid = uuid.uuid4().hex[:8]
        return f"job-{date_str}-{short_uuid}"
