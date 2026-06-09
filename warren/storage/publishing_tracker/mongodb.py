"""MongoDB implementation of PublishingTrackerInterface."""

from typing import TYPE_CHECKING

from datetime import UTC, datetime

from basics.base import Base
from pymongo import ASCENDING, AsyncMongoClient

from document_processing.distributed.warren.storage.publishing_tracker.interface import (
    PublishingTrackerInterface,
)


if TYPE_CHECKING:
    from pymongo.asynchronous.collection import AsyncCollection


class MongoDBPublishingTracker(Base, PublishingTrackerInterface):
    """Async MongoDB implementation of PublishingTrackerInterface.

    Stores publishing outcomes in a ``job_publishing_results``
    collection. Success records are keyed by ``(job_id, doc_id)``.
    Failure records may have ``doc_id=None`` if the failure occurred
    before a doc_id could be assigned.

    Requires calling ``await setup()`` after construction to create
    indexes.
    """

    def __init__(
        self,
        client: AsyncMongoClient,
        *,
        database_name: str,
        collection_name: str = "job_publishing_results",
        name: str | None = None,
    ) -> None:
        super().__init__(pybase_logger_name=name)
        self._client = client
        self._database_name = database_name
        self._collection_name = collection_name
        self._collection: AsyncCollection = client[database_name][collection_name]

    async def setup(self) -> None:
        """Create indexes for the job_publishing_results collection."""
        # Unique on (job_id, doc_id) where doc_id is not None
        await self._collection.create_index(
            [
                ("job_id", ASCENDING),
                ("doc_id", ASCENDING),
            ],
            unique=True,
            partialFilterExpression={"doc_id": {"$type": "string"}},
        )
        # Quick success/failure counts
        await self._collection.create_index(
            [
                ("job_id", ASCENDING),
                ("success", ASCENDING),
            ],
        )

    async def record_success(
        self,
        job_id: str,
        doc_id: str,
    ) -> None:
        now = datetime.now(UTC)
        await self._collection.update_one(
            {"job_id": job_id, "doc_id": doc_id},
            {
                "$set": {
                    "time": now,
                    "success": True,
                },
            },
            upsert=True,
        )

    async def record_failure(
        self,
        job_id: str,
        doc_id: str | None,
        source: str,
        error: str,
        stage: str,
    ) -> None:
        now = datetime.now(UTC)
        doc = {
            "job_id": job_id,
            "doc_id": doc_id,
            "time": now,
            "success": False,
            "source": source,
            "error": error,
            "stage": stage,
        }
        # Failures with doc_id=None can't use upsert (no unique key),
        # so always insert.
        if doc_id is None:
            await self._collection.insert_one(doc)
        else:
            await self._collection.update_one(
                {"job_id": job_id, "doc_id": doc_id},
                {"$set": doc},
                upsert=True,
            )

    async def get_results(self, job_id: str) -> list[dict]:
        cursor = self._collection.find(
            {"job_id": job_id},
            projection={"_id": 0},
        )
        return await cursor.to_list()

    async def get_failures(self, job_id: str) -> list[dict]:
        cursor = self._collection.find(
            {"job_id": job_id, "success": False},
            projection={"_id": 0},
        )
        return await cursor.to_list()
