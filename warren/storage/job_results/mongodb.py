"""MongoDB implementation of JobResultsStoreInterface."""

from typing import TYPE_CHECKING

from datetime import UTC, datetime

from basics.base import Base
from pymongo import ASCENDING, AsyncMongoClient

from warren.storage.job_results.interface import (
    JobResultsStoreInterface,
)
from warren.storage.mongo_errors import (
    classify_transient_methods,
)


if TYPE_CHECKING:
    from pymongo.asynchronous.collection import AsyncCollection


@classify_transient_methods
class MongoDBJobResultsStore(Base, JobResultsStoreInterface):
    """Async MongoDB implementation of JobResultsStoreInterface.

    Stores per-document processing results in a ``job_results``
    collection. Each record is keyed by ``(job_id, data_type, doc_id)``
    via a unique composite index, enabling atomic upsert operations.

    Requires calling ``await setup()`` after construction to create
    indexes.
    """

    def __init__(
        self,
        client: AsyncMongoClient,
        *,
        database_name: str,
        collection_name: str = "job_results",
        name: str | None = None,
    ) -> None:
        super().__init__(pybase_logger_name=name)
        self._client = client
        self._database_name = database_name
        self._collection_name = collection_name
        self._collection: AsyncCollection = client[database_name][collection_name]

    async def setup(self) -> None:
        """Create indexes for the job_results collection."""
        # Primary key — one record per (job_id, data_type, doc_id)
        await self._collection.create_index(
            [
                ("job_id", ASCENDING),
                ("data_type", ASCENDING),
                ("doc_id", ASCENDING),
            ],
            unique=True,
        )
        # Progress bar queries
        await self._collection.create_index(
            [
                ("job_id", ASCENDING),
                ("data_type", ASCENDING),
                ("success", ASCENDING),
            ],
        )
        # Job-level failure queries
        await self._collection.create_index(
            [
                ("job_id", ASCENDING),
                ("success", ASCENDING),
            ],
        )

    # --- Recording results ---

    async def record_success(
        self,
        job_id: str,
        data_type: str,
        doc_id: str,
        origin_type: str,
        origin_name: str,
    ) -> None:
        now = datetime.now(UTC)
        await self._collection.update_one(
            {
                "job_id": job_id,
                "data_type": data_type,
                "doc_id": doc_id,
            },
            {
                "$set": {
                    "time": now,
                    "success": True,
                    "origin_type": origin_type,
                    "origin_name": origin_name,
                },
                # Clear any previous failure fields
                "$unset": {
                    "retry_count": "",
                    "soft_failures": "",
                    "hard_failure": "",
                    "consumer_type": "",
                    "consumer_name": "",
                },
            },
            upsert=True,
        )

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
        now = datetime.now(UTC)
        await self._collection.update_one(
            {
                "job_id": job_id,
                "data_type": data_type,
                "doc_id": doc_id,
            },
            {
                "$set": {
                    "time": now,
                    "success": False,
                    "origin_type": origin_type,
                    "origin_name": origin_name,
                    "consumer_type": consumer_type,
                    "consumer_name": consumer_name,
                    "retry_count": retry_count,
                },
                "$push": {
                    "soft_failures": error_reason,
                },
                # Clear hard_failure if present (shouldn't happen, but
                # defensive)
                "$unset": {
                    "hard_failure": "",
                },
            },
            upsert=True,
        )

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
        now = datetime.now(UTC)
        await self._collection.update_one(
            {
                "job_id": job_id,
                "data_type": data_type,
                "doc_id": doc_id,
            },
            {
                "$set": {
                    "time": now,
                    "success": False,
                    "origin_type": origin_type,
                    "origin_name": origin_name,
                    "consumer_type": consumer_type,
                    "consumer_name": consumer_name,
                    "hard_failure": error,
                },
                # Clear soft-failure fields if present
                "$unset": {
                    "retry_count": "",
                    "soft_failures": "",
                },
            },
            upsert=True,
        )

    # --- Counting ---

    async def count_completed_docs(
        self,
        job_id: str,
        final_data_type: str,
    ) -> int:
        return await self._collection.count_documents(
            {
                "job_id": job_id,
                "data_type": final_data_type,
                "success": True,
            }
        )

    async def count_hard_failed_docs(self, job_id: str) -> int:
        pipeline = [
            {
                "$match": {
                    "job_id": job_id,
                    "hard_failure": {"$exists": True},
                },
            },
            {
                "$group": {
                    "_id": "$doc_id",
                },
            },
            {
                "$count": "total",
            },
        ]
        results = await self._run_pipeline(pipeline)
        if not results:
            return 0
        return results[0]["total"]

    # --- Queries ---

    async def get_stage_counts(self, job_id: str) -> list[dict]:
        pipeline = [
            {"$match": {"job_id": job_id}},
            {
                "$group": {
                    "_id": "$data_type",
                    "total": {"$sum": 1},
                    "succeeded": {
                        "$sum": {"$cond": ["$success", 1, 0]},
                    },
                    "soft_failed": {
                        "$sum": {
                            "$cond": [
                                {
                                    "$and": [
                                        {"$not": "$success"},
                                        {
                                            "$gt": [
                                                {"$type": "$soft_failures"},
                                                "missing",
                                            ]
                                        },
                                    ],
                                },
                                1,
                                0,
                            ],
                        },
                    },
                    "hard_failed": {
                        "$sum": {
                            "$cond": [
                                {"$gt": [{"$type": "$hard_failure"}, "missing"]},
                                1,
                                0,
                            ],
                        },
                    },
                },
            },
            {
                "$project": {
                    "_id": 0,
                    "data_type": "$_id",
                    "total": 1,
                    "succeeded": 1,
                    "soft_failed": 1,
                    "hard_failed": 1,
                },
            },
        ]
        return await self._run_pipeline(pipeline)

    async def get_doc_status(
        self,
        job_id: str,
        doc_id: str,
    ) -> list[dict]:
        cursor = self._collection.find(
            {"job_id": job_id, "doc_id": doc_id},
            projection={"_id": 0, "job_id": 0},
        )
        return await cursor.to_list()

    async def get_failures(
        self,
        job_id: str,
        data_type: str | None = None,
    ) -> list[dict]:
        query: dict = {"job_id": job_id, "success": False}
        if data_type is not None:
            query["data_type"] = data_type
        cursor = self._collection.find(
            query,
            projection={"_id": 0, "job_id": 0},
        )
        return await cursor.to_list()

    async def get_unique_errors(
        self,
        job_id: str,
        data_type: str | None = None,
    ) -> list[dict]:
        match_stage: dict = {"job_id": job_id, "success": False}
        if data_type is not None:
            match_stage["data_type"] = data_type

        pipeline = [
            {"$match": match_stage},
            {
                "$project": {
                    "data_type": 1,
                    "time": 1,
                    "error": {
                        "$cond": [
                            {"$gt": [{"$type": "$hard_failure"}, "missing"]},
                            "$hard_failure",
                            {"$arrayElemAt": ["$soft_failures", -1]},
                        ],
                    },
                },
            },
            {
                "$group": {
                    "_id": {"error": "$error", "data_type": "$data_type"},
                    "count": {"$sum": 1},
                    "first_seen": {"$min": "$time"},
                    "last_seen": {"$max": "$time"},
                },
            },
            {
                "$project": {
                    "_id": 0,
                    "error": "$_id.error",
                    "data_type": "$_id.data_type",
                    "count": 1,
                    "first_seen": 1,
                    "last_seen": 1,
                },
            },
            {"$sort": {"count": -1}},
        ]
        return await self._run_pipeline(pipeline)

    async def _run_pipeline(self, pipeline: list[dict]) -> list[dict]:
        """Execute an aggregation pipeline and return the results."""
        cursor = await self._collection.aggregate(pipeline)
        return await cursor.to_list()
