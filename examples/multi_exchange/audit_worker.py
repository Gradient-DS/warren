"""
Audit worker for the multi-exchange example.

Subscribes to the **events** topic exchange (binding ``#`` — every event) and
appends one record per observed message to an ``audit_events`` MongoDB
collection. It is a passive sidecar: it consumes from a different exchange than
the main pipeline and publishes nothing (``publish=[]``).
"""

from typing import Any

from pymongo import AsyncMongoClient

from warren.workers.workers import FilteringWorkerBase


class AuditWorker(FilteringWorkerBase):
    """Records every event message it receives into an append-only collection."""

    def __init__(
        self,
        worker_name: str,
        *,
        mongo_client: AsyncMongoClient,
        database_name: str,
        collection_name: str = "audit_events",
    ) -> None:
        super().__init__(worker_name)
        self._collection = mongo_client[database_name][collection_name]

    def should_process(self, message: dict) -> bool:
        # The topic '#' binding already delivers only event messages; audit all.
        return True

    async def process(self, message: dict) -> dict | None:
        record: dict[str, Any] = {
            "data_type": message.get("data_type"),
            "doc_id": message.get("data", {}).get("doc_id"),
            "job_id": message.get("job_id"),
            "origin": message.get("origin"),
        }
        await self._collection.insert_one(record)
        self._log.info(f"Audited {record['data_type']} for doc {record['doc_id']}")
        return None
