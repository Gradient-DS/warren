"""
Audit worker for the multi-exchange example — a **sync** worker.

Subscribes to the ``events`` topic exchange (binding ``#`` — every event) and
appends one record per observed message to an ``audit_events`` MongoDB
collection. It is a passive sidecar: it consumes from a different exchange than
the main pipeline and publishes nothing (``publish=[]``).

This worker is a ``SyncProcessingWorkerBase`` to demonstrate mixing **sync and
async** workers in one pipeline. The framework detects the plain (non-async)
``__call__`` and runs it in a thread pool, so the event loop stays free for
RabbitMQ heartbeats. A sync worker must not touch the asyncio loop, so it uses a
**sync** ``pymongo`` client (not the async client the runner hands to async
workers). Sync DB drivers, ``requests``, file I/O, and CPU-bound work all work
naturally here.
"""

from typing import Any

from pymongo import MongoClient
from pymongo.collection import Collection

from warren.workers.workers import SyncProcessingWorkerBase


class AuditWorker(SyncProcessingWorkerBase):
    """Records every event message it receives into an append-only collection."""

    def __init__(self, worker_name: str, *, collection: Collection) -> None:
        super().__init__(worker_name)
        self._collection = collection

    def __call__(self, message: dict) -> dict | None:
        record: dict[str, Any] = {
            "data_type": message.get("data_type"),
            "doc_id": message.get("data", {}).get("doc_id"),
            "job_id": message.get("job_id"),
            "origin": message.get("origin"),
        }
        # Synchronous insert — runs in the framework's thread pool.
        self._collection.insert_one(record)
        self._log.info(f"Audited {record['data_type']} for doc {record['doc_id']}")
        return None


def create_audit_collection(host: str, port: int, database: str) -> Collection:
    """Build a sync pymongo collection for the audit worker."""
    return MongoClient(host, port)[database]["audit_events"]
