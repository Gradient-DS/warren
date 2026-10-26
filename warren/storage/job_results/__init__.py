"""Job results: one status record for each document and stage of a job."""

from warren.storage.job_results.interface import (
    JobResultsStoreInterface,
)
from warren.storage.job_results.memory import MemoryJobResultsStore
from warren.storage.job_results.mongodb import MongoDBJobResultsStore


__all__ = [
    "JobResultsStoreInterface",
    "MemoryJobResultsStore",
    "MongoDBJobResultsStore",
]
