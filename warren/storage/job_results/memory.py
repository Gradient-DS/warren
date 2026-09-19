"""In-process ``JobResultsStoreInterface`` implementation."""

import copy
from datetime import UTC, datetime

from basics.base import Base

from warren.storage.job_results.interface import JobResultsStoreInterface


_RecordKey = tuple[str, str, str]  # (job_id, data_type, doc_id)


class MemoryJobResultsStore(Base, JobResultsStoreInterface):
    """Per-document, per-stage status ledger held in a dict.

    One record per ``(job_id, data_type, doc_id)``, the same identity the
    Mongo store enforces with its unique index. A record's failure state
    is carried by which fields are present: ``soft_failures`` (a list of
    reasons) or ``hard_failure`` (one error string). Recording an outcome
    removes the fields that belong to the other outcomes, exactly as the
    Mongo store does with ``$unset``.
    """

    def __init__(self, *, name: str | None = None) -> None:
        super().__init__(pybase_logger_name=name)
        self._records: dict[_RecordKey, dict] = {}

    async def setup(self) -> None:
        """Nothing to create; present because the Protocol requires it."""

    async def record_success(
        self,
        job_id: str,
        data_type: str,
        doc_id: str,
        origin_type: str,
        origin_name: str,
    ) -> None:
        record = self._record(job_id, data_type, doc_id)
        record.update(
            time=datetime.now(UTC),
            success=True,
            origin_type=origin_type,
            origin_name=origin_name,
        )
        for field in (
            "retry_count",
            "soft_failures",
            "hard_failure",
            "consumer_type",
            "consumer_name",
        ):
            record.pop(field, None)

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
        record = self._record(job_id, data_type, doc_id)
        record.update(
            time=datetime.now(UTC),
            success=False,
            origin_type=origin_type,
            origin_name=origin_name,
            consumer_type=consumer_type,
            consumer_name=consumer_name,
            retry_count=retry_count,
        )
        record.setdefault("soft_failures", []).append(error_reason)
        record.pop("hard_failure", None)

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
        record = self._record(job_id, data_type, doc_id)
        record.update(
            time=datetime.now(UTC),
            success=False,
            origin_type=origin_type,
            origin_name=origin_name,
            consumer_type=consumer_type,
            consumer_name=consumer_name,
            hard_failure=error,
        )
        record.pop("retry_count", None)
        record.pop("soft_failures", None)

    async def count_completed_docs(self, job_id: str, final_data_type: str) -> int:
        return sum(
            1
            for r in self._for_job(job_id)
            if r["data_type"] == final_data_type and r["success"]
        )

    async def count_hard_failed_docs(self, job_id: str) -> int:
        return len({r["doc_id"] for r in self._for_job(job_id) if "hard_failure" in r})

    async def get_stage_counts(self, job_id: str) -> list[dict]:
        stages: dict[str, dict] = {}
        for r in self._for_job(job_id):
            stage = stages.setdefault(
                r["data_type"],
                {
                    "data_type": r["data_type"],
                    "total": 0,
                    "succeeded": 0,
                    "soft_failed": 0,
                    "hard_failed": 0,
                },
            )
            stage["total"] += 1
            if r["success"]:
                stage["succeeded"] += 1
            elif "soft_failures" in r:
                stage["soft_failed"] += 1
            if "hard_failure" in r:
                stage["hard_failed"] += 1
        return list(stages.values())

    async def get_doc_status(self, job_id: str, doc_id: str) -> list[dict]:
        return [self._public(r) for r in self._for_job(job_id) if r["doc_id"] == doc_id]

    async def get_failures(
        self,
        job_id: str,
        data_type: str | None = None,
    ) -> list[dict]:
        return [self._public(r) for r in self._failures(job_id, data_type)]

    async def get_unique_errors(
        self,
        job_id: str,
        data_type: str | None = None,
    ) -> list[dict]:
        groups: dict[tuple[str | None, str], dict] = {}
        for r in self._failures(job_id, data_type):
            soft = r.get("soft_failures") or [None]
            error = r["hard_failure"] if "hard_failure" in r else soft[-1]
            group = groups.setdefault(
                (error, r["data_type"]),
                {
                    "error": error,
                    "data_type": r["data_type"],
                    "count": 0,
                    "first_seen": r["time"],
                    "last_seen": r["time"],
                },
            )
            group["count"] += 1
            group["first_seen"] = min(group["first_seen"], r["time"])
            group["last_seen"] = max(group["last_seen"], r["time"])
        return sorted(groups.values(), key=lambda g: g["count"], reverse=True)

    def _record(self, job_id: str, data_type: str, doc_id: str) -> dict:
        return self._records.setdefault(
            (job_id, data_type, doc_id),
            {"job_id": job_id, "data_type": data_type, "doc_id": doc_id},
        )

    def _for_job(self, job_id: str) -> list[dict]:
        return [r for (jid, _, _), r in self._records.items() if jid == job_id]

    def _failures(self, job_id: str, data_type: str | None) -> list[dict]:
        return [
            r
            for r in self._for_job(job_id)
            if not r["success"] and (data_type is None or r["data_type"] == data_type)
        ]

    def _public(self, record: dict) -> dict:
        """A caller-owned copy without ``job_id``, as the Mongo projection returns."""
        public = copy.deepcopy(record)
        del public["job_id"]
        return public
