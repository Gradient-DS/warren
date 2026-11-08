import asyncio
import copy
from unittest.mock import AsyncMock, Mock

import pytest

from warren.jobs.status.job_status_worker import JobStatusWorker
from warren.retry_management.retry_worker import RetryWorker
from warren.workers.messages import validate_message


@pytest.mark.parametrize("value", [{"$ne": "other"}, [], 1, True, "job\nforged", "job\rforged"])
@pytest.mark.parametrize("field", ["job_id", "doc_id"])
@pytest.mark.parametrize("wrapped", [False, True])
def test_invalid_identity_never_reaches_stores(value: object, field: str, wrapped: bool) -> None:
    message = {"data_type": "parsed", "job_id": "job", "data": {"doc_id": "doc"}}
    if field == "job_id":
        message[field] = value
    else:
        message["data"][field] = value
    if wrapped:
        message = {"data_type": "soft-failure", "job_id": "job", "data": message}
    jobs, results, retries = AsyncMock(), AsyncMock(), Mock()
    retries.get_doc_id_field.return_value = "retry_key"
    retries.insert = AsyncMock()
    status = JobStatusWorker("status", job_store=jobs, job_results_store=results)
    retry = RetryWorker("retry", retry_store=retries, republish_publisher=AsyncMock())

    async def run() -> None:
        with pytest.raises(ValueError, match="must be a string"):
            await status.process(message)
        if wrapped:
            with pytest.raises(ValueError, match="must be a string"):
                await retry(message)

    asyncio.run(run())
    jobs.get_job.assert_not_awaited()
    results.record_success.assert_not_awaited()
    results.record_soft_failure.assert_not_awaited()
    retries.insert.assert_not_awaited()


def test_missing_optional_identity_is_valid() -> None:
    validate_message({"data": {}})


@pytest.mark.parametrize("field", ["job_id", "doc_id", "both"])
@pytest.mark.parametrize("wrapped", [False, True])
def test_explicit_null_identity_is_valid(field: str, wrapped: bool) -> None:
    message = {"data_type": "input", "data": {}}
    if field in ("job_id", "both"):
        message["job_id"] = None
    if field in ("doc_id", "both"):
        message["data"]["doc_id"] = None
    if wrapped:
        message = {"data_type": "soft-failure", "job_id": None, "data": message}
    before = copy.deepcopy(message)
    validate_message(message)
    assert message == before
