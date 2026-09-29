import asyncio

import pytest

from tests.storage.job_store_doubles import JobCollection, MongoClient
from warren.storage.jobs.interface import JobNotFoundError
from warren.storage.jobs.mongodb import MongoDBJobStore


@pytest.mark.parametrize("missing_timestamp", [False, True])
def test_completion_is_conditional_and_preserves_the_first_status(
    *, missing_timestamp: bool
) -> None:
    client = MongoClient()
    collection = JobCollection()
    client["test"].collections["jobs"] = collection
    store = MongoDBJobStore(client, database_name="test")

    async def scenario() -> None:
        job_id = await store.create_job("final")
        if missing_timestamp:
            del collection.rows[job_id]["status"]["completed_at"]
        assert await store.update_completion(job_id, completed=True, with_failures=True)
        done = await store.get_job(job_id)
        assert done["status"]["completed"] is True
        assert done["status"]["with_failures"] is True
        assert done["status"]["completed_at"] is not None
        assert not await store.update_completion(
            job_id, completed=True, with_failures=False
        )
        assert not await store.update_completion(
            job_id, completed=False, with_failures=False
        )
        assert await store.get_job(job_id) == done
        assert (
            collection.update_filters
            == [{"job_id": job_id, "status.completed_at": None}] * 3
        )

    asyncio.run(scenario())


def test_completion_raises_for_a_missing_job() -> None:
    client = MongoClient()
    client["test"].collections["jobs"] = JobCollection()
    store = MongoDBJobStore(client, database_name="test")
    with pytest.raises(JobNotFoundError):
        asyncio.run(
            store.update_completion("missing", completed=True, with_failures=False)
        )
