"""Contract tests for ``JobStoreInterface``, run against the memory store."""

import asyncio
import re

import pytest

from warren.storage.jobs.interface import JobNotFoundError
from warren.storage.jobs.memory import MemoryJobStore


STORE_FACTORIES = [MemoryJobStore]


@pytest.mark.parametrize("make", STORE_FACTORIES)
def test_create_job_returns_a_dated_id_and_full_record(make) -> None:
    async def scenario() -> tuple[str, dict]:
        store = make()
        await store.setup()
        job_id = await store.create_job("final", {"p": 1}, 3, {"job_name": "n"})
        return job_id, await store.get_job(job_id)

    job_id, job = asyncio.run(scenario())
    assert re.fullmatch(r"job-\d{4}-\d{2}-\d{2}-[0-9a-f]{8}", job_id)
    assert job["job_id"] == job_id
    assert job["final_data_type"] == "final"
    assert job["parameters"] == {"p": 1}
    assert job["num_documents"] == 3
    assert job["metadata"] == {"job_name": "n"}
    assert job["status"] == {
        "completed": False,
        "with_failures": False,
        "completed_at": None,
    }
    assert job["created_at"] == job["updated_at"]


@pytest.mark.parametrize("make", STORE_FACTORIES)
def test_parameters_default_to_empty_dict(make) -> None:
    async def scenario() -> dict:
        store = make()
        return await store.get_job(await store.create_job("final"))

    job = asyncio.run(scenario())
    assert job["parameters"] == {}
    assert job["num_documents"] is None


@pytest.mark.parametrize("make", STORE_FACTORIES)
def test_update_and_increment_num_documents(make) -> None:
    async def scenario() -> tuple[int, int]:
        store = make()
        job_id = await store.create_job("final", num_documents=0)
        await store.update_num_documents(job_id, 3)
        incremented = await store.increment_num_documents(job_id, 2)
        return incremented, (await store.get_job(job_id))["num_documents"]

    assert asyncio.run(scenario()) == (5, 5)


@pytest.mark.parametrize("make", STORE_FACTORIES)
def test_update_completion_sets_status_and_timestamp(make) -> None:
    async def scenario() -> tuple[dict, dict]:
        store = make()
        job_id = await store.create_job("final")
        await store.update_completion(job_id, completed=True, with_failures=True)
        done = await store.get_status(job_id)
        await store.update_completion(job_id, completed=False, with_failures=False)
        return done, await store.get_status(job_id)

    done, reopened = asyncio.run(scenario())
    assert done["completed"] is True
    assert done["with_failures"] is True
    assert done["completed_at"] is not None
    assert reopened == {
        "completed": False,
        "with_failures": False,
        "completed_at": None,
    }


@pytest.mark.parametrize("make", STORE_FACTORIES)
def test_update_vector_db_state_merges_and_ignores_empty(make) -> None:
    async def scenario() -> dict:
        store = make()
        job_id = await store.create_job("final")
        await store.update_vector_db_state(job_id, {})
        await store.update_vector_db_state(job_id, {"state": "building"})
        await store.update_vector_db_state(job_id, {"uri": "x"})
        return await store.get_job(job_id)

    assert asyncio.run(scenario())["vector_db"] == {"state": "building", "uri": "x"}


@pytest.mark.parametrize("make", STORE_FACTORIES)
@pytest.mark.parametrize(
    "call",
    [
        lambda s: s.get_job("nope"),
        lambda s: s.get_status("nope"),
        lambda s: s.update_num_documents("nope", 1),
        lambda s: s.increment_num_documents("nope"),
        lambda s: s.update_completion("nope", True, False),
        lambda s: s.update_vector_db_state("nope", {"a": 1}),
    ],
)
def test_unknown_job_raises(make, call) -> None:
    with pytest.raises(JobNotFoundError):
        asyncio.run(call(make()))


@pytest.mark.parametrize("make", STORE_FACTORIES)
def test_returned_job_is_a_copy(make) -> None:
    async def scenario() -> dict:
        store = make()
        job_id = await store.create_job("final", {"p": 1})
        (await store.get_job(job_id))["parameters"]["p"] = 99
        return await store.get_job(job_id)

    assert asyncio.run(scenario())["parameters"] == {"p": 1}
