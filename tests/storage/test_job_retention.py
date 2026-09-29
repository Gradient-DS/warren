import asyncio
from datetime import UTC, datetime

import pytest

from tests.storage.job_store_doubles import MongoClient
from warren.storage.job_results.mongodb import MongoDBJobResultsStore
from warren.storage.jobs.mongodb import MongoDBJobStore
from warren.storage.publishing_tracker.mongodb import MongoDBPublishingTracker


@pytest.mark.parametrize(
    ("store_type", "collection", "field"),
    [
        (MongoDBJobStore, "jobs", "status.completed_at"),
        (MongoDBJobResultsStore, "job_results", "time"),
        (MongoDBPublishingTracker, "job_publishing_results", "time"),
    ],
)
@pytest.mark.parametrize("ttl", [None, 0, 3600])
def test_retention_index_specs(
    store_type: type, collection: str, field: str, ttl: int | None
) -> None:
    client = MongoClient()
    store = store_type(client, database_name="test", job_records_ttl_seconds=ttl)
    asyncio.run(store.setup())
    specs = list(client["test"][collection].indexes.values())
    ttls = [spec for spec in specs if "expireAfterSeconds" in spec]
    expected = [] if ttl is None else [{"key": [(field, 1)], "expireAfterSeconds": ttl}]
    assert ttls == expected


def test_job_max_age_is_independent_of_completion_ttl() -> None:
    client = MongoClient()
    store = MongoDBJobStore(
        client, database_name="test", job_records_max_age_seconds=7200
    )
    asyncio.run(store.setup())
    assert client["test"]["jobs"].indexes["created_at_1"] == {
        "key": [("created_at", 1)],
        "expireAfterSeconds": 7200,
    }
    assert "status.completed_at_1" not in client["test"]["jobs"].indexes


@pytest.mark.parametrize(
    "previous_options",
    [{}, {"expireAfterSeconds": 10}, {"expireAfterSeconds": 20, "sparse": True}],
)
@pytest.mark.parametrize(
    ("store_type", "collection", "field", "setting"),
    [
        (MongoDBJobStore, "jobs", "status.completed_at", "job_records_ttl_seconds"),
        (MongoDBJobStore, "jobs", "created_at", "job_records_max_age_seconds"),
        (MongoDBJobResultsStore, "job_results", "time", "job_records_ttl_seconds"),
        (
            MongoDBPublishingTracker,
            "job_publishing_results",
            "time",
            "job_records_ttl_seconds",
        ),
    ],
)
def test_setup_replaces_conflicting_index_options(
    store_type: type, collection: str, field: str, setting: str, previous_options: dict
) -> None:
    client = MongoClient()
    coll = client["test"][collection]
    coll.indexes["old_index"] = {"key": [(field, 1)], **previous_options}
    store = store_type(client, database_name="test", **{setting: 20})

    async def scenario() -> None:
        await store.setup()
        await store.setup()

    asyncio.run(scenario())
    assert coll.dropped == ["old_index"]
    assert coll.indexes[f"{field}_1"] == {
        "key": [(field, 1)],
        "expireAfterSeconds": 20,
    }


def test_disabling_retention_removes_ttl_but_preserves_regular_indexes() -> None:
    client = MongoClient()
    coll = client["test"]["jobs"]
    coll.indexes["old_ttl"] = {
        "key": [("status.completed_at", 1)],
        "expireAfterSeconds": 20,
    }
    coll.indexes["created_at_1"] = {"key": [("created_at", 1)]}
    asyncio.run(MongoDBJobStore(client, database_name="test").setup())
    assert coll.dropped == ["old_ttl"]
    assert coll.indexes["created_at_1"] == {"key": [("created_at", 1)]}


def test_setup_preserves_an_equivalent_index_with_a_custom_name() -> None:
    client = MongoClient()
    coll = client["test"]["jobs"]
    spec = {"v": 2, "key": [("status.completed_at", 1)], "expireAfterSeconds": 20}
    coll.indexes["existing_ttl"] = spec
    store = MongoDBJobStore(client, database_name="test", job_records_ttl_seconds=20)
    asyncio.run(store.setup())
    assert coll.dropped == []
    assert coll.indexes["existing_ttl"] == spec


def test_all_result_writes_refresh_the_retention_datetime() -> None:
    client = MongoClient()

    async def scenario() -> None:
        results = MongoDBJobResultsStore(client, database_name="test")
        await results.record_success("j", "stage", "a", "worker", "w")
        await results.record_soft_failure(
            "j", "stage", "a", "worker", "w", "worker", "w2", 1, "retry"
        )
        await results.record_hard_failure(
            "j", "stage", "a", "worker", "w", "worker", "w2", "failed"
        )
        tracker = MongoDBPublishingTracker(client, database_name="test")
        await tracker.record_success("j", "a")
        await tracker.record_failure("j", "a", "source", "failed", "publish")
        await tracker.record_failure("j", None, "source", "failed", "load")
        await tracker.record_failure("j", None, "source", "failed", "register")

    before = datetime.now(UTC)
    asyncio.run(scenario())
    after = datetime.now(UTC)
    results = client["test"]["job_results"].writes
    publishing = client["test"]["job_publishing_results"].writes
    assert len(results) == 3
    assert len(publishing) == 4
    assert [row["doc_id"] for row in publishing] == ["a", "a", None, None]
    for row in results + publishing:
        assert isinstance(row["time"], datetime)
        assert before <= row["time"] <= after
