"""Contract tests for ``JobResultsStoreInterface``, run against the memory store."""

import asyncio

import pytest

from warren.storage.job_results.memory import MemoryJobResultsStore


STORE_FACTORIES = [MemoryJobResultsStore]

_ORIGIN = ("parser", "p-0")
_CONSUMER = ("chunker", "c-0")


async def _seed(store) -> None:
    await store.setup()
    await store.record_success("j", "text", "d1", *_ORIGIN)
    await store.record_success("j", "chunks", "d1", *_ORIGIN)
    await store.record_soft_failure("j", "text", "d2", *_ORIGIN, *_CONSUMER, 1, "boom")
    await store.record_soft_failure(
        "j", "text", "d2", *_ORIGIN, *_CONSUMER, 2, "boom again"
    )
    await store.record_hard_failure("j", "text", "d3", *_ORIGIN, *_CONSUMER, "dead")
    await store.record_hard_failure("j", "chunks", "d3", *_ORIGIN, *_CONSUMER, "dead")
    await store.record_success("other", "text", "d1", *_ORIGIN)


@pytest.mark.parametrize("make", STORE_FACTORIES)
def test_counts(make) -> None:
    async def scenario() -> tuple[int, int, int]:
        store = make()
        await _seed(store)
        return (
            await store.count_completed_docs("j", "text"),
            await store.count_completed_docs("j", "missing"),
            await store.count_hard_failed_docs("j"),
        )

    # d3 hard-failed at two stages and still counts as one document
    assert asyncio.run(scenario()) == (1, 0, 1)


@pytest.mark.parametrize("make", STORE_FACTORIES)
def test_stage_counts_separate_soft_from_hard(make) -> None:
    async def scenario() -> list[dict]:
        store = make()
        await _seed(store)
        return sorted(await store.get_stage_counts("j"), key=lambda s: s["data_type"])

    assert asyncio.run(scenario()) == [
        {
            "data_type": "chunks",
            "total": 2,
            "succeeded": 1,
            "soft_failed": 0,
            "hard_failed": 1,
        },
        {
            "data_type": "text",
            "total": 3,
            "succeeded": 1,
            "soft_failed": 1,
            "hard_failed": 1,
        },
    ]


@pytest.mark.parametrize("make", STORE_FACTORIES)
def test_success_clears_earlier_failures(make) -> None:
    async def scenario() -> list[dict]:
        store = make()
        await store.record_soft_failure(
            "j", "text", "d1", *_ORIGIN, *_CONSUMER, 1, "boom"
        )
        await store.record_success("j", "text", "d1", *_ORIGIN)
        return await store.get_doc_status("j", "d1")

    (record,) = asyncio.run(scenario())
    assert record["success"] is True
    assert "soft_failures" not in record
    assert "retry_count" not in record
    assert "consumer_type" not in record
    assert "job_id" not in record


@pytest.mark.parametrize("make", STORE_FACTORIES)
def test_hard_failure_clears_soft_failure_fields(make) -> None:
    async def scenario() -> list[dict]:
        store = make()
        await store.record_soft_failure(
            "j", "text", "d1", *_ORIGIN, *_CONSUMER, 1, "boom"
        )
        await store.record_hard_failure("j", "text", "d1", *_ORIGIN, *_CONSUMER, "dead")
        return await store.get_doc_status("j", "d1")

    (record,) = asyncio.run(scenario())
    assert record["hard_failure"] == "dead"
    assert "soft_failures" not in record
    assert "retry_count" not in record


@pytest.mark.parametrize("make", STORE_FACTORIES)
def test_get_failures_filters_by_stage(make) -> None:
    async def scenario() -> tuple[int, int]:
        store = make()
        await _seed(store)
        return len(await store.get_failures("j")), len(
            await store.get_failures("j", "chunks")
        )

    assert asyncio.run(scenario()) == (3, 1)


@pytest.mark.parametrize("make", STORE_FACTORIES)
def test_unique_errors_group_and_sort_by_count(make) -> None:
    async def scenario() -> list[dict]:
        store = make()
        await _seed(store)
        return await store.get_unique_errors("j", "text")

    errors = asyncio.run(scenario())
    assert sorted(e["error"] for e in errors) == ["boom again", "dead"]
    assert all(e["count"] == 1 and e["data_type"] == "text" for e in errors)
    assert all(e["first_seen"] <= e["last_seen"] for e in errors)


@pytest.mark.parametrize("make", STORE_FACTORIES)
def test_unique_errors_are_sorted_by_count_descending(make) -> None:
    async def scenario() -> list[tuple[str, int]]:
        store = make()
        for doc in ("a", "b", "c"):
            await store.record_hard_failure(
                "j", "text", doc, *_ORIGIN, *_CONSUMER, "common"
            )
        await store.record_hard_failure("j", "text", "z", *_ORIGIN, *_CONSUMER, "rare")
        return [(e["error"], e["count"]) for e in await store.get_unique_errors("j")]

    assert asyncio.run(scenario()) == [("common", 3), ("rare", 1)]
