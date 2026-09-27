"""Contract tests for ``DocumentStoreInterface``, run against the memory store.

Written against the Protocol, not the implementation: add a factory to
``STORE_FACTORIES`` to run the same cases against another store.
"""

import asyncio
from collections.abc import Awaitable

import pytest

from warren.storage.document_store.interface import (
    DocumentAlreadyExistsError,
    DocumentNotFoundError,
)
from warren.storage.document_store.memory import MemoryDocumentStore


def _memory_store(**kwargs):
    kwargs.setdefault("collection_name", "things")
    return MemoryDocumentStore(**kwargs)


STORE_FACTORIES = [_memory_store]


def _result(result_id: str, part_idx: int) -> dict:
    """A results-store row: unique on (doc_id, job_id, part_idx)."""
    return {"result_id": result_id, "doc_id": "d1", "job_id": "j", "part_idx": part_idx}


async def _rejected(call: Awaitable) -> bool:
    """Whether awaiting ``call`` raises ``DocumentAlreadyExistsError``."""
    try:
        await call
    except DocumentAlreadyExistsError:
        return True
    return False


@pytest.mark.parametrize("make", STORE_FACTORIES)
def test_insert_then_get_roundtrips(make) -> None:
    async def scenario() -> tuple[str, dict]:
        store = make()
        doc_id = await store.insert({"doc_id": "d1", "v": 1})
        return doc_id, await store.get_document("d1")

    assert asyncio.run(scenario()) == ("d1", {"doc_id": "d1", "v": 1})


@pytest.mark.parametrize("make", STORE_FACTORIES)
def test_insert_generates_an_id_when_missing(make) -> None:
    async def scenario() -> bool:
        store = make()
        doc_id = await store.insert({"v": 1})
        return (await store.get_document(doc_id))["doc_id"] == doc_id

    assert asyncio.run(scenario())


@pytest.mark.parametrize("make", STORE_FACTORIES)
def test_duplicate_insert_is_rejected(make) -> None:
    async def scenario() -> None:
        store = make()
        await store.insert({"doc_id": "d1"})
        await store.insert({"doc_id": "d1"})

    with pytest.raises(DocumentAlreadyExistsError):
        asyncio.run(scenario())


@pytest.mark.parametrize("make", STORE_FACTORIES)
def test_overwrite_replaces_by_doc_id(make) -> None:
    async def scenario() -> dict:
        store = make()
        await store.insert({"doc_id": "d1", "v": 1, "stale": True})
        await store.insert({"doc_id": "d1", "v": 2}, overwrite_existing=True)
        return await store.get_document("d1")

    assert asyncio.run(scenario()) == {"doc_id": "d1", "v": 2}


@pytest.mark.parametrize("make", STORE_FACTORIES)
def test_overwrite_replaces_by_first_unique_index(make) -> None:
    async def scenario() -> list[dict]:
        store = make(
            doc_id_field="result_id", unique_indexes=[("doc_id", "job_id", "part_idx")]
        )
        await store.insert({"doc_id": "d1", "job_id": "j", "part_idx": 0, "v": 1})
        await store.insert(
            {"doc_id": "d1", "job_id": "j", "part_idx": 0, "v": 2},
            overwrite_existing=True,
        )
        return [d async for d in store.query({"doc_id": "d1"})]

    docs = asyncio.run(scenario())
    assert len(docs) == 1
    assert docs[0]["v"] == 2


@pytest.mark.parametrize("make", STORE_FACTORIES)
def test_unique_index_is_enforced_without_overwrite(make) -> None:
    async def scenario() -> None:
        store = make(
            doc_id_field="result_id", unique_indexes=[("doc_id", "job_id", "part_idx")]
        )
        await store.insert({"doc_id": "d1", "job_id": "j", "part_idx": 0})
        await store.insert({"doc_id": "d1", "job_id": "j", "part_idx": 0})

    with pytest.raises(DocumentAlreadyExistsError):
        asyncio.run(scenario())


@pytest.mark.parametrize("make", STORE_FACTORIES)
def test_overwrite_still_rejects_a_clash_on_another_constraint(make) -> None:
    async def scenario() -> bool:
        store = make(
            doc_id_field="result_id", unique_indexes=[("doc_id", "job_id", "part_idx")]
        )
        await store.insert(_result("r1", 0))
        await store.insert(_result("r2", 1))
        # Replaces r1 by the unique index, but its id is r2's.
        return await _rejected(store.insert(_result("r2", 0), overwrite_existing=True))

    assert asyncio.run(scenario())


@pytest.mark.parametrize("make", STORE_FACTORIES)
def test_overwrite_frees_the_replaced_documents_id(make) -> None:
    async def scenario() -> list[str]:
        store = make(
            doc_id_field="result_id", unique_indexes=[("doc_id", "job_id", "part_idx")]
        )
        await store.insert(_result("r1", 0))
        await store.insert(_result("r9", 0), overwrite_existing=True)
        await store.insert(_result("r1", 1))
        return [d["result_id"] async for d in store.query({})]

    assert asyncio.run(scenario()) == ["r9", "r1"]


@pytest.mark.parametrize("make", STORE_FACTORIES)
def test_delete_frees_the_id_and_the_unique_key(make) -> None:
    async def scenario() -> list[dict]:
        store = make(
            doc_id_field="result_id", unique_indexes=[("doc_id", "job_id", "part_idx")]
        )
        doc = _result("r1", 0)
        await store.insert(doc)
        await store.delete("r1")
        await store.insert(doc)
        return [d async for d in store.query({})]

    assert asyncio.run(scenario()) == [_result("r1", 0)]


@pytest.mark.parametrize("make", STORE_FACTORIES)
def test_update_of_an_indexed_field_moves_its_unique_key(make) -> None:
    async def scenario() -> tuple[str, bool]:
        store = make(
            doc_id_field="result_id", unique_indexes=[("doc_id", "job_id", "part_idx")]
        )
        await store.insert(_result("r1", 0))
        await store.update("r1", {"part_idx": 1})
        # The old key is free again, the new one is taken.
        freed = await store.insert(_result("r2", 0))
        return freed, await _rejected(store.insert(_result("r3", 1)))

    assert asyncio.run(scenario()) == ("r2", True)


@pytest.mark.parametrize("make", STORE_FACTORIES)
def test_update_merges_fields(make) -> None:
    async def scenario() -> dict:
        store = make()
        await store.insert({"doc_id": "d1", "v": 1})
        await store.update("d1", {"v": 2, "extra": True})
        return await store.get_document("d1")

    assert asyncio.run(scenario()) == {"doc_id": "d1", "v": 2, "extra": True}


@pytest.mark.parametrize("make", STORE_FACTORIES)
def test_missing_document_errors(make) -> None:
    async def get() -> None:
        await make().get_document("nope")

    async def update() -> None:
        await make().update("nope", {"v": 1})

    with pytest.raises(DocumentNotFoundError):
        asyncio.run(get())
    with pytest.raises(DocumentNotFoundError):
        asyncio.run(update())


@pytest.mark.parametrize("make", STORE_FACTORIES)
def test_empty_doc_id_is_a_value_error(make) -> None:
    async def scenario() -> None:
        await make().exists("")

    with pytest.raises(ValueError, match="empty"):
        asyncio.run(scenario())


@pytest.mark.parametrize("make", STORE_FACTORIES)
def test_exists_and_delete(make) -> None:
    async def scenario() -> tuple[bool, bool, bool, bool]:
        store = make()
        await store.insert({"doc_id": "d1"})
        return (
            await store.exists("d1"),
            await store.delete("d1"),
            await store.exists("d1"),
            await store.delete("d1"),
        )

    assert asyncio.run(scenario()) == (True, True, False, False)


@pytest.mark.parametrize("make", STORE_FACTORIES)
def test_query_matches_flat_equality_and_empty_matches_all(make) -> None:
    async def scenario() -> tuple[list[str], int]:
        store = make()
        await store.insert({"doc_id": "a", "job_id": "j1"})
        await store.insert({"doc_id": "b", "job_id": "j2"})
        await store.insert({"doc_id": "c", "job_id": "j1", "part_idx": None})
        matched = sorted([d["doc_id"] async for d in store.query({"job_id": "j1"})])
        everything = len([d async for d in store.query({})])
        return matched, everything

    assert asyncio.run(scenario()) == (["a", "c"], 3)


@pytest.mark.parametrize("make", STORE_FACTORIES)
def test_query_none_matches_a_missing_field(make) -> None:
    async def scenario() -> list[str]:
        store = make()
        await store.insert({"doc_id": "a"})
        await store.insert({"doc_id": "b", "job_id": "j"})
        return [d["doc_id"] async for d in store.query({"job_id": None})]

    assert asyncio.run(scenario()) == ["a"]


@pytest.mark.parametrize("make", STORE_FACTORIES)
def test_stored_state_is_isolated_from_callers(make) -> None:
    async def scenario() -> dict:
        store = make()
        original = {"doc_id": "d1", "nested": {"v": 1}}
        await store.insert(original)
        original["nested"]["v"] = 99
        fetched = await store.get_document("d1")
        fetched["nested"]["v"] = 42
        return await store.get_document("d1")

    assert asyncio.run(scenario()) == {"doc_id": "d1", "nested": {"v": 1}}


@pytest.mark.parametrize("make", STORE_FACTORIES)
def test_accessors_and_unique_index_introspection(make) -> None:
    async def scenario() -> tuple:
        store = make(
            doc_id_field="result_id", unique_indexes=[("doc_id", "job_id", "part_idx")]
        )
        return (
            store.get_document_type(),
            store.get_doc_id_field(),
            await store.has_unique_index(("doc_id", "job_id", "part_idx")),
            await store.has_unique_index("result_id"),
            await store.has_unique_index("doc_id"),
        )

    assert asyncio.run(scenario()) == ("things", "result_id", True, True, False)


def test_memory_store_rejects_an_update_that_duplicates_a_unique_key() -> None:
    # Memory-only: MongoDB rejects this too, but with pymongo's DuplicateKeyError.
    async def scenario() -> tuple[bool, dict, bool]:
        store = _memory_store(
            doc_id_field="result_id", unique_indexes=[("doc_id", "job_id", "part_idx")]
        )
        await store.insert(_result("r1", 0))
        await store.insert(_result("r2", 1))
        rejected = await _rejected(store.update("r2", {"part_idx": 0}))
        # r2 keeps its document and its key.
        kept = await store.get_document("r2")
        return rejected, kept, await _rejected(store.insert(_result("r3", 1)))

    assert asyncio.run(scenario()) == (True, _result("r2", 1), True)


def test_memory_store_frees_the_keys_of_a_document_replaced_through_a_shared_id() -> (
    None
):
    # Ids 5 and "5" do not clash (fields compare raw) but share a stored key,
    # so the second insert replaces the first, and its unique value is free.
    async def scenario() -> str:
        store = _memory_store(doc_id_field="rid", unique_indexes=["a"])
        await store.insert({"rid": 5, "a": 1})
        await store.insert({"rid": "5", "a": 2})
        return await store.insert({"rid": "x", "a": 1})

    assert asyncio.run(scenario()) == "x"


def test_memory_store_rejects_query_operators() -> None:
    async def scenario() -> None:
        _ = [d async for d in _memory_store().query({"v": {"$gt": 1}})]

    with pytest.raises(ValueError, match="equality"):
        asyncio.run(scenario())
