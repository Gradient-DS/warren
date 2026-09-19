"""Tests for ``MemoryStoreRegistry``: one store per name, shared by all askers."""

import asyncio

from warren.storage.memory_registry import MemoryStoreRegistry


def test_same_name_returns_the_same_store() -> None:
    registry = MemoryStoreRegistry()

    assert registry.document_store("docs") is registry.document_store("docs")
    assert registry.document_store("docs") is not registry.document_store("other")
    assert registry.job_store() is registry.job_store()
    assert registry.job_results_store() is registry.job_results_store()
    assert registry.cache("bytes") is registry.cache("bytes")


def test_results_written_through_one_handle_are_read_through_another() -> None:
    async def scenario() -> dict:
        registry = MemoryStoreRegistry()
        writer = await registry.results_store("parsed")
        reader = await registry.results_store("parsed")
        await writer.store(result={"markdown": "hi"}, doc_id="d1", job_id="j")
        return (await reader.get_result(doc_id="d1", job_id="j")).result

    assert asyncio.run(scenario()) == {"markdown": "hi"}


def test_results_store_supports_multiple_parts() -> None:
    async def scenario() -> list[dict]:
        store = await MemoryStoreRegistry().results_store("chunks")
        for i in range(3):
            await store.store(result={"i": i}, doc_id="d1", part_idx=i, job_id="j")
        parts = [
            r.result
            async for r in store.stream_doc_processing_results(doc_id="d1", job_id="j")
        ]
        return sorted(parts, key=lambda p: p["i"])

    assert asyncio.run(scenario()) == [{"i": 0}, {"i": 1}, {"i": 2}]


def test_two_registries_are_independent() -> None:
    async def scenario() -> bool:
        first, second = MemoryStoreRegistry(), MemoryStoreRegistry()
        await first.document_store("docs").insert({"doc_id": "d1"})
        return await second.document_store("docs").exists("d1")

    assert asyncio.run(scenario()) is False
