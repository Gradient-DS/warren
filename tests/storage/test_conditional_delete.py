import asyncio

import pytest

from tests.storage.scoping_doubles import Collection, MongoClient
from warren.storage.cache.memory import MemoryCache
from warren.storage.cached_document_store import CachedDocumentStore
from warren.storage.document_store.memory import MemoryDocumentStore
from warren.storage.document_store.mongodb import MongoDBDocumentStore


class ConditionalCollection(Collection):
    @staticmethod
    def matches(row: dict, query: dict) -> bool:
        if "$and" in query:
            return all(
                ConditionalCollection.matches(row, part) for part in query["$and"]
            )
        return Collection.matches(row, query)


@pytest.mark.parametrize("backend", ["memory", "mongodb"])
@pytest.mark.parametrize("cached", [False, True])
def test_conditional_delete_preserves_replaced_rows(backend: str, cached: bool) -> None:
    async def run() -> None:
        mongo = MongoClient()
        mongo["control"].collections["retries"] = ConditionalCollection()
        store = (
            MemoryDocumentStore(collection_name="retries", doc_id_field="retry_key")
            if backend == "memory"
            else MongoDBDocumentStore(
                mongo,
                database_name="control",
                collection_name="retries",
                doc_id_field="retry_key",
            )
        )
        if cached:
            store = CachedDocumentStore(store, MemoryCache())
        await store.insert(
            {"retry_key": "key", "generation": "old"}, overwrite_existing=True
        )
        await store.insert(
            {"retry_key": "key", "generation": "new"}, overwrite_existing=True
        )
        assert not await store.delete("key", expected={"generation": "old"})
        assert (await store.get_document("key"))["generation"] == "new"
        assert not await store.delete("absent", expected={"generation": "new"})
        assert await store.delete("key", expected={"generation": "new"})
        assert not await store.exists("key")
        await store.insert({"retry_key": "legacy"})
        assert await store.delete("legacy", expected={"generation": None})

    asyncio.run(run())
