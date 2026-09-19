"""Per-process registry of in-process stores, keyed by collection name."""

from warren.storage.cache.memory import MemoryCache
from warren.storage.document_store.interface import IndexSpec
from warren.storage.document_store.memory import MemoryDocumentStore
from warren.storage.job_results.memory import MemoryJobResultsStore
from warren.storage.jobs.memory import MemoryJobStore
from warren.storage.results.default import DefaultResultsStore


class MemoryStoreRegistry:
    """Hands out in-process stores so co-hosted runners share state.

    A database shares state by name: two workers that open the collection
    ``"chunks"`` see the same documents. In one process that role is played
    by this registry: every method is get-or-create on a name, so the
    worker that writes ``"chunks"`` and the worker that reads it get the
    same store. Build one per process (or one per test, for a fresh world)
    and pass it to whatever constructs the runners.

    The first call for a name fixes that store's configuration; later calls
    return the existing store and ignore their keyword arguments.
    """

    def __init__(self) -> None:
        self._document_stores: dict[str, MemoryDocumentStore] = {}
        self._results_stores: dict[str, DefaultResultsStore] = {}
        self._caches: dict[str, MemoryCache] = {}
        self._job_store: MemoryJobStore | None = None
        self._job_results_store: MemoryJobResultsStore | None = None

    def document_store(
        self,
        collection_name: str,
        *,
        doc_id_field: str = "doc_id",
        unique_indexes: list[IndexSpec] | None = None,
    ) -> MemoryDocumentStore:
        store = self._document_stores.get(collection_name)
        if store is None:
            store = MemoryDocumentStore(
                collection_name=collection_name,
                doc_id_field=doc_id_field,
                unique_indexes=unique_indexes,
            )
            self._document_stores[collection_name] = store
        return store

    async def results_store(self, collection_name: str) -> DefaultResultsStore:
        """A real ``DefaultResultsStore`` over a memory document store.

        Same ``doc_id_field`` and unique index ``create_default_results_store``
        uses on MongoDB, and no cache: reads already come from memory.
        """
        store = self._results_stores.get(collection_name)
        if store is None:
            store = DefaultResultsStore(
                document_store=self.document_store(
                    collection_name,
                    doc_id_field="result_id",
                    unique_indexes=[("doc_id", "job_id", "part_idx")],
                ),
                cache=None,
                result_type=collection_name,
            )
            await store.setup()
            self._results_stores[collection_name] = store
        return store

    def job_store(self) -> MemoryJobStore:
        if self._job_store is None:
            self._job_store = MemoryJobStore()
        return self._job_store

    def job_results_store(self) -> MemoryJobResultsStore:
        if self._job_results_store is None:
            self._job_results_store = MemoryJobResultsStore()
        return self._job_results_store

    def cache(
        self,
        name: str,
        *,
        default_ttl_seconds: int | None = None,
    ) -> MemoryCache:
        cache = self._caches.get(name)
        if cache is None:
            cache = MemoryCache(default_ttl_seconds=default_ttl_seconds)
            self._caches[name] = cache
        return cache
