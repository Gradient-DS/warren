"""
Generic cached document store — write-through/read-through wrapper.

Conforms to ``DocumentStoreInterface`` via structural typing and composes
an injected ``DocumentStoreInterface`` (backing store) with a
``CacheInterface[Dict]`` (cache layer). Interchangeable with uncached stores.

Not specific to any use case (retry, results, etc.) — usable anywhere
persistence with fast cached retrieval is needed.
"""

from collections.abc import AsyncGenerator

from basics.base import Base
from basics.logging_utils import summarize_exception_chain

from warren.storage.cache.interface import (
    CacheInterface,
)
from warren.storage.document_store.interface import (
    DocumentStoreInterface,
    IndexSpec,
)


class CachedDocumentStore(Base):
    """Document store with read-through/write-through cache.

    Wraps any ``DocumentStoreInterface`` with a ``CacheInterface[Dict]`` for
    fast reads. All writes go to both store and cache (write-through).
    Reads check cache first, falling back to store on miss (read-through).

    Cache failures are logged as warnings but never propagate — the
    underlying store is the source of truth.

    :param store: Persistent document store.
    :param cache: Cache for fast reads.
    :param cache_ttl_seconds: TTL for cached entries. None uses
        cache default.
    """

    def __init__(
        self,
        store: DocumentStoreInterface,
        cache: CacheInterface[dict],
        *,
        cache_ttl_seconds: int | None = None,
        name: str | None = None,
    ) -> None:
        super().__init__(pybase_logger_name=name)
        self._store = store
        self._cache = cache
        self._cache_ttl = cache_ttl_seconds

    async def insert(
        self,
        doc: dict,
        overwrite_existing: bool = False,
    ) -> str:
        """Insert document into store and cache.

        :param doc: Document to insert.
        :param overwrite_existing: Allow overwrite if True.

        :return: Document ID.

        :raises DocumentAlreadyExistsError: If overwrite_existing is False
            and a document with the same ID already exists.
        """
        doc_id = await self._store.insert(doc, overwrite_existing)
        await self._safe_cache_set(doc_id, doc)
        return doc_id

    async def update(
        self,
        doc_id: str,
        updates: dict,
    ) -> None:
        """Update document in store and invalidate cache.

        :param doc_id: Document ID.
        :param updates: Dictionary with updates.

        :raises DocumentNotFoundError: If document does not exist.
        """
        await self._store.update(doc_id, updates)
        await self._safe_cache_delete(doc_id)

    async def delete(
        self,
        doc_id: str,
    ) -> bool:
        """Delete document from store and cache.

        :param doc_id: Document ID.

        :return: True if document existed and was deleted.
        """
        deleted = await self._store.delete(doc_id)
        await self._safe_cache_delete(doc_id)
        return deleted

    async def exists(
        self,
        doc_id: str,
    ) -> bool:
        """Check if document exists, trying cache first.

        :param doc_id: Document ID.

        :return: True if document exists.
        """
        try:
            if await self._cache.exists(doc_id):
                return True
        except Exception as e:
            self._log.warning(
                f"Cache exists check failed for doc_id={doc_id}: "
                f"{summarize_exception_chain(e)}"
            )
        return await self._store.exists(doc_id)

    async def get_document(
        self,
        doc_id: str,
    ) -> dict:
        """Get document, trying cache first (read-through).

        :param doc_id: Document ID.

        :return: Document dict.

        :raises DocumentNotFoundError: If document does not exist.
        """
        try:
            cached = await self._cache.get(doc_id)
            if cached is not None:
                return cached
        except Exception as e:
            self._log.warning(
                f"Cache get failed for doc_id={doc_id}: {summarize_exception_chain(e)}"
            )

        doc = await self._store.get_document(doc_id)
        await self._safe_cache_set(doc_id, doc)
        return doc

    def query(
        self,
        params: dict,
    ) -> AsyncGenerator[dict, None]:
        """Query document store (cache is bypassed).

        :param params: Query parameters.

        :return: Async generator of documents found.
        """
        return self._store.query(params)

    def get_document_type(self) -> str:
        """Return document type from underlying store."""
        return self._store.get_document_type()

    def get_doc_id_field(self) -> str:
        """Return doc ID field name from underlying store."""
        return self._store.get_doc_id_field()

    async def has_unique_index(self, index_spec: IndexSpec) -> bool:
        """Check unique index on underlying store."""
        return await self._store.has_unique_index(index_spec)

    async def _safe_cache_set(self, key: str, value: dict) -> None:
        """Set cache entry, logging failures as warnings."""
        try:
            await self._cache.set(key, value, self._cache_ttl)
        except Exception as e:
            self._log.warning(
                f"Cache set failed for key={key}: {summarize_exception_chain(e)}"
            )

    async def _safe_cache_delete(self, key: str) -> None:
        """Delete cache entry, logging failures as warnings."""
        try:
            await self._cache.delete(key)
        except Exception as e:
            self._log.warning(
                f"Cache delete failed for key={key}: {summarize_exception_chain(e)}"
            )
