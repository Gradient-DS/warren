"""Binary-payload results store.

Closes the 04-03-2026 architectural loop: ``DefaultResultsStore``'s
cache is dict-only (``RedisDictCache`` + ``json.dumps``), which forced
byte-producing stages (originally: PDFs registered via publish, more
recently: HTML bytes produced by ``WebFetchWorker``) onto a separate
``DocumentStore`` + ``CachedDocumentFetcher`` track. That parallel
track was a workaround — the 04-03-2026 dev note called out
"RedisBinaryCache exists but cannot be used (type mismatch)."

``BinaryResultsStore`` is the fix: it mirrors ``DefaultResultsStore``'s
persistence + cache shape, but uses ``RedisBinaryCache`` so byte
payloads can live in the ResultsStore track natively. Worker-produced
byte results now share the same storage semantics as worker-produced
dict results.

Persistence: MongoDB document store keyed on ``(doc_id, job_id,
part_idx)``. BSON handles ``bytes`` natively as BSON Binary.

Cache: ``RedisBinaryCache`` under the ``"documents"`` base key, using
the doc-id-scoped key ``doc:<doc_id>`` — identical to what
``CachedDocumentFetcher`` reads from. This lets ``ParserWorker`` (which
reads via ``GetDocumentFunc``) transparently hit the cache populated
by an upstream byte-producing stage. No fetcher signature change
required.

**Trade-off (by design, for v1):** cache is doc-id-scoped, persistence
is (doc_id, job_id, part_idx)-scoped. For a single-job run this is
fine. If multiple concurrent jobs process the same ``doc_id``, cache
reads aren't job-isolated (they'll see whichever job wrote last).
Widening ``GetDocumentFunc`` to include ``job_id`` would close this;
deferred until operational pressure appears.
"""

from collections.abc import AsyncGenerator
from typing import Any

from basics.base import Base
from basics.logging_utils import summarize_exception_chain

from document_processing.distributed.warren.storage.cache.interface import (
    CacheInterface,
)
from document_processing.distributed.warren.storage.document_store.interface import (
    DocumentStoreInterface,
)
from document_processing.distributed.warren.storage.documents.fetcher import (
    build_document_cache_key,
)
from document_processing.distributed.warren.storage.results.interface import (
    ResultNotFound,
)
from document_processing.distributed.warren.storage.utils import current_time_str


_PAYLOAD_FIELD: str = "payload"


class BinaryResultsStore(Base):
    """Results store for byte payloads.

    Focused API — intentionally does not implement
    ``ResultsStoreInterface`` because that Protocol's ``result: Dict``
    contract doesn't fit byte payloads (and broadening it to ``Any``
    would ripple through every reader).

    :param document_store: MongoDB-backed store for persistence.
        Must have a unique composite index on
        ``(doc_id, job_id, part_idx)`` (validated in ``setup()``).
    :param cache: Optional ``CacheInterface[bytes]`` for fast reads.
        When absent, every read hits MongoDB.
    :param result_type: Type identifier; defaults to the document
        store's collection name.
    :param name: Optional logger name.
    """

    def __init__(
        self,
        document_store: DocumentStoreInterface,
        cache: CacheInterface[bytes] | None = None,
        result_type: str | None = None,
        name: str | None = None,
    ) -> None:
        super().__init__(pybase_logger_name=name)
        self._document_store: DocumentStoreInterface = document_store
        self._cache: CacheInterface[bytes] | None = cache
        self._result_type: str = (
            result_type
            if result_type is not None
            else document_store.get_document_type()
        )

    async def setup(self) -> None:
        """Validate the document store's unique composite index."""
        if not await self._document_store.has_unique_index(
            ("doc_id", "job_id", "part_idx")
        ):
            raise ValueError(
                "Document store must have a unique composite index on "
                "(doc_id, job_id, part_idx)."
            )

    async def store_bytes(
        self,
        payload: bytes,
        *,
        doc_id: str,
        part_idx: int = 0,
        job_id: str | None = None,
        metadata: dict[str, Any] | None = None,
        do_cache: bool = True,
        overwrite_existing: bool = True,
    ) -> str:
        """Persist ``payload`` and populate the cache.

        The cache write uses the *shared* document cache key
        (``doc:<doc_id>``) so downstream consumers using
        ``GetDocumentFunc`` hit it transparently. See module docstring
        for the trade-off this implies.

        :return: Result ID from the document store.
        """
        doc: dict[str, Any] = {
            "doc_id": doc_id,
            "part_idx": part_idx,
            "job_id": job_id,
            _PAYLOAD_FIELD: payload,
            "metadata": metadata,
            "created_at": current_time_str(),
        }
        result_id = await self._document_store.insert(doc, overwrite_existing)

        if do_cache:
            await self._safe_cache_set(doc_id, payload)

        return result_id

    async def get_bytes(
        self,
        doc_id: str,
        *,
        part_idx: int = 0,
        job_id: str | None = None,
    ) -> bytes:
        """Read bytes by business keys. Cache-first, then MongoDB.

        :raises ResultNotFound: If no record exists for the given keys.
        """
        cached = await self._safe_cache_get(doc_id)
        if cached is not None:
            return cached

        async for found in self._document_store.query(
            {"doc_id": doc_id, "part_idx": part_idx, "job_id": job_id}
        ):
            payload = found.get(_PAYLOAD_FIELD)
            if not isinstance(payload, (bytes, bytearray)):
                raise ResultNotFound(
                    f"Stored record for doc_id={doc_id}, job_id={job_id}, "
                    f"part_idx={part_idx} has no {_PAYLOAD_FIELD!r} bytes."
                )
            payload_bytes = bytes(payload)
            await self._safe_cache_set(doc_id, payload_bytes)
            return payload_bytes

        raise ResultNotFound(
            f"No binary result for doc_id={doc_id}, job_id={job_id}, "
            f"part_idx={part_idx}."
        )

    async def query_metadata(
        self,
        *,
        doc_id: str,
        job_id: str | None = None,
    ) -> AsyncGenerator[dict[str, Any], None]:
        """Yield stored records (without the payload bytes).

        Useful for auditing / listing what's been scraped for a given
        doc/job without pulling HTML bodies back through the wire.
        """
        async for doc in self._document_store.query(
            {"doc_id": doc_id, "job_id": job_id}
        ):
            # Strip the potentially large payload before handing it up.
            doc.pop(_PAYLOAD_FIELD, None)
            yield doc

    async def _safe_cache_set(self, doc_id: str, payload: bytes) -> None:
        if self._cache is None:
            return
        try:
            await self._cache.set(build_document_cache_key(doc_id), payload)
        except Exception as exc:
            self._log.warning(
                f"BinaryResultsStore cache set failed for doc_id={doc_id}: "
                f"{summarize_exception_chain(exc)}"
            )

    async def _safe_cache_get(self, doc_id: str) -> bytes | None:
        if self._cache is None:
            return None
        try:
            return await self._cache.get(build_document_cache_key(doc_id))
        except Exception as exc:
            self._log.warning(
                f"BinaryResultsStore cache get failed for doc_id={doc_id}: "
                f"{summarize_exception_chain(exc)}"
            )
            return None
