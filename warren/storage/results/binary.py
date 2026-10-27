"""Binary-payload results store.

``BinaryResultsStore`` has the persistence and cache shape of
``DefaultResultsStore``, but it stores ``bytes`` payloads. The
``DefaultResultsStore`` cache holds dict values only.

Persistence: a document store keyed on ``(doc_id, job_id, part_idx)``.
BSON stores ``bytes`` as BSON Binary.

Cache: a ``CacheInterface[bytes]``, for example ``RedisBinaryCache``
under the ``"documents"`` base key. The cache key is
``doc:<doc_id>:<job_id>``, the key that ``CachedDocumentFetcher`` reads
when the caller gives ``job_id``. So a worker that reads through
``GetDocumentFunc`` gets the bytes that an upstream worker stored in
the same job. A new job for the same ``doc_id`` does not read the bytes
of an earlier job. When ``job_id`` is ``None``, the key is
``doc:<doc_id>``.
"""

from typing import Any

from collections.abc import AsyncGenerator
from datetime import UTC, datetime

from basics.base import Base
from basics.logging_utils import summarize_exception_chain

from warren.common import HardFailureException
from warren.storage.cache.interface import (
    CacheInterface,
)
from warren.storage.document_store.interface import (
    DocumentStoreInterface,
)
from warren.storage.documents.fetcher import (
    build_document_cache_key,
)
from warren.storage.results.interface import (
    ResultNotFound,
)


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
    :param result_type: Not used. Give each results store its own
        cache ``base_key``. Two stores that share one cache instance
        overwrite each other's entries.
    :param name: Optional logger name.
    :param cache_ttl_seconds: Expiry for cached payloads.
    """

    def __init__(
        self,
        document_store: DocumentStoreInterface,
        cache: CacheInterface[bytes] | None = None,
        result_type: str | None = None,
        name: str | None = None,
        *,
        cache_ttl_seconds: int = 3600,
    ) -> None:
        super().__init__(pybase_logger_name=name)
        self._cache_ttl_seconds = cache_ttl_seconds
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
            msg = (
                "Document store must have a unique composite index on "
                "(doc_id, job_id, part_idx)."
            )
            raise ValueError(msg)

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

        The cache key is ``doc:<doc_id>:<job_id>``, or ``doc:<doc_id>``
        when ``job_id`` is ``None``. This is the key that
        ``CachedDocumentFetcher`` reads.

        :param payload: The bytes to store.
        :param doc_id: Document ID.
        :param part_idx: Part index. ``0`` for a single-part payload.
        :param job_id: Job ID, or ``None`` if you do not group by job.
        :param metadata: Optional metadata, stored in the field
            ``metadata`` (``DefaultResultsStore`` uses
            ``result_metadata``).
        :param do_cache: Whether to write the payload to the cache.
        :param overwrite_existing: Whether to replace a stored record
            with the same ``(doc_id, job_id, part_idx)``.
        :return: Result ID from the document store.
        :raises DocumentAlreadyExistsError: If ``overwrite_existing`` is
            False and a record for these keys exists.
        """
        doc: dict[str, Any] = {
            "doc_id": doc_id,
            "part_idx": part_idx,
            "job_id": job_id,
            _PAYLOAD_FIELD: payload,
            "metadata": metadata,
            "created_at": datetime.now(UTC),
        }
        result_id = await self._document_store.insert(doc, overwrite_existing)

        if do_cache:
            await self._safe_cache_set(doc_id, payload, job_id=job_id)

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
        cached = await self._safe_cache_get(doc_id, job_id=job_id)
        if cached is not None:
            return cached

        async for found in self._document_store.query(
            {"doc_id": doc_id, "part_idx": part_idx, "job_id": job_id}
        ):
            payload = found.get(_PAYLOAD_FIELD)
            if not isinstance(payload, (bytes, bytearray)):
                msg = (
                    f"Stored record for doc_id={doc_id}, job_id={job_id}, "
                    f"part_idx={part_idx} has no {_PAYLOAD_FIELD!r} bytes."
                )
                raise ResultNotFound(msg)
            payload_bytes = bytes(payload)
            await self._safe_cache_set(doc_id, payload_bytes, job_id=job_id)
            return payload_bytes

        msg = (
            f"No binary result for doc_id={doc_id}, job_id={job_id}, "
            f"part_idx={part_idx}."
        )
        raise ResultNotFound(msg)

    async def query_metadata(
        self,
        *,
        doc_id: str,
        job_id: str | None = None,
    ) -> AsyncGenerator[dict[str, Any], None]:
        """Yield the stored records of a document and job, without their payload.

        Use it to list records without the transfer of the payload bytes.
        """
        async for doc in self._document_store.query(
            {"doc_id": doc_id, "job_id": job_id}
        ):
            # Strip the potentially large payload before handing it up.
            doc.pop(_PAYLOAD_FIELD, None)
            yield doc

    async def _safe_cache_set(
        self,
        doc_id: str,
        payload: bytes,
        *,
        job_id: str | None,
    ) -> None:
        if self._cache is None:
            return
        try:
            await self._cache.set(
                build_document_cache_key(doc_id, job_id),
                payload,
                ttl_seconds=self._cache_ttl_seconds,
            )
        except HardFailureException:
            raise
        except Exception as exc:
            self._log.warning(
                f"BinaryResultsStore cache set failed for doc_id={doc_id}: "
                f"{summarize_exception_chain(exc)}"
            )

    async def _safe_cache_get(
        self,
        doc_id: str,
        *,
        job_id: str | None,
    ) -> bytes | None:
        if self._cache is None:
            return None
        try:
            return await self._cache.get(build_document_cache_key(doc_id, job_id))
        except HardFailureException:
            raise
        except Exception as exc:
            self._log.warning(
                f"BinaryResultsStore cache get failed for doc_id={doc_id}: "
                f"{summarize_exception_chain(exc)}"
            )
            return None
