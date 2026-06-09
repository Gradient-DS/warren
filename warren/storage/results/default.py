from collections.abc import AsyncGenerator

from basics.base import Base
from basics.logging_utils import summarize_exception_chain

from warren.storage.cache.interface import (
    CacheInterface,
)
from warren.storage.document_store.interface import (
    DocumentStoreInterface,
)
from warren.storage.results.interface import (
    DocumentProcessingResultsNotFound,
    ResultDoc,
    ResultNotFound,
    ResultsStoreInterface,
)
from warren.storage.utils import current_time_str
from warren.workers.messages import (
    build_message_key,
    build_message_key_prefix,
)


class DefaultResultsStore(Base, ResultsStoreInterface):
    """
    Default async implementation of ResultsStoreInterface.

    Stores document processing results using an injected DocumentStoreInterface
    for persistence and an optional CacheInterface for caching.

    The document store must have a unique composite index on (doc_id, job_id, part_idx)
    to ensure uniqueness and enable efficient queries. The ``setup()`` method validates
    this requirement and raises ValueError if the index is missing.

    Requires calling ``await setup()`` after construction. The factory function
    handles this automatically.
    """

    def __init__(
        self,
        document_store: DocumentStoreInterface,
        cache: CacheInterface[dict] | None = None,
        result_type: str | None = None,
        name: str | None = None,
    ) -> None:
        """
        Initialize the results store.

        :param document_store: Storage backend for persistence. Must have a unique
            composite index on (doc_id, job_id, part_idx).
        :param cache: Optional cache for read-through/write-through caching.
        :param result_type: Type identifier for cache keys. If None, uses
            document_store.get_document_type().
        :param name: Optional name for logging purposes.
        """
        super().__init__(pybase_logger_name=name)

        self._document_store = document_store
        self._cache = cache
        self._result_type = (
            result_type
            if result_type is not None
            else document_store.get_document_type()
        )
        self._doc_id_field = document_store.get_doc_id_field()

    async def setup(self) -> None:
        """
        Validate that the document store has the required unique index.

        Must be called after construction before using the store.
        The factory function handles this automatically.

        :raises ValueError: If document store lacks required unique index.
        """
        if not await self._document_store.has_unique_index(
            ("doc_id", "job_id", "part_idx")
        ):
            msg = (
                "Document store must have a unique composite index on "
                "(doc_id, job_id, part_idx). Without this index, duplicate "
                "results may be created."
            )
            raise ValueError(msg)

    async def store(
        self,
        result: dict,
        doc_id: str,
        part_idx: int | None = None,
        job_id: str | None = None,
        result_metadata: dict | None = None,
        do_cache: bool = True,
        overwrite_existing: bool = True,
    ) -> str:
        """
        Store a processing result.

        :param result: The processing result to store.
        :param doc_id: Document ID.
        :param part_idx: Part index (None or 0 for single-part results).
        :param job_id: Job ID (None if not using job grouping).
        :param result_metadata: Optional metadata about how the result was produced.
        :param do_cache: Whether to cache the result.
        :param overwrite_existing: Whether to overwrite existing result.

        :return: Document ID from the document store.
        """
        normalized_part_idx = 0 if part_idx is None else part_idx

        doc = {
            "doc_id": doc_id,
            "part_idx": normalized_part_idx,
            "job_id": job_id,
            "result": result,
            "result_metadata": result_metadata,
            "created_at": current_time_str(),
        }

        result_id = await self._document_store.insert(doc, overwrite_existing)
        doc["result_id"] = result_id

        if do_cache:
            await self._cache_set(doc_id, normalized_part_idx, job_id, doc)

        return result_id

    async def get_result(
        self,
        doc_id: str,
        part_idx: int | None = None,
        job_id: str | None = None,
    ) -> ResultDoc:
        """
        Retrieve a processing result by business keys.

        :param doc_id: Document ID.
        :param part_idx: Part index (None or 0 for single-part results).
        :param job_id: Job ID (None if not using job grouping).

        :return: The result document.

        :raises ResultNotFound: If no result exists for the given keys.
        """
        normalized_part_idx = 0 if part_idx is None else part_idx

        cached = await self._cache_get(doc_id, normalized_part_idx, job_id)
        if cached is not None:
            return self._dict_to_result_doc(cached)

        query = self._build_query(doc_id, normalized_part_idx, job_id)
        async for doc in self._document_store.query(query):
            doc = self._add_result_id(doc)
            await self._cache_set(doc_id, normalized_part_idx, job_id, doc)
            return self._dict_to_result_doc(doc)

        msg = (
            f"Result not found for doc_id={doc_id}, part_idx={normalized_part_idx}, "
            f"job_id={job_id}"
        )
        raise ResultNotFound(msg)

    async def stream_doc_processing_results(
        self,
        doc_id: str,
        job_id: str | None = None,
        try_cache: bool = True,
    ) -> AsyncGenerator[ResultDoc, None]:
        """
        Stream all processing results for a document.

        :param doc_id: Document ID.
        :param job_id: Job ID (None if not using job grouping).
        :param try_cache: Whether to try cache first.

        :return: Async generator yielding result documents.

        :raises DocumentProcessingResultsNotFound: If no results exist.
        """
        results_found = False

        if try_cache:
            cached_items = await self._cache_get_by_prefix(doc_id, job_id)
            if cached_items:
                for doc in cached_items.values():
                    results_found = True
                    yield self._dict_to_result_doc(doc)
                return

        query = self._build_query(doc_id, job_id=job_id)
        async for doc in self._document_store.query(query):
            results_found = True
            doc = self._add_result_id(doc)
            part_idx = doc.get("part_idx", 0)
            await self._cache_set(doc_id, part_idx, job_id, doc)
            yield self._dict_to_result_doc(doc)

        if not results_found:
            msg = f"No results found for doc_id={doc_id}, job_id={job_id}"
            raise DocumentProcessingResultsNotFound(msg)

    def _add_result_id(self, doc: dict) -> dict:
        """Add result_id to doc from the store's doc_id_field if not already present."""
        if "result_id" in doc:
            return doc
        result_id = doc.get(self._doc_id_field)
        if result_id is not None:
            doc["result_id"] = result_id
        return doc

    def _dict_to_result_doc(self, doc: dict) -> ResultDoc:
        """Convert a dict to ResultDoc."""
        return ResultDoc(
            doc_id=doc["doc_id"],
            part_idx=doc.get("part_idx", 0),
            job_id=doc.get("job_id"),
            result=doc["result"],
            result_metadata=doc.get("result_metadata"),
            created_at=doc.get("created_at"),
            result_id=doc.get("result_id"),
        )

    async def _cache_set(
        self,
        doc_id: str,
        part_idx: int,
        job_id: str | None,
        doc: dict,
    ) -> None:
        """Set a value in cache, silently ignoring failures."""
        if self._cache is None:
            return
        try:
            cache_key = self._build_cache_key(doc_id, part_idx, job_id)
            await self._cache.set(cache_key, doc)
        except Exception as e:
            self._log.warning(
                f"Caching of document failed:\n"
                f"doc_id={doc_id}\n"
                f"part_id={part_idx}\n"
                f"job_id={job_id}\n"
                f"Exceptions(s): {summarize_exception_chain(e)}"
            )

    async def _cache_get(
        self,
        doc_id: str,
        part_idx: int,
        job_id: str | None,
    ) -> dict | None:
        """Get a value from cache, returning None on failure."""
        if self._cache is None:
            return None
        try:
            cache_key = self._build_cache_key(doc_id, part_idx, job_id)
            return await self._cache.get(cache_key)
        except Exception as e:
            self._log.warning(
                f"Retrieving from cache failed for:\n"
                f"doc_id={doc_id}\n"
                f"part_id={part_idx}\n"
                f"job_id={job_id}\n"
                f"Exceptions(s): {summarize_exception_chain(e)}"
            )
            return None

    async def _cache_get_by_prefix(
        self,
        doc_id: str,
        job_id: str | None,
    ) -> dict[str, dict]:
        """Get values from cache by prefix, returning empty dict on failure."""
        if self._cache is None:
            return {}
        try:
            cache_prefix = self._build_cache_prefix(doc_id, job_id)
            return await self._cache.get_by_key_prefix(cache_prefix)
        except Exception as e:
            self._log.warning(
                f"Retrieving from cache using key prefix failed for:\n"
                f"doc_id={doc_id}\n"
                f"job_id={job_id}\n"
                f"Exceptions(s): {summarize_exception_chain(e)}"
            )

            return {}

    def _build_cache_key(
        self,
        doc_id: str,
        part_idx: int,
        job_id: str | None,
    ) -> str:
        """Build a cache key from business keys."""
        return build_message_key(job_id=job_id, doc_id=doc_id, part_idx=part_idx)

    def _build_cache_prefix(
        self,
        doc_id: str,
        job_id: str | None,
    ) -> str:
        """Build a cache prefix for streaming queries."""
        return build_message_key_prefix(doc_id=doc_id, job_id=job_id)

    def _build_query(
        self,
        doc_id: str,
        part_idx: int | None = None,
        job_id: str | None = None,
    ) -> dict:
        """Build a query dict for the document store.

        :param doc_id: Document ID.
        :param part_idx: Part index. If None, part_idx is not filtered (for streaming
            all parts). If int, queries for exact part_idx value.
        :param job_id: Job ID.

        :return: Query dictionary.
        """
        query: dict = {"doc_id": doc_id}
        if part_idx is not None:
            query["part_idx"] = part_idx
        query["job_id"] = job_id
        return query
