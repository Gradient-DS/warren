from typing import Protocol

from collections.abc import AsyncGenerator

from pydantic import BaseModel

from document_processing.distributed.warren.storage.exceptions import (
    ResourceNotFoundError,
)


class ResultDoc(BaseModel):
    """Document representing a stored processing result."""

    doc_id: str
    part_idx: int = 0
    job_id: str | None = None
    result: dict
    result_metadata: dict | None = None
    created_at: str | None = None
    result_id: str | None = None


class ResultNotFound(ResourceNotFoundError):
    pass


class DocumentProcessingResultsNotFound(ResourceNotFoundError):
    pass


class ResultsStoreInterface(Protocol):
    """
    Async document-level results store for a specific type of document processing.

    All methods are async — implementations must use non-blocking I/O
    to avoid blocking the event loop.

    Assumes storage and caching capabilities that are aware of the specific result type.
    The implementation is dependent on the result/worker type.
    """

    # TODO: Add batch_store() method for storing multiple results in a single
    #   call, enabling bulk writes and reducing round-trips to the storage
    #   backend. Signature should accept a sequence of (result, doc_id,
    #   part_idx, job_id) tuples.

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
        Store (partial) result for processing a document.

        :param result: Document processing (partial) result.
        :param doc_id: Original document ID.
        :param part_idx: Part index of the result (None or 0 for single-part results).
        :param job_id: Job ID of the processing job (None if not using job grouping).
        :param result_metadata: Metadata about how the result was produced.
        :param do_cache: Whether to cache the result.
        :param overwrite_existing: Whether to overwrite existing result.

        :returns: ID of the stored result.
        """
        ...

    async def get_result(
        self,
        doc_id: str,
        part_idx: int | None = None,
        job_id: str | None = None,
    ) -> ResultDoc:
        """
        Retrieves the document processing result for given business keys.

        :param doc_id: The document ID.
        :param part_idx: The part index (None or 0 for single-part results).
        :param job_id: The job ID (None if not using job grouping).

        :returns: The result document.
        :raises: ResultNotFound if result does not exist.
        """
        ...

    def stream_doc_processing_results(
        self,
        doc_id: str,
        job_id: str | None = None,
        try_cache: bool = True,
    ) -> AsyncGenerator[ResultDoc, None]:
        """
        Stream all processing results for given document ID and job ID.

        Returns an async generator — callers use
        ``async for result in store.stream_doc_processing_results(...)``.

        :param doc_id: The document ID.
        :param job_id: The job ID (None if not using job grouping).
        :param try_cache: Whether to try cache first before querying document store.

        :returns: Async generator yielding result documents.
        :raises: DocumentProcessingResultsNotFound if no results exist for the given
            document ID and job ID.
        """
        ...
