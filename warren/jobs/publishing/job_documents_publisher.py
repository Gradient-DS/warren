"""
Abstract base for publishing job documents into the processing pipeline.

Orchestrates the load → register → publish flow for each document and
automatically records outcomes via ``PublishingTrackerInterface``. Concrete
publishers implement the four abstract methods for their specific source
types; the base handles orchestration, error handling, and tracking.
"""

from typing import Any

from abc import ABCMeta, abstractmethod
from collections.abc import AsyncIterable

from basics.base import Base
from basics.logging_utils import summarize_exception_chain

from document_processing.distributed.warren.pubsub.common import PublisherInterface
from document_processing.distributed.warren.storage.jobs.interface import (
    JobStoreInterface,
)
from document_processing.distributed.warren.storage.publishing_tracker.interface import (
    PublishingTrackerInterface,
)


class JobDocumentsPublisher(Base, metaclass=ABCMeta):
    """Abstract publisher harness with automatic outcome tracking.

    The publishing flow for each document:

    1. ``_load_document(source)`` — retrieve document bytes/data
    2. ``_register_document(job_id, doc_data)`` — store in document DB,
       get ``doc_id``
    3. ``_create_message(job_id, doc_id, doc_data)`` — build the
       exchange message

    If any step fails, the failure is recorded with the specific stage
    that failed, and processing continues with the next document.

    :param publisher: Publisher for sending messages to the exchange.
    :param tracker: Tracks publishing outcomes per document.
    :param job_store: Used to set ``num_documents`` after publishing
        and to mark zero-document jobs as completed.
    :param name: Optional logger name.
    """

    def __init__(
        self,
        *,
        publisher: PublisherInterface,
        tracker: PublishingTrackerInterface,
        job_store: JobStoreInterface,
        name: str | None = None,
    ) -> None:
        super().__init__(pybase_logger_name=name)
        self._publisher = publisher
        self._tracker = tracker
        self._job_store = job_store

    async def publish_job(
        self,
        job_id: str,
        sources: AsyncIterable,
        job_parameters: dict[str, Any] | None = None,
    ) -> dict:
        """Publish documents for a job from an async source.

        Iterates over sources, processing each through the
        load → register → publish pipeline.

        :param job_id: The job identifier (must already exist in
            ``job_store``).
        :param sources: Async iterable of document sources (type is
            application-defined).
        :param job_parameters: Job-level configuration (extraction
            settings, HTML backend choice, etc.) passed through to
            ``_create_message`` so concrete publishers can fold job
            settings into the per-document message. ``None`` is
            normalised to an empty dict.
        :return: Summary dict with ``published``, ``failed``, and
            ``total`` counts.
        """
        effective_params: dict[str, Any] = dict(job_parameters or {})

        published = 0
        failed = 0
        total = 0

        async for source in sources:
            total += 1
            source_id = self._get_source_id(source)

            doc_id = await self._process_source(
                job_id,
                source,
                source_id,
                effective_params,
            )
            if doc_id is not None:
                published += 1
            else:
                failed += 1

        await self._job_store.update_num_documents(job_id, published)

        if published == 0:
            self._log.warning(
                f"Job {job_id}: no documents published "
                f"({failed} failed, {total} attempted). "
                f"Marking job as completed."
            )
            await self._job_store.update_completion(
                job_id=job_id,
                completed=True,
                with_failures=(failed > 0),
            )

        return {"published": published, "failed": failed, "total": total}

    async def _process_source(
        self,
        job_id: str,
        source: Any,
        source_id: str,
        job_parameters: dict[str, Any],
    ) -> str | None:
        """Process a single source through load → register → publish.

        :return: The doc_id on success, or ``None`` on failure.
        """
        try:
            doc_data = await self._load_document(source)
        except Exception as e:
            chain = summarize_exception_chain(e)
            self._log.error(f"Failed to load {source_id}: {chain}")
            await self._record_failure_safe(job_id, None, source_id, chain, "load")
            return None

        try:
            doc_id = await self._register_document(job_id, doc_data)
        except Exception as e:
            chain = summarize_exception_chain(e)
            self._log.error(f"Failed to register {source_id}: {chain}")
            await self._record_failure_safe(
                job_id, None, source_id, chain, "register"
            )
            return None

        try:
            message = self._create_message(
                job_id,
                doc_id,
                doc_data,
                job_parameters,
            )
            await self._publisher(message)
        except Exception as e:
            chain = summarize_exception_chain(e)
            self._log.error(f"Failed to publish {source_id}: {chain}")
            await self._record_failure_safe(
                job_id, doc_id, source_id, chain, "publish"
            )
            return None

        # Publish succeeded. Recording success is best-effort and lives
        # outside the publish try/except so a tracker failure can never be
        # misclassified as a publish failure — the document *was* published.
        await self._record_success_safe(job_id, doc_id)
        return doc_id

    async def _record_failure_safe(
        self,
        job_id: str,
        doc_id: str | None,
        source_id: str,
        error: str,
        stage: str,
    ) -> None:
        """Record a per-document failure, best-effort.

        A tracker failure must not abort the publishing loop — a
        document's outcome is independent of whether we persisted its
        tracking record — so it is logged and swallowed.
        """
        try:
            await self._tracker.record_failure(
                job_id, doc_id, source_id, error, stage
            )
        except Exception as e:
            self._log.warning(
                f"Failed to record '{stage}' failure for {source_id}: "
                f"{summarize_exception_chain(e)}"
            )

    async def _record_success_safe(self, job_id: str, doc_id: str) -> None:
        """Record a per-document success, best-effort.

        The document has already been published, so a tracker failure
        here is logged and swallowed rather than aborting the loop or
        being misread as a publish failure.
        """
        try:
            await self._tracker.record_success(job_id, doc_id)
        except Exception as e:
            self._log.warning(
                f"Failed to record success for doc '{doc_id}': "
                f"{summarize_exception_chain(e)}"
            )

    @abstractmethod
    async def _load_document(self, source: Any) -> Any:
        """Load a document from the given source.

        :param source: Application-defined source (path, URL, cloud
            location, etc.).
        :return: Loaded document data.
        :raises Exception: If loading fails.
        """
        ...

    @abstractmethod
    async def _register_document(self, job_id: str, doc_data: Any) -> str:
        """Register the document in the document store.

        :param job_id: Job identifier.
        :param doc_data: Data returned by ``_load_document``.
        :return: Document ID assigned by the store.
        :raises Exception: If registration fails.
        """
        ...

    @abstractmethod
    def _create_message(
        self,
        job_id: str,
        doc_id: str,
        doc_data: Any,
        job_parameters: dict[str, Any],
    ) -> dict:
        """Create the message dict to publish to the exchange.

        Must include ``origin.type = "publisher"`` per framework
        convention (see ``PUBLISHER_ORIGIN_TYPE``).

        :param job_id: Job identifier.
        :param doc_id: Document ID from registration.
        :param doc_data: Data returned by ``_load_document``.
        :param job_parameters: Job-level settings passed through from
            ``publish_job``. Concrete publishers may fold these into the
            message's ``job_parameters`` field or use them to decide
            per-document routing (e.g. set ``preprocessing_required``
            based on ``job_parameters["extraction"]["enabled"]``).
        :return: Message dict for the exchange.
        """
        ...

    @abstractmethod
    def _get_source_id(self, source: Any) -> str:
        """Extract a human-readable identifier from a source.

        Used in error messages and failure tracking when ``doc_id``
        is not yet available.
        """
        ...
