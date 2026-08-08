"""
Concrete publisher for the synthetic exchange examples.

Publishes the stand-in documents from ``FAKE_DOCUMENTS`` — no actual file
loading or document store registration. Each doc_id is pre-assigned. (The
``examples/rag`` pipeline shows the real path: registering documents by
location and fetching their bytes.)
"""

from typing import Any

from warren.constants import PUBLISHER_ORIGIN_TYPE
from warren.jobs.publishing.job_documents_publisher import (
    JobDocumentsPublisher,
)
from warren.pubsub.common import PublisherInterface
from warren.storage.jobs.interface import (
    JobStoreInterface,
)
from warren.storage.publishing_tracker.interface import (
    PublishingTrackerInterface,
)


class MockDocumentsPublisher(JobDocumentsPublisher):
    """Publishes synthetic documents for the exchange examples.

    Sources are ``(doc_id, content)`` tuples from ``FAKE_DOCUMENTS``.
    No actual file loading or document store registration — the
    doc_id is pre-assigned and the content is in-memory.

    :param publisher: RMQ publisher for sending messages.
    :param tracker: Publishing outcome tracker.
    :param job_store: Job store for setting num_documents.
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
        super().__init__(
            publisher=publisher,
            tracker=tracker,
            job_store=job_store,
            name=name,
        )

    async def _load_document(self, source: Any) -> tuple[str, str]:
        """Return the source as-is (doc_id, content)."""
        return source

    async def _register_document(
        self,
        job_id: str,
        doc_data: Any,
    ) -> str:
        """Extract the pre-assigned doc_id."""
        doc_id, _content = doc_data
        return doc_id

    def _create_message(
        self,
        job_id: str,
        doc_id: str,
        doc_data: Any,
        job_parameters: dict[str, Any],
    ) -> dict:
        """Create a pdf_document message for the exchange.

        ``job_parameters`` is accepted for base-class conformance but
        ignored — the synthetic scenario exercises only the in-memory happy
        path and has no settings that parametrise message construction.
        """
        return {
            "data_type": "pdf_document",
            "data": {
                "doc_id": doc_id,
                "path": f"/synthetic/path/{doc_id}.pdf",
            },
            "job_id": job_id,
            "origin": {
                "type": PUBLISHER_ORIGIN_TYPE,
                "name": "mock-documents-publisher",
            },
        }

    def _get_source_id(self, source: Any) -> str:
        """Extract doc_id from source tuple."""
        doc_id, _content = source
        return doc_id
