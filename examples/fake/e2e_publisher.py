"""
Concrete publisher for the fake E2E scenario.

Publishes synthetic documents from ``FAKE_DOCUMENTS`` — no actual file
loading or document store registration. Each doc_id is pre-assigned.
"""

from typing import Optional, Tuple, Dict, Any

from document_processing.distributed.framework.constants import PUBLISHER_ORIGIN_TYPE
from document_processing.distributed.framework.pubsub.common import PublisherInterface
from document_processing.distributed.framework.storage.jobs.interface import (
    JobStoreInterface,
)
from document_processing.distributed.framework.storage.publishing_tracker.interface import (
    PublishingTrackerInterface,
)
from document_processing.distributed.framework.workers.publishing_worker_base import (
    PublishingWorkerBase,
)


class FakeE2EPublisher(PublishingWorkerBase):
    """Publishes fake documents for E2E testing.

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
        name: Optional[str] = None,
    ) -> None:
        super().__init__(
            publisher=publisher,
            tracker=tracker,
            job_store=job_store,
            name=name,
        )

    async def _load_document(self, source: Any) -> Tuple[str, str]:
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
    ) -> Dict:
        """Create a pdf_document message for the exchange."""
        return {
            "data_type": "pdf_document",
            "data": {
                "doc_id": doc_id,
                "path": f"/fake/path/{doc_id}.pdf",
            },
            "job_id": job_id,
            "origin": {
                "type": PUBLISHER_ORIGIN_TYPE,
                "name": "e2e-fake-publisher",
            },
        }

    def _get_source_id(self, source: Any) -> str:
        """Extract doc_id from source tuple."""
        doc_id, _content = source
        return doc_id
