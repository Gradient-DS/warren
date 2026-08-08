"""
Fake document parser worker for E2E testing.

Receives pdf_document messages, looks up fake markdown content,
stores the result, and publishes a markdown_document message.
"""

from examples.exchanges.data import FAKE_DOCUMENTS
from warren.storage.results.interface import (
    ResultsStoreInterface,
)
from warren.workers.workers import (
    FilteringWorkerBase,
)


class DocumentParserWorker(FilteringWorkerBase):
    """Fake parser that maps doc_id to pre-defined markdown content."""

    def __init__(
        self,
        worker_name: str,
        *,
        write_store: ResultsStoreInterface,
    ) -> None:
        super().__init__(worker_name)
        self._write_store = write_store

    def should_process(self, message: dict) -> bool:
        return message.get("data_type") == "pdf_document"

    async def process(self, message: dict) -> dict | None:
        doc_id: str = message["data"]["doc_id"]
        job_id: str = message["job_id"]

        markdown = FAKE_DOCUMENTS[doc_id]

        await self._write_store.store(
            result={"markdown": markdown},
            doc_id=doc_id,
            job_id=job_id,
        )

        self._log.info(f"Parsed {doc_id} ({len(markdown)} chars)")

        return {
            "data_type": "markdown_document",
            "data": {"doc_id": doc_id},
            "job_id": job_id,
            "origin": {"type": self.type, "name": self.name},
        }
