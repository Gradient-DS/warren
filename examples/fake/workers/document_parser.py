"""
Fake document parser worker for E2E testing.

Receives pdf_document messages, looks up fake markdown content,
stores the result, and publishes a markdown_document message.
"""
from typing import Dict, Optional

from document_processing.distributed.e2e_test.fake.data import FAKE_DOCUMENTS
from document_processing.distributed.framework.storage.results.interface import (
    ResultsStoreInterface,
)
from document_processing.distributed.framework.workers.workers import FilteringWorkerBase


class DocumentParserWorker(FilteringWorkerBase):
    """Fake parser that maps doc_id to pre-defined markdown content."""

    def __init__(
        self,
        worker_id: str,
        *,
        write_store: ResultsStoreInterface,
    ) -> None:
        super().__init__(worker_id)
        self._write_store = write_store

    def should_process(self, message: Dict) -> bool:
        return message.get("data_type") == "pdf_document"

    async def process(self, message: Dict) -> Optional[Dict]:
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
            "origin": {"type": "document_parser", "name": self._worker_id},
        }
