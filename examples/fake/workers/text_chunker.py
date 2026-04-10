"""
Text chunker worker for E2E testing.

Receives markdown_document messages, reads the stored markdown,
splits by paragraph (double newline), stores each chunk, and
publishes a text_chunks message.
"""

from typing import Dict, Optional

from document_processing.distributed.framework.storage.results.interface import (
    ResultsStoreInterface,
)
from document_processing.distributed.framework.workers.workers import (
    FilteringWorkerBase,
)


class TextChunkerWorker(FilteringWorkerBase):
    """Splits markdown documents into paragraph-level chunks."""

    def __init__(
        self,
        worker_name: str,
        *,
        read_store: ResultsStoreInterface,
        write_store: ResultsStoreInterface,
    ) -> None:
        super().__init__(worker_name)
        self._read_store = read_store
        self._write_store = write_store

    def should_process(self, message: Dict) -> bool:
        return message.get("data_type") == "markdown_document"

    async def process(self, message: Dict) -> Optional[Dict]:
        doc_id: str = message["data"]["doc_id"]
        job_id: str = message["job_id"]

        result_doc = await self._read_store.get_result(
            doc_id=doc_id,
            job_id=job_id,
        )

        markdown: str = result_doc.result["markdown"]
        chunks = [chunk for chunk in markdown.split("\n\n") if chunk.strip()]

        for i, chunk in enumerate(chunks):
            await self._write_store.store(
                result={"text": chunk},
                doc_id=doc_id,
                part_idx=i,
                job_id=job_id,
            )

        self._log.info(f"Chunked {doc_id} into {len(chunks)} chunks")

        return {
            "data_type": "text_chunks",
            "data": {
                "doc_id": doc_id,
                "num_chunks": len(chunks),
            },
            "job_id": job_id,
            "origin": {"type": self.type, "name": self.name},
        }
