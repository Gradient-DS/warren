"""
Fake embedding generator worker for E2E testing.

Receives text_chunks messages, reads stored chunks, generates fake
embedding vectors, and stores them. Publishes an ``embedded_document``
message for observability (job status tracking).
"""

from typing import Dict, List, Optional

import hashlib
import struct

from document_processing.distributed.framework.storage.results.interface import (
    ResultsStoreInterface,
)
from document_processing.distributed.framework.workers.workers import (
    FilteringWorkerBase,
)

EMBEDDING_DIM: int = 8


def _fake_embedding(text: str) -> List[float]:
    """Generate a deterministic fake embedding from text content."""
    digest = hashlib.sha256(text.encode()).digest()
    return list(struct.unpack(f"{EMBEDDING_DIM}f", digest[: EMBEDDING_DIM * 4]))


class EmbeddingGeneratorWorker(FilteringWorkerBase):
    """Generates fake embeddings for text chunks."""

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
        return message.get("data_type") == "text_chunks"

    async def process(self, message: Dict) -> Optional[Dict]:
        doc_id: str = message["data"]["doc_id"]
        job_id: str = message["job_id"]

        chunks = [
            chunk
            async for chunk in self._read_store.stream_doc_processing_results(
                doc_id=doc_id,
                job_id=job_id,
            )
        ]

        for chunk in chunks:
            text: str = chunk.result["text"]
            embedding = _fake_embedding(text)

            await self._write_store.store(
                result={"embedding": embedding, "text": text},
                doc_id=doc_id,
                part_idx=chunk.part_idx,
                job_id=job_id,
            )

        self._log.info(f"Embedded {doc_id}: {len(chunks)} vectors")

        return {
            "data_type": "embedded_document",
            "data": {"doc_id": doc_id, "num_embeddings": len(chunks)},
            "job_id": job_id,
            "origin": {"type": self.type, "name": self.name},
        }
