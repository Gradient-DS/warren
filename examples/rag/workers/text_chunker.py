"""
Text chunker worker — the second stage of the ``examples/rag`` pipeline.

Reads the parsed text, splits it into overlapping character-bounded chunks
on paragraph/sentence boundaries (a small, dependency-free splitter — good
enough for a runnable example, not a production chunking strategy), stores
each chunk, and publishes a ``text_chunks`` message.
"""

from warren.storage.results.interface import ResultsStoreInterface
from warren.workers.workers import FilteringWorkerBase


# Conservative defaults: small chunks keep the example fast and the
# embedding calls cheap. Tune for your own corpus.
CHUNK_SIZE: int = 500
CHUNK_OVERLAP: int = 80


def chunk_text(
    text: str,
    *,
    size: int = CHUNK_SIZE,
    overlap: int = CHUNK_OVERLAP,
) -> list[str]:
    """Split text into ~``size``-char chunks, breaking on whitespace.

    Greedy: accumulate paragraphs/words up to ``size``, then start a new
    chunk carrying ``overlap`` characters of tail context. Never splits a
    word in half.
    """
    words = text.split()
    chunks: list[str] = []
    current: list[str] = []
    length = 0

    for word in words:
        add = len(word) + (1 if current else 0)
        if length + add > size and current:
            chunk = " ".join(current)
            chunks.append(chunk)
            tail = chunk[-overlap:].split(" ", 1)
            current = [tail[-1]] if overlap and len(tail) > 1 else []
            length = len(current[0]) if current else 0
        current.append(word)
        length += add

    if current:
        chunks.append(" ".join(current))
    return chunks


class TextChunkerWorker(FilteringWorkerBase):
    """Splits parsed document text into overlapping chunks."""

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

    def should_process(self, message: dict) -> bool:
        return message.get("data_type") == "parsed_document"

    async def process(self, message: dict) -> dict | None:
        doc_id: str = message["data"]["doc_id"]
        job_id: str = message["job_id"]

        result_doc = await self._read_store.get_result(doc_id=doc_id, job_id=job_id)
        chunks = chunk_text(result_doc.result["text"])

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
            "data": {"doc_id": doc_id, "num_chunks": len(chunks)},
            "job_id": job_id,
            "origin": {"type": self.type, "name": self.name},
        }
