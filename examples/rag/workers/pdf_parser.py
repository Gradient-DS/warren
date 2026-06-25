"""
PDF parser worker — the first real stage of the ``examples/rag`` pipeline.

Receives a ``pdf_document`` message carrying a document *location* (not the
bytes), fetches the raw PDF through the framework's document fetcher (which
caches and resolves the location), extracts the text with ``pypdf``, stores
it, and publishes a ``parsed_document`` message.

Two patterns worth noting:

- **Document fetcher.** The worker never reads the filesystem itself. It is
  handed a ``get_document_func`` (because its ``WorkerSpec`` sets
  ``needs_document_fetcher=True``) and asks that for bytes by location — so
  the same worker works against local paths, URLs, or cloud storage depending
  only on how the runtime is wired.
- **Offloading CPU work.** Fetching bytes is async I/O, but ``pypdf`` parsing
  is CPU-bound and blocking. We run it in a thread via ``run_in_executor`` so
  the event loop stays free for RabbitMQ heartbeats (see
  ``AsyncProcessingWorkerBase`` for the rule).
"""

import asyncio
import io

from pydantic import TypeAdapter

from warren.common import HardFailureException
from warren.storage.documents.interface import (
    DocumentNotFoundError,
    DocumentResolutionError,
    GetDocumentFunc,
)
from warren.storage.documents.location import AnyDocumentLocation, DocumentLocation
from warren.storage.results.interface import ResultsStoreInterface
from warren.workers.workers import FilteringWorkerBase


_LOCATION_ADAPTER: TypeAdapter[DocumentLocation] = TypeAdapter(AnyDocumentLocation)


def _extract_text(raw: bytes) -> str:
    """Extract page text from PDF bytes (blocking — run off the loop)."""
    import pypdf

    reader = pypdf.PdfReader(io.BytesIO(raw))
    return "\n\n".join(page.extract_text() for page in reader.pages).strip()


class PdfParserWorker(FilteringWorkerBase):
    """Fetches a PDF by location and extracts its text with ``pypdf``."""

    def __init__(
        self,
        worker_name: str,
        *,
        get_document_func: GetDocumentFunc,
        write_store: ResultsStoreInterface,
    ) -> None:
        super().__init__(worker_name)
        self._get_document = get_document_func
        self._write_store = write_store

    def should_process(self, message: dict) -> bool:
        return message.get("data_type") == "pdf_document"

    async def process(self, message: dict) -> dict | None:
        doc_id: str = message["data"]["doc_id"]
        job_id: str = message["job_id"]
        location = _LOCATION_ADAPTER.validate_python(
            message["data"]["document_location"]
        )

        # DocumentNotFoundError (file gone) is a hard failure; transient
        # resolution errors stay soft and propagate to the retry worker.
        try:
            raw = await self._get_document(doc_id, location)
        except DocumentNotFoundError as exc:
            msg = f"PDF not found: {doc_id}"
            raise HardFailureException(msg, cause=exc) from exc
        except DocumentResolutionError:
            raise

        text = await asyncio.get_event_loop().run_in_executor(None, _extract_text, raw)

        await self._write_store.store(
            result={"text": text},
            doc_id=doc_id,
            job_id=job_id,
        )
        self._log.info(f"Parsed {doc_id} ({len(text)} chars)")

        return {
            "data_type": "parsed_document",
            "data": {"doc_id": doc_id},
            "job_id": job_id,
            "origin": {"type": self.type, "name": self.name},
        }
