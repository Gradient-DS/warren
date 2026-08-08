"""
PDF parser worker — the first real stage of the ``examples/rag`` pipeline.

Receives a ``pdf_document`` message carrying a PDF ``url``, downloads the PDF,
extracts its text with ``pypdf``, stores it, and publishes a
``parsed_document`` message.

Two patterns worth noting:

- **The worker fetches its own input.** The message carries just a URL; the
  worker downloads the bytes itself with ``httpx``. No document-location
  plumbing — for a self-contained example, "here's a URL, go get it" is the
  simplest thing that works.
- **Offloading CPU work.** Downloading is async I/O, but ``pypdf`` parsing is
  CPU-bound and blocking. We run it in a thread via ``run_in_executor`` so the
  event loop stays free for RabbitMQ heartbeats (see
  ``AsyncProcessingWorkerBase`` for the rule).
"""

import asyncio
import io

from warren.common import HardFailureException, SoftFailureException
from warren.storage.results.interface import ResultsStoreInterface
from warren.workers.workers import FilteringWorkerBase


def _extract_text(raw: bytes) -> str:
    """Extract page text from PDF bytes (blocking — run off the loop)."""
    import pypdf

    reader = pypdf.PdfReader(io.BytesIO(raw))
    return "\n\n".join(page.extract_text() for page in reader.pages).strip()


class PdfParserWorker(FilteringWorkerBase):
    """Downloads a PDF by URL and extracts its text with ``pypdf``."""

    def __init__(
        self,
        worker_name: str,
        *,
        write_store: ResultsStoreInterface,
    ) -> None:
        super().__init__(worker_name)
        self._write_store = write_store
        self._client = None

    async def setup(self) -> None:
        # Imported lazily so the framework runs without the optional `httpx`
        # dependency (it ships with the `examples` extra).
        import httpx

        self._client = httpx.AsyncClient(
            follow_redirects=True,
            timeout=60.0,
            headers={"User-Agent": "warren-example/0.1"},
        )

    async def teardown(self) -> None:
        if self._client is not None:
            await self._client.aclose()

    def should_process(self, message: dict) -> bool:
        return message.get("data_type") == "pdf_document"

    async def process(self, message: dict) -> dict | None:
        import httpx

        doc_id: str = message["data"]["doc_id"]
        job_id: str = message["job_id"]
        url: str = message["data"]["url"]

        try:
            response = await self._client.get(url)
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            # 4xx won't fix itself on retry; 5xx might.
            if 400 <= exc.response.status_code < 500:
                msg = f"PDF unavailable ({exc.response.status_code}): {url}"
                raise HardFailureException(msg, cause=exc) from exc
            msg = f"PDF server error ({exc.response.status_code}): {url}"
            raise SoftFailureException(msg, cause=exc) from exc
        except httpx.HTTPError as exc:
            # Connection/timeout errors are transient — retry with back-off.
            msg = f"PDF download failed: {url}"
            raise SoftFailureException(msg, cause=exc) from exc

        text = await asyncio.get_event_loop().run_in_executor(
            None, _extract_text, response.content
        )

        await self._write_store.store(
            result={"text": text},
            doc_id=doc_id,
            job_id=job_id,
        )
        self._log.info(
            f"Parsed {doc_id} ({len(text)} chars from {len(response.content)} bytes)"
        )

        return {
            "data_type": "parsed_document",
            "data": {"doc_id": doc_id},
            "job_id": job_id,
            "origin": {"type": self.type, "name": self.name},
        }
