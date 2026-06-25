"""
Embedding worker — the final stage of the ``examples/rag`` pipeline.

Reads the stored chunks for a document, embeds them with OpenAI
(``text-embedding-3-small`` by default), stores ``{embedding, text}`` per
chunk, and publishes an ``embedded_document`` message so the job-status
worker can see the document reach the end of the pipeline.

Real network I/O, so this is the stage that exercises the failure
lifecycle: a transient API error becomes a ``SoftFailureException`` and the
message is retried with back-off; a missing key / bad request becomes a
``HardFailureException`` and is dropped. The OpenAI client is pure async
I/O (``AsyncOpenAI``), so the worker stays on the event loop — no threads.
"""

from warren.common import HardFailureException, SoftFailureException
from warren.storage.results.interface import ResultsStoreInterface
from warren.workers.workers import FilteringWorkerBase


class EmbeddingGeneratorWorker(FilteringWorkerBase):
    """Embeds text chunks via the OpenAI embeddings API."""

    def __init__(
        self,
        worker_name: str,
        *,
        read_store: ResultsStoreInterface,
        write_store: ResultsStoreInterface,
        api_key: str,
        model: str = "text-embedding-3-small",
    ) -> None:
        super().__init__(worker_name)
        self._read_store = read_store
        self._write_store = write_store
        self._api_key = api_key
        self._model = model
        self._client = None

    async def setup(self) -> None:
        # Imported lazily so the rest of the pipeline runs without the
        # optional `openai` dependency installed.
        from openai import AsyncOpenAI

        self._client = AsyncOpenAI(api_key=self._api_key)

    async def teardown(self) -> None:
        if self._client is not None:
            await self._client.close()

    def should_process(self, message: dict) -> bool:
        return message.get("data_type") == "text_chunks"

    async def process(self, message: dict) -> dict | None:
        from openai import APIError, APIStatusError

        doc_id: str = message["data"]["doc_id"]
        job_id: str = message["job_id"]

        chunks = [
            chunk
            async for chunk in self._read_store.stream_doc_processing_results(
                doc_id=doc_id,
                job_id=job_id,
            )
        ]
        texts = [chunk.result["text"] for chunk in chunks]
        if not texts:
            return None

        try:
            response = await self._client.embeddings.create(
                model=self._model,
                input=texts,
            )
        except APIStatusError as exc:
            # 4xx (bad request, auth, quota) won't fix itself on retry.
            if 400 <= exc.status_code < 500:
                msg = f"OpenAI rejected the request ({exc.status_code})"
                raise HardFailureException(msg, cause=exc) from exc
            msg = "OpenAI server error"
            raise SoftFailureException(msg, cause=exc) from exc
        except APIError as exc:
            # Connection/timeout errors are transient — retry with back-off.
            msg = "OpenAI API call failed"
            raise SoftFailureException(msg, cause=exc) from exc

        for chunk, item in zip(chunks, response.data, strict=True):
            await self._write_store.store(
                result={"embedding": item.embedding, "text": chunk.result["text"]},
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
