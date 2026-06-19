"""
Capability workers for the job-defined-routing example.

Same parse → chunk → embed work as ``examples/fake``, but written as
``CapabilityWorkerBase`` subclasses: each declares the ``data_type`` it
``accepts`` (so ``should_process`` is derived, not hand-written) and the
``data_type`` it ``produces``. Unlike the fake workers, they **propagate
``job_parameters``** to their output messages, so the per-job ``RoutingPlan``
rides along the chain and the next hop can be resolved.

On a direct exchange these workers bind to their own *worker-type id* (e.g.
``document_parser``), and a ``RoutingPlanRouter`` on each publisher sends the
result to whichever worker the job's plan names next.
"""

from typing import Any

from examples.fake.data import FAKE_DOCUMENTS
from examples.fake.workers.embedding_generator import _fake_embedding
from warren.storage.results.interface import ResultsStoreInterface
from warren.workers.workers import CapabilityWorkerBase


def _emit(worker: CapabilityWorkerBase, message: dict, *, data: dict) -> dict:
    """Build an output message, propagating job_parameters (the routing plan)."""
    return {
        "data_type": worker.produces,
        "data": data,
        "job_id": message["job_id"],
        "origin": {"type": worker.type, "name": worker.name},
        # Carry the RoutingPlan forward so downstream hops can be resolved.
        "job_parameters": message.get("job_parameters", {}),
    }


class DocumentParserWorker(CapabilityWorkerBase):
    """accepts pdf_document -> produces markdown_document."""

    def __init__(
        self, worker_name: str, *, write_store: ResultsStoreInterface, **kw: Any
    ):
        super().__init__(worker_name, **kw)
        self._write_store = write_store

    async def process(self, message: dict) -> dict | None:
        doc_id = message["data"]["doc_id"]
        markdown = FAKE_DOCUMENTS[doc_id]
        await self._write_store.store(
            result={"markdown": markdown}, doc_id=doc_id, job_id=message["job_id"]
        )
        self._log.info(f"Parsed {doc_id} ({len(markdown)} chars)")
        return _emit(self, message, data={"doc_id": doc_id})


class TextChunkerWorker(CapabilityWorkerBase):
    """accepts markdown_document -> produces text_chunks."""

    def __init__(
        self,
        worker_name: str,
        *,
        read_store: ResultsStoreInterface,
        write_store: ResultsStoreInterface,
        **kw: Any,
    ):
        super().__init__(worker_name, **kw)
        self._read_store = read_store
        self._write_store = write_store

    async def process(self, message: dict) -> dict | None:
        doc_id = message["data"]["doc_id"]
        job_id = message["job_id"]
        result_doc = await self._read_store.get_result(doc_id=doc_id, job_id=job_id)
        chunks = [c for c in result_doc.result["markdown"].split("\n\n") if c.strip()]
        for i, chunk in enumerate(chunks):
            await self._write_store.store(
                result={"text": chunk}, doc_id=doc_id, part_idx=i, job_id=job_id
            )
        self._log.info(f"Chunked {doc_id} into {len(chunks)} chunks")
        return _emit(self, message, data={"doc_id": doc_id, "num_chunks": len(chunks)})


class EmbeddingGeneratorWorker(CapabilityWorkerBase):
    """accepts text_chunks -> produces embedded_document."""

    def __init__(
        self,
        worker_name: str,
        *,
        read_store: ResultsStoreInterface,
        write_store: ResultsStoreInterface,
        **kw: Any,
    ):
        super().__init__(worker_name, **kw)
        self._read_store = read_store
        self._write_store = write_store

    async def process(self, message: dict) -> dict | None:
        doc_id = message["data"]["doc_id"]
        job_id = message["job_id"]
        chunks = [
            c
            async for c in self._read_store.stream_doc_processing_results(
                doc_id=doc_id, job_id=job_id
            )
        ]
        for chunk in chunks:
            await self._write_store.store(
                result={"embedding": _fake_embedding(chunk.result["text"])},
                doc_id=doc_id,
                part_idx=chunk.part_idx,
                job_id=job_id,
            )
        self._log.info(f"Embedded {doc_id}: {len(chunks)} vectors")
        return _emit(
            self, message, data={"doc_id": doc_id, "num_embeddings": len(chunks)}
        )
