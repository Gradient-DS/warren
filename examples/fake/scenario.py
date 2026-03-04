"""
Fake scenario for E2E testing.

Uses fake workers with pre-baked data — fast, deterministic, no
external dependencies. 4 fake documents produce 18 chunks and
18 embeddings.
"""
from typing import Dict

from document_processing.distributed.common import ConsumeMessageFunc
from document_processing.distributed.e2e_test.fake.workers.document_parser import (
    DocumentParserWorker,
)
from document_processing.distributed.e2e_test.fake.workers.embedding_generator import (
    EmbeddingGeneratorWorker,
)
from document_processing.distributed.e2e_test.fake.workers.text_chunker import (
    TextChunkerWorker,
)
from document_processing.distributed.e2e_test.spec import ScenarioSpec, WorkerSpec
from document_processing.distributed.storage.results.interface import (
    ResultsStoreInterface,
)


def _create_document_parser(
    worker_id: str,
    stores: Dict[str, ResultsStoreInterface],
) -> ConsumeMessageFunc:
    return DocumentParserWorker(
        worker_id=worker_id,
        write_store=stores["write"],
    )


def _create_text_chunker(
    worker_id: str,
    stores: Dict[str, ResultsStoreInterface],
) -> ConsumeMessageFunc:
    return TextChunkerWorker(
        worker_id=worker_id,
        read_store=stores["read"],
        write_store=stores["write"],
    )


def _create_embedding_generator(
    worker_id: str,
    stores: Dict[str, ResultsStoreInterface],
) -> ConsumeMessageFunc:
    return EmbeddingGeneratorWorker(
        worker_id=worker_id,
        read_store=stores["read"],
        write_store=stores["write"],
    )


SCENARIO: ScenarioSpec = ScenarioSpec(
    workers={
        "document_parser": WorkerSpec(
            collections={"write": "parsed_documents"},
            factory=_create_document_parser,
        ),
        "text_chunker": WorkerSpec(
            collections={"read": "parsed_documents", "write": "chunks"},
            factory=_create_text_chunker,
        ),
        "embedding_generator": WorkerSpec(
            collections={"read": "chunks", "write": "embeddings"},
            factory=_create_embedding_generator,
            terminal=True,
        ),
    },
    result_collections=["parsed_documents", "chunks", "embeddings"],
    reference_collection="chunks",
    completion_collection="embeddings",
)
