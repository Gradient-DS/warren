"""
Fake scenario for E2E testing.

Uses fake workers with pre-baked data — fast, deterministic, no
external dependencies. 4 fake documents produce 18 chunks and
18 embeddings.
"""

from typing import Dict, Optional

from document_processing.distributed.framework.common import MessageConsumerInterface
from document_processing.distributed.framework.storage.documents.interface import (
    GetDocumentFunc,
)
from document_processing.distributed.e2e_test.fake.workers.document_parser import (
    DocumentParserWorker,
)
from document_processing.distributed.e2e_test.fake.workers.embedding_generator import (
    EmbeddingGeneratorWorker,
)
from document_processing.distributed.e2e_test.fake.workers.text_chunker import (
    TextChunkerWorker,
)
from document_processing.distributed.e2e_test.failure_injection import FailureSpec
from document_processing.distributed.e2e_test.spec import (
    E2ETestWorkerSpec,
    ScenarioSpec,
    WorkerSpec,
)
from document_processing.distributed.framework.storage.results.interface import (
    ResultsStoreInterface,
)


def _create_document_parser(
    worker_name: str,
    stores: Dict[str, ResultsStoreInterface],
    get_document_func: Optional[GetDocumentFunc],
) -> MessageConsumerInterface:
    return DocumentParserWorker(
        worker_name=worker_name,
        write_store=stores["write"],
    )


def _create_text_chunker(
    worker_name: str,
    stores: Dict[str, ResultsStoreInterface],
    get_document_func: Optional[GetDocumentFunc],
) -> MessageConsumerInterface:
    return TextChunkerWorker(
        worker_name=worker_name,
        read_store=stores["read"],
        write_store=stores["write"],
    )


def _create_embedding_generator(
    worker_name: str,
    stores: Dict[str, ResultsStoreInterface],
    get_document_func: Optional[GetDocumentFunc],
) -> MessageConsumerInterface:
    return EmbeddingGeneratorWorker(
        worker_name=worker_name,
        read_store=stores["read"],
        write_store=stores["write"],
    )


SCENARIO: ScenarioSpec = ScenarioSpec(
    workers={
        "document_parser": WorkerSpec(
            collections={"write": "parsed_documents"},
            factory=_create_document_parser,
        ),
        "text_chunker": E2ETestWorkerSpec(
            collections={"read": "parsed_documents", "write": "chunks"},
            factory=_create_text_chunker,
            failure_spec=FailureSpec(
                fail_at_attempts=[1, 2],
                target_data_type="markdown_document",
                retry_after=2,
                retry_max=3,
            ),
        ),
        "embedding_generator": WorkerSpec(
            collections={"read": "chunks", "write": "embeddings"},
            factory=_create_embedding_generator,
        ),
    },
    result_collections=["parsed_documents", "chunks", "embeddings"],
    reference_collection="chunks",
    completion_collection="embeddings",
    final_data_type="embedded_document",
)
