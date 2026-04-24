"""
Fake scenario for E2E testing.

Uses fake workers with pre-baked data — fast, deterministic, no
external dependencies. 4 fake documents produce 18 chunks and
18 embeddings.
"""

from document_processing.distributed.e2e_test.failure_injection import FailureSpec
from document_processing.distributed.e2e_test.fake.workers.document_parser import (
    DocumentParserWorker,
)
from document_processing.distributed.e2e_test.fake.workers.embedding_generator import (
    EmbeddingGeneratorWorker,
)
from document_processing.distributed.e2e_test.fake.workers.text_chunker import (
    TextChunkerWorker,
)
from document_processing.distributed.e2e_test.spec import (
    E2ETestWorkerSpec,
    ScenarioSpec,
    WorkerFactoryContext,
    WorkerSpec,
)
from document_processing.distributed.framework.common import MessageConsumerInterface


async def _create_document_parser(
    ctx: WorkerFactoryContext,
) -> MessageConsumerInterface:
    return DocumentParserWorker(
        worker_name=ctx.worker_name,
        write_store=ctx.stores["write"],
    )


async def _create_text_chunker(ctx: WorkerFactoryContext) -> MessageConsumerInterface:
    return TextChunkerWorker(
        worker_name=ctx.worker_name,
        read_store=ctx.stores["read"],
        write_store=ctx.stores["write"],
    )


async def _create_embedding_generator(
    ctx: WorkerFactoryContext,
) -> MessageConsumerInterface:
    return EmbeddingGeneratorWorker(
        worker_name=ctx.worker_name,
        read_store=ctx.stores["read"],
        write_store=ctx.stores["write"],
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
