"""
Fake pipeline spec for the minimal runnable example.

Uses fake workers with pre-baked data — fast, deterministic, no
external dependencies. 4 fake documents produce 18 chunks and
18 embeddings.
"""

from examples.fake.workers.document_parser import (
    DocumentParserWorker,
)
from examples.fake.workers.embedding_generator import (
    EmbeddingGeneratorWorker,
)
from examples.fake.workers.text_chunker import (
    TextChunkerWorker,
)
from warren.common import MessageConsumerInterface
from warren.runtime.spec import (
    PipelineSpec,
    WorkerFactoryContext,
    WorkerSpec,
)


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


PIPELINE: PipelineSpec = PipelineSpec(
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
        ),
    },
    result_collections=["parsed_documents", "chunks", "embeddings"],
    reference_collection="chunks",
    completion_collection="embeddings",
    final_data_type="embedded_document",
)
