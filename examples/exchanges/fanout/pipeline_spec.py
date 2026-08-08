"""
Fake pipeline spec for the minimal runnable example.

Uses fake workers with pre-baked data — fast, deterministic, no
external dependencies. 4 fake documents produce 18 chunks and
18 embeddings.
"""

from examples.exchanges.workers.document_parser import (
    DocumentParserWorker,
)
from examples.exchanges.workers.embedding_generator import (
    EmbeddingGeneratorWorker,
)
from examples.exchanges.workers.text_chunker import (
    TextChunkerWorker,
)
from warren.common import MessageConsumerInterface
from warren.pubsub.rabbitmq.config import RMQExchangeConfig
from warren.runtime.spec import (
    PipelineSpec,
    PublishSpec,
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


# A single fanout exchange: every worker receives every message and
# self-selects via should_process(). No routing keys (binding_key=None,
# publish carries no route).
PIPELINE: PipelineSpec = PipelineSpec(
    exchange=RMQExchangeConfig(name="jobs", type="fanout"),
    workers={
        "document_parser": WorkerSpec(
            collections={"write": "parsed_documents"},
            factory=_create_document_parser,
            publish=PublishSpec(),
        ),
        "text_chunker": WorkerSpec(
            collections={"read": "parsed_documents", "write": "chunks"},
            factory=_create_text_chunker,
            publish=PublishSpec(),
        ),
        "embedding_generator": WorkerSpec(
            collections={"read": "chunks", "write": "embeddings"},
            factory=_create_embedding_generator,
            # Not terminal: still publishes "embedded_document" so the
            # job-status worker can observe completion.
            publish=PublishSpec(),
        ),
    },
    result_collections=["parsed_documents", "chunks", "embeddings"],
    reference_collection="chunks",
    completion_collection="embeddings",
    final_data_type="embedded_document",
)
