"""
Real PDF -> chunks -> embeddings pipeline (fanout exchange).

The get-started example that does actual work: it downloads real PDFs (two
arXiv papers by default), extracts their text with ``pypdf``, splits the text
into chunks, and embeds each chunk with the OpenAI API (bring your own
``OPENAI_API_KEY``).

It runs on a **fanout** exchange — the simplest topology. Every worker
receives every message in its own queue and self-selects via
``should_process``; the routing keys are ignored. Adding a fourth stage
(say, a summariser) is purely additive: drop in a worker, no routing
changes anywhere. See the README's "When to use which exchange" for when a
topic or direct exchange earns its keep instead.
"""

import os

from examples.rag.workers.embedding_generator import EmbeddingGeneratorWorker
from examples.rag.workers.pdf_parser import PdfParserWorker
from examples.rag.workers.text_chunker import TextChunkerWorker
from warren.common import MessageConsumerInterface
from warren.pubsub.rabbitmq.config import RMQExchangeConfig
from warren.runtime.spec import (
    PipelineSpec,
    PublishSpec,
    WorkerFactoryContext,
    WorkerSpec,
)


async def _create_pdf_parser(ctx: WorkerFactoryContext) -> MessageConsumerInterface:
    return PdfParserWorker(
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
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        msg = (
            "OPENAI_API_KEY is not set. The embedding worker needs an OpenAI "
            "API key — export OPENAI_API_KEY before starting it."
        )
        raise RuntimeError(msg)
    return EmbeddingGeneratorWorker(
        worker_name=ctx.worker_name,
        read_store=ctx.stores["read"],
        write_store=ctx.stores["write"],
        api_key=api_key,
        model=os.environ.get("OPENAI_EMBEDDING_MODEL", "text-embedding-3-small"),
    )


PIPELINE: PipelineSpec = PipelineSpec(
    exchange=RMQExchangeConfig(name="pdf-jobs", type="fanout"),
    workers={
        "pdf_parser": WorkerSpec(
            collections={"write": "parsed_documents"},
            factory=_create_pdf_parser,
            # The worker downloads the PDF from the URL in the message itself,
            # so it needs no document fetcher.
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
            publish=PublishSpec(),
        ),
    },
    result_collections=["parsed_documents", "chunks", "embeddings"],
    reference_collection="chunks",
    completion_collection="embeddings",
    final_data_type="embedded_document",
)
