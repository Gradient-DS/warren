"""
Job-defined-routing pipeline (direct exchange).

The deployment is the same three capabilities (parse → chunk → embed), but the
**path is decided per job**, not by the topology. Each worker binds its queue to
its own *worker-type id* on a ``direct`` exchange and publishes via a
``RoutingPlanRouter``, which reads the ``RoutingPlan`` the submitter put in
``job_parameters`` and forwards the result to whichever worker the plan names
next. See ``examples/routed/publish_routed.py``.
"""

from examples.routed.workers import (
    DocumentParserWorker,
    EmbeddingGeneratorWorker,
    TextChunkerWorker,
)
from warren.common import MessageConsumerInterface
from warren.pubsub.rabbitmq.config import RMQExchangeConfig
from warren.pubsub.routing import RoutingPlanRouter
from warren.runtime.spec import (
    PipelineSpec,
    PublishSpec,
    WorkerFactoryContext,
    WorkerSpec,
)


async def _create_parser(ctx: WorkerFactoryContext) -> MessageConsumerInterface:
    return DocumentParserWorker(
        worker_name=ctx.worker_name,
        write_store=ctx.stores["write"],
        accepts=ctx.accepts,
        produces=ctx.produces,
        worker_type=ctx.worker_type,
    )


async def _create_chunker(ctx: WorkerFactoryContext) -> MessageConsumerInterface:
    return TextChunkerWorker(
        worker_name=ctx.worker_name,
        read_store=ctx.stores["read"],
        write_store=ctx.stores["write"],
        accepts=ctx.accepts,
        produces=ctx.produces,
        worker_type=ctx.worker_type,
    )


async def _create_embedder(ctx: WorkerFactoryContext) -> MessageConsumerInterface:
    return EmbeddingGeneratorWorker(
        worker_name=ctx.worker_name,
        read_store=ctx.stores["read"],
        write_store=ctx.stores["write"],
        accepts=ctx.accepts,
        produces=ctx.produces,
        worker_type=ctx.worker_type,
    )


def _addressed(binding_key: str) -> dict:
    """Common wiring: bind to own worker-id, publish via the routing plan."""
    return {
        "binding_key": binding_key,
        "publish": PublishSpec(route_func=RoutingPlanRouter()),
    }


PIPELINE: PipelineSpec = PipelineSpec(
    exchange=RMQExchangeConfig(name="route", type="direct"),
    workers={
        "document_parser": WorkerSpec(
            collections={"write": "parsed_documents"},
            factory=_create_parser,
            accepts=frozenset({"pdf_document"}),
            produces="markdown_document",
            **_addressed("document_parser"),
        ),
        "text_chunker": WorkerSpec(
            collections={"read": "parsed_documents", "write": "chunks"},
            factory=_create_chunker,
            accepts=frozenset({"markdown_document"}),
            produces="text_chunks",
            **_addressed("text_chunker"),
        ),
        "embedding_generator": WorkerSpec(
            collections={"read": "chunks", "write": "embeddings"},
            factory=_create_embedder,
            accepts=frozenset({"text_chunks"}),
            produces="embedded_document",
            **_addressed("embedding_generator"),
        ),
    },
    result_collections=["parsed_documents", "chunks", "embeddings"],
    reference_collection="chunks",
    completion_collection="embeddings",
    final_data_type="embedded_document",
)
