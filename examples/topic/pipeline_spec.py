"""
Topic-exchange variant of the fake pipeline.

Same three workers as ``examples/fake`` (parse → chunk → embed), but on a
**topic** exchange: instead of broadcasting every message to every worker,
the broker routes each message by its ``data_type`` to the worker that binds
that key. Each worker:

- binds its queue to the ``data_type`` it consumes (``binding_key``), and
- publishes with ``MessageFieldRouter`` so the routing key is the produced
  ``data_type``.

``should_process`` still guards each worker (defense in depth), but with topic
routing the broker has already delivered only matching messages.
"""

from examples.fake.pipeline_spec import (
    _create_document_parser,
    _create_embedding_generator,
    _create_text_chunker,
)
from warren.pubsub.rabbitmq.config import RMQExchangeConfig
from warren.pubsub.routing import MessageFieldRouter
from warren.runtime.spec import (
    PipelineSpec,
    PublishSpec,
    WorkerSpec,
)


PIPELINE: PipelineSpec = PipelineSpec(
    exchanges={"docs": RMQExchangeConfig(name="docs", type="topic")},
    default_exchange="docs",
    workers={
        "document_parser": WorkerSpec(
            collections={"write": "parsed_documents"},
            factory=_create_document_parser,
            consume_exchange="docs",
            binding_key="pdf_document",
            publish=[PublishSpec(exchange="docs", route_func=MessageFieldRouter())],
        ),
        "text_chunker": WorkerSpec(
            collections={"read": "parsed_documents", "write": "chunks"},
            factory=_create_text_chunker,
            consume_exchange="docs",
            binding_key="markdown_document",
            publish=[PublishSpec(exchange="docs", route_func=MessageFieldRouter())],
        ),
        "embedding_generator": WorkerSpec(
            collections={"read": "chunks", "write": "embeddings"},
            factory=_create_embedding_generator,
            consume_exchange="docs",
            binding_key="text_chunks",
            publish=[PublishSpec(exchange="docs", route_func=MessageFieldRouter())],
        ),
    },
    result_collections=["parsed_documents", "chunks", "embeddings"],
    reference_collection="chunks",
    completion_collection="embeddings",
    final_data_type="embedded_document",
)
