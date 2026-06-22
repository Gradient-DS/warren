"""
Multi-exchange pipeline: a fanout pipeline + a topic event side-channel.

Two exchanges coexist:

- ``jobs`` (**fanout**) carries the main parse → chunk → embed pipeline; workers
  self-select via ``should_process`` exactly as in ``examples/fake``.
- ``events`` (**topic**) is an audit side-channel.

Each pipeline worker publishes its result to **both** exchanges at once
(multi-publish): to ``jobs`` to drive the next stage, and to ``events`` keyed by
``data_type`` (via ``MessageFieldRouter``). An ``AuditWorker`` binds ``#`` on
``events`` and records every stage event — proving a single worker publishing to
a fanout and a topic exchange simultaneously.
"""

from examples.fake.pipeline_spec import (
    _create_document_parser,
    _create_embedding_generator,
    _create_text_chunker,
)
from examples.multi_exchange.audit_worker import AuditWorker
from warren.common import MessageConsumerInterface
from warren.pubsub.rabbitmq.config import RMQExchangeConfig
from warren.pubsub.routing import MessageFieldRouter
from warren.runtime.spec import (
    PipelineSpec,
    PublishSpec,
    WorkerFactoryContext,
    WorkerSpec,
)


async def _create_audit(ctx: WorkerFactoryContext) -> MessageConsumerInterface:
    return AuditWorker(
        worker_name=ctx.worker_name,
        mongo_client=ctx.mongo_client,
        database_name=ctx.database_name,
    )


def _dual_publish() -> list[PublishSpec]:
    """Publish to the fanout pipeline AND the topic event side-channel."""
    return [
        PublishSpec(exchange="jobs"),
        PublishSpec(exchange="events", route_func=MessageFieldRouter()),
    ]


PIPELINE: PipelineSpec = PipelineSpec(
    exchanges={
        "jobs": RMQExchangeConfig(name="jobs", type="fanout"),
        "events": RMQExchangeConfig(name="events", type="topic"),
    },
    default_exchange="jobs",
    workers={
        "document_parser": WorkerSpec(
            collections={"write": "parsed_documents"},
            factory=_create_document_parser,
            consume_exchange="jobs",
            publish=_dual_publish(),
        ),
        "text_chunker": WorkerSpec(
            collections={"read": "parsed_documents", "write": "chunks"},
            factory=_create_text_chunker,
            consume_exchange="jobs",
            publish=_dual_publish(),
        ),
        "embedding_generator": WorkerSpec(
            collections={"read": "chunks", "write": "embeddings"},
            factory=_create_embedding_generator,
            consume_exchange="jobs",
            publish=_dual_publish(),
        ),
        "audit": WorkerSpec(
            collections={},
            factory=_create_audit,
            consume_exchange="events",
            binding_key="#",  # observe every event on the topic exchange
            publish=[],  # passive sidecar
        ),
    },
    result_collections=["parsed_documents", "chunks", "embeddings"],
    reference_collection="chunks",
    completion_collection="embeddings",
    final_data_type="embedded_document",
)
