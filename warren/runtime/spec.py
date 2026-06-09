"""
Specification dataclasses for processing-worker runners.

These dataclasses are application-agnostic: they describe a worker type
in terms it can be wired by a runner (RabbitMQ, MongoDB, Redis), without
knowing what the worker does. Concrete deployments (production or test)
declare a ``PipelineSpec`` whose ``workers`` dict maps a type name to a
``WorkerSpec``; the runner consumes that to build infrastructure.

Extensions (e.g. failure injection for testing) live alongside the
pipeline definition and inherit from ``WorkerSpec`` so the runner can
treat them uniformly.
"""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from pymongo import AsyncMongoClient
from redis.asyncio import Redis

from warren.common import MessageConsumerInterface
from warren.storage.document_store.interface import (
    DocumentStoreInterface,
)
from warren.storage.documents.interface import (
    GetDocumentFunc,
)
from warren.storage.results.interface import (
    ResultsStoreInterface,
)


@dataclass(frozen=True)
class WorkerFactoryContext:
    """Bundle of resources the runner hands to each worker factory.

    Grew organically as worker kinds diversified (parser needs a
    document fetcher; web-fetch / field-extract need direct storage;
    EZ-style workers need raw Mongo/Redis clients to construct
    additional stores like ``BinaryResultsStore``). A single context
    keeps factory signatures stable as more concerns get added.

    :param worker_name: Runner-assigned unique name for this worker.
    :param stores: Pre-built ``ResultsStoreInterface`` per role (keys
        come from ``WorkerSpec.collections``). Factories that don't use
        them ignore the dict.
    :param get_document_func: Present when ``needs_document_fetcher``
        is set on the spec. ``None`` otherwise.
    :param document_store: Present when ``needs_document_store`` is
        set. ``None`` otherwise.
    :param mongo_client: Raw async Mongo client, passed through so
        factories can construct additional specialised stores
        (e.g. ``BinaryResultsStore``). Always populated by the runner.
    :param redis_client: Raw async Redis client — same rationale.
    :param database_name: Mongo database name to use for any
        factory-constructed stores.
    """

    worker_name: str
    stores: dict[str, ResultsStoreInterface]
    mongo_client: AsyncMongoClient
    redis_client: Redis
    database_name: str
    get_document_func: GetDocumentFunc | None = None
    document_store: DocumentStoreInterface | None = None


WorkerFactory = Callable[[WorkerFactoryContext], Awaitable[MessageConsumerInterface]]
"""Async factory callable: receives a context, returns a worker.

Async because factories may need to construct stores whose ``setup()``
is async (index creation, schema validation). Simple factories that
don't need async work can still be ``async def`` with no ``await``
inside.
"""


@dataclass(frozen=True)
class WorkerSpec:
    """One worker type: its collections, factory, and terminal flag.

    :param collections: Maps role ("read", "write") to MongoDB collection
        name. The runner creates a DefaultResultsStore per role.
    :param factory: Callable that creates the worker given a
        ``WorkerFactoryContext``.
    :param terminal: If True, the worker publishes nothing downstream.
    :param needs_document_fetcher: If True, the runner passes a
        GetDocumentFunc to the factory. Otherwise passes None.
    :param needs_document_store: If True, the runner passes a
        DocumentStoreInterface for direct reads/writes against the
        "documents" collection (distinct from the GetDocumentFunc path,
        which fetches bytes via location resolvers). Used by workers
        that register or update entries by ``doc_id`` (e.g. WebFetch
        writes HTML bytes; FieldExtract reads them back).
    """

    collections: dict[str, str]
    factory: WorkerFactory
    terminal: bool = False
    needs_document_fetcher: bool = False
    needs_document_store: bool = False


@dataclass(frozen=True)
class PipelineSpec:
    """Full pipeline composition: workers + completion criteria.

    :param workers: Maps worker type name to its WorkerSpec.
    :param result_collections: All MongoDB collection names to report
        in summaries (e.g., ["parsed_documents", "chunks", "embeddings"]).
    :param reference_collection: Collection whose count defines "expected"
        (e.g., "chunks").
    :param completion_collection: Collection to compare against reference
        (e.g., "embeddings"). Done when counts match and both > 0.
    :param final_data_type: The data_type that marks a document as fully
        processed (e.g., "embedded_document"). Used by the job store for
        completion detection.
    """

    workers: dict[str, WorkerSpec]
    result_collections: list[str]
    reference_collection: str
    completion_collection: str
    final_data_type: str
