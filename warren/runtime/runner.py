"""
Default worker runner for the distributed processing framework.

``DefaultWorkerRunner`` wires the configured pubsub backend (RabbitMQ or
Kafka), MongoDB, and Redis for any worker type defined in a
``PipelineSpec``. The backend is selected by ``config.backend`` and the
publisher / consumer manager are built through
:mod:`warren.runtime.backends`, so the runner never branches on backend.
All worker-specific decisions are driven by the ``WorkerSpec`` — no
branching on worker type name.

Subclasses can override ``_wrap_worker()`` to intercept the worker
after factory creation (e.g. for failure injection in tests), or
``_create_resolvers()`` to customize document resolution strategies.
"""

import logging
from functools import partial

from basics.logging import get_logger
from basics.logging_utils import summarize_exception_chain

from warren.common import MessageConsumerInterface
from warren.pubsub.common import (
    ConsumerManagerInterface,
    PublisherInterface,
)
from warren.runtime import backends
from warren.runtime.config import RuntimeConfig
from warren.runtime.infrastructure import (
    RuntimeInfra,
    close_runtime_infrastructure,
    create_runtime_infrastructure,
)
from warren.runtime.spec import (
    WorkerFactoryContext,
    WorkerSpec,
)
from warren.storage.document_store import (
    DocumentStoreInterface,
    MongoDBDocumentStore,
)
from warren.storage.documents.factories import (
    create_cached_document_fetcher,
)
from warren.storage.documents.interface import (
    GetDocumentFunc,
    ResolveDocumentFunc,
)
from warren.storage.documents.resolvers import (
    resolve_path,
)
from warren.storage.results import (
    ResultsStoreInterface,
)
from warren.storage.results.factories import (
    create_default_results_store,
)
from warren.workers.runners import WorkerRunnerBase


module_logger: logging.Logger = get_logger(__name__)


class DefaultWorkerRunner(WorkerRunnerBase):
    """Concrete runner that wires pubsub + MongoDB + Redis for a worker.

    The pubsub backend (RabbitMQ or Kafka) is selected by
    ``config.backend`` via :mod:`warren.runtime.backends`. All
    worker-specific decisions are driven by the ``WorkerSpec`` — no
    branching on worker type or backend name.

    Creates infrastructure from ``RuntimeConfig`` and builds default
    components in ``setup()``. Inject custom components to override
    defaults (e.g. for testing).

    :param config: runtime infrastructure configuration.
    :param worker_name: unique worker instance identifier.
    :param worker_type: name of the worker type (used for queue/group
        naming).
    :param worker_spec: spec defining collections, factory, and flags.
    :param document_fetcher: optional override for the document fetcher.
        Default: ``CachedDocumentFetcher`` with path + GCS resolvers
        (created when ``worker_spec.needs_document_fetcher`` is True).
    :param document_store: optional override for the document store.
        Default: ``MongoDBDocumentStore`` on the ``documents``
        collection (created when ``worker_spec.needs_document_store``
        is True).
    :param results_stores: optional override for the results stores.
        Default: one ``DefaultResultsStore`` per collection in
        ``worker_spec.collections``.
    """

    def __init__(
        self,
        config: RuntimeConfig,
        worker_name: str,
        *,
        worker_type: str,
        worker_spec: WorkerSpec,
        document_fetcher: GetDocumentFunc | None = None,
        document_store: DocumentStoreInterface | None = None,
        results_stores: dict[str, ResultsStoreInterface] | None = None,
    ) -> None:
        super().__init__(name=worker_name)
        self._worker_type = worker_type
        self._worker_name = worker_name
        self._config = config
        self._worker_spec: WorkerSpec = worker_spec
        self._document_fetcher = document_fetcher
        self._document_store = document_store
        self._results_stores = results_stores
        self._infra: RuntimeInfra | None = None
        self._worker: MessageConsumerInterface | None = None

    async def setup(self) -> None:
        """Wire connections, stores, worker, publishers, and consumer.

        1. Set up infrastructure (MongoDB, Redis, pubsub backend)
        2. Build default stores/fetcher for any not injected
        3. Create worker via spec's factory (async)
        4. Wrap worker (subclass hook, e.g. for failure injection)
        5. Run worker-level setup (start owned async resources)
        6. Create publishers for downstream routing
        7. Create and set up the consumer manager
        """
        with self._exception_wrapping("Infrastructure setup (pubsub/MongoDB/Redis)"):
            self._infra = await create_runtime_infrastructure(self._config)

        if self._results_stores is None:
            with self._exception_wrapping("Results store creation"):
                self._results_stores = await self._create_default_results_stores()

        if self._document_fetcher is None and self._worker_spec.needs_document_fetcher:
            with self._exception_wrapping("Document fetcher creation"):
                self._document_fetcher = self._create_default_document_fetcher()

        if self._document_store is None and self._worker_spec.needs_document_store:
            with self._exception_wrapping("Document store creation"):
                self._document_store = await self._create_default_document_store()

        with self._exception_wrapping("Worker creation"):
            self._worker = await self._create_worker(
                self._results_stores,
                self._document_fetcher,
                self._document_store,
            )
        with self._exception_wrapping("Worker wrapping"):
            self._worker = self._wrap_worker(self._worker, self._worker_spec)
        with self._exception_wrapping("Worker setup"):
            await self._worker.setup()

        with self._exception_wrapping("Consumer manager creation"):
            publishers = self._create_publishers()
            self._consumer_manager = self._create_consumer_manager(
                self._worker,
                publishers,
            )
        with self._exception_wrapping("Consumer manager setup"):
            await self._consumer_manager.setup()

        self._mark_setup_succeeded()

    async def _on_teardown(self) -> None:
        if self._worker is not None:
            try:
                await self._worker.teardown()
            except Exception as exc:
                self._log.warning(
                    f"Worker teardown failed: {summarize_exception_chain(exc)}"
                )

        if self._infra is not None:
            try:
                await close_runtime_infrastructure(self._infra)
            except Exception as exc:
                self._log.warning(
                    f"Infrastructure teardown failed: {summarize_exception_chain(exc)}"
                )

    def _wrap_worker(
        self,
        worker: MessageConsumerInterface,
        spec: WorkerSpec,
    ) -> MessageConsumerInterface:
        """Hook for subclasses to wrap the worker after factory creation.

        Called between factory invocation and ``worker.setup()``.
        Default returns the worker unchanged.

        :param worker: The worker created by the spec's factory.
        :param spec: The worker spec (subclasses may check for
            extended spec types).
        :return: The worker to use (possibly wrapped).
        """
        return worker

    def _create_resolvers(self) -> dict[str, ResolveDocumentFunc]:
        """Build the document resolver registry.

        Override to add or replace resolvers (e.g. for custom cloud
        storage backends).

        :return: Mapping of location type to resolver function.
        """
        resolvers: dict[str, ResolveDocumentFunc] = {"path": resolve_path}

        try:
            from google.cloud.storage import Client as GCSClient

            from warren.storage.documents.resolve_gcs import (
                resolve_gcs,
            )

            resolvers["cloud"] = partial(resolve_gcs, client=GCSClient())
        except ImportError:
            # Optional 'gcs' extra. The 'cloud' resolver is only attempted,
            # never required — pipelines that never resolve cloud documents
            # run fine without it, so we degrade gracefully rather than
            # raise. Selecting a 'cloud' document location without the
            # resolver later fails with UnknownLocationTypeError.
            module_logger.debug(
                "google-cloud-storage not installed — 'cloud' document "
                'resolver disabled; install with: pip install "warren[gcs]"'
            )
        except Exception as exc:
            module_logger.warning(
                "Could not initialise GCS client — 'cloud' document "
                f"resolver disabled: {summarize_exception_chain(exc)}"
            )

        return resolvers

    async def _create_default_results_stores(
        self,
    ) -> dict[str, ResultsStoreInterface]:
        stores: dict[str, ResultsStoreInterface] = {}

        for role, collection_name in self._worker_spec.collections.items():
            stores[role] = await create_default_results_store(
                collection_name=collection_name,
                mongo_client=self._infra.mongo_client,
                redis_client=self._infra.redis_client,
                database_name=self._config.mongodb.database,
            )

        return stores

    async def _create_default_document_store(self) -> DocumentStoreInterface:
        """Create a MongoDBDocumentStore for the ``documents`` collection."""
        store = MongoDBDocumentStore(
            client=self._infra.mongo_client,
            database_name=self._config.mongodb.database,
            collection_name="documents",
            doc_id_field="doc_id",
            unique_indexes=[("doc_id",)],
        )
        await store.setup()
        return store

    def _create_default_document_fetcher(self) -> GetDocumentFunc:
        """Create a CachedDocumentFetcher with path + GCS resolvers."""
        with self._exception_wrapping("Creation of resolvers"):
            resolvers = self._create_resolvers()
        return create_cached_document_fetcher(
            redis_client=self._infra.redis_client,
            resolvers=resolvers,
        )

    async def _create_worker(
        self,
        stores: dict[str, ResultsStoreInterface],
        get_document_func: GetDocumentFunc | None,
        document_store: DocumentStoreInterface | None,
    ) -> MessageConsumerInterface:
        context = WorkerFactoryContext(
            worker_name=self._worker_name,
            stores=stores,
            mongo_client=self._infra.mongo_client,
            redis_client=self._infra.redis_client,
            database_name=self._config.mongodb.database,
            get_document_func=get_document_func,
            document_store=document_store,
        )
        return await self._worker_spec.factory(context)

    def _create_publishers(self) -> list[PublisherInterface]:
        if self._worker_spec.terminal:
            return []

        return [
            backends.create_publisher(
                self._config,
                self._infra.pubsub_connection_manager,
            )
        ]

    def _create_consumer_manager(
        self,
        consumer: MessageConsumerInterface,
        publishers: list[PublisherInterface],
    ) -> ConsumerManagerInterface:
        return backends.create_consumer_manager(
            self._config,
            self._infra.pubsub_connection_manager,
            worker_type=self._worker_type,
            consumer=consumer,
            publishers=publishers,
        )
