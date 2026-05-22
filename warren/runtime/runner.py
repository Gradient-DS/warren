"""
Default worker runner for the distributed processing framework.

``DefaultWorkerRunner`` wires RabbitMQ, MongoDB, and Redis for any
worker type defined in a ``PipelineSpec``. All worker-specific decisions
are driven by the ``WorkerSpec`` — no branching on worker type name.

Subclasses can override ``_wrap_worker()`` to intercept the worker
after factory creation (e.g. for failure injection in tests), or
``_create_resolvers()`` to customize document resolution strategies.
"""

import logging
from functools import partial

from basics.logging import get_logger

from document_processing.distributed.warren.common import MessageConsumerInterface
from document_processing.distributed.warren.pubsub.common import (
    ConsumerManagerInterface,
    PublisherInterface,
)
from document_processing.distributed.warren.pubsub.rabbitmq import (
    RMQConsumerManagerConfig,
    RMQQueueConfig,
)
from document_processing.distributed.warren.pubsub.rabbitmq.aio_pika import (
    RMQConsumerManager,
    RMQPublisher,
)
from document_processing.distributed.warren.storage.document_store import (
    DocumentStoreInterface,
    MongoDBDocumentStore,
)
from document_processing.distributed.warren.storage.documents.factories import (
    create_cached_document_fetcher,
)
from document_processing.distributed.warren.storage.documents.interface import (
    GetDocumentFunc,
    ResolveDocumentFunc,
)
from document_processing.distributed.warren.storage.documents.resolvers import (
    resolve_path,
)
from document_processing.distributed.warren.storage.results import (
    ResultsStoreInterface,
)
from document_processing.distributed.warren.storage.results.factories import (
    create_default_results_store,
)
from document_processing.distributed.warren.workers.runners import WorkerRunnerBase
from document_processing.distributed.warren.runtime.config import RuntimeConfig
from document_processing.distributed.warren.runtime.infrastructure import (
    RuntimeInfra,
    close_runtime_infrastructure,
    create_runtime_infrastructure,
)
from document_processing.distributed.warren.runtime.spec import (
    PipelineSpec,
    WorkerFactoryContext,
    WorkerSpec,
)


module_logger: logging.Logger = get_logger(__name__)


class DefaultWorkerRunner(WorkerRunnerBase):
    """Concrete runner that wires RMQ + MongoDB + Redis for a worker.

    All worker-specific decisions are driven by the ``WorkerSpec``
    from the pipeline — no branching on worker type name.

    :param worker_type: Name of the worker type (must exist in
        pipeline spec).
    :param worker_name: Unique worker instance identifier.
    :param config: Runtime infrastructure configuration.
    :param pipeline: Pipeline spec defining workers and completion
        criteria.
    """

    def __init__(
        self,
        worker_type: str,
        worker_name: str,
        config: RuntimeConfig,
        pipeline: PipelineSpec,
    ) -> None:
        super().__init__(name=worker_name)
        self._worker_type = worker_type
        self._worker_name = worker_name
        self._config = config

        if worker_type not in pipeline.workers:
            valid = ", ".join(pipeline.workers.keys())
            raise ValueError(
                f"Unknown worker type '{worker_type}'. "
                f"Valid types: {valid}"
            )

        self._worker_spec: WorkerSpec = pipeline.workers[worker_type]
        self._infra: RuntimeInfra | None = None
        self._worker: MessageConsumerInterface | None = None

    async def setup(self) -> None:
        """Wire connections, stores, worker, publishers, and consumer.

        1. Set up infrastructure (MongoDB, Redis, RabbitMQ)
        2. Create results stores from pipeline's collection config
        3. Create document fetcher (if worker spec requires it)
        4. Create document store (if worker spec requires it)
        5. Create worker via pipeline's factory (async)
        6. Wrap worker (subclass hook, e.g. for failure injection)
        7. Run worker-level setup (start owned async resources)
        8. Create publishers for downstream routing
        9. Create and set up the consumer manager
        """
        self._infra = await create_runtime_infrastructure(self._config)
        stores = await self._create_results_stores()
        document_fetcher = self._create_document_fetcher()
        document_store = await self._create_document_store()
        self._worker = await self._create_worker(
            stores,
            document_fetcher,
            document_store,
        )
        self._worker = self._wrap_worker(self._worker, self._worker_spec)
        await self._worker.setup()
        publishers = self._create_publishers()
        self._consumer_manager = self._create_consumer_manager(
            self._worker,
            publishers,
        )
        await self._consumer_manager.setup()
        self._mark_setup_succeeded()

    async def _on_teardown(self) -> None:
        if self._worker is not None:
            try:
                await self._worker.teardown()
            except Exception as exc:
                self._log.warning(
                    f"Worker teardown failed: {type(exc).__name__}: {exc}"
                )
        if self._infra is not None:
            await close_runtime_infrastructure(self._infra)

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

            from document_processing.distributed.warren.storage.documents.resolve_gcs import (
                resolve_gcs,
            )

            resolvers["cloud"] = partial(resolve_gcs, client=GCSClient())
        except ImportError:
            pass
        except Exception:
            module_logger.info(
                "GCS credentials not available — 'cloud' document resolver disabled"
            )

        return resolvers

    async def _create_results_stores(
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

    async def _create_document_store(self) -> DocumentStoreInterface | None:
        """Create a MongoDBDocumentStore if the worker spec requires it."""
        if not self._worker_spec.needs_document_store:
            return None

        store = MongoDBDocumentStore(
            client=self._infra.mongo_client,
            database_name=self._config.mongodb.database,
            collection_name="documents",
            doc_id_field="doc_id",
            unique_indexes=[("doc_id",)],
        )
        await store.setup()
        return store

    def _create_document_fetcher(self) -> GetDocumentFunc | None:
        """Create a CachedDocumentFetcher if the worker spec requires it."""
        if not self._worker_spec.needs_document_fetcher:
            return None

        # TODO: wrap _create_resolvers() in try/except — it's an
        #  override hook and subclass implementations are untrusted.
        #  Deferred to B4 (exception hierarchy).
        return create_cached_document_fetcher(
            redis_client=self._infra.redis_client,
            resolvers=self._create_resolvers(),
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
            RMQPublisher(
                connection_manager=self._infra.rmq_connection_manager,
                exchange_config=self._config.rabbitmq.exchange,
            )
        ]

    def _create_consumer_manager(
        self,
        consumer: MessageConsumerInterface,
        publishers: list[PublisherInterface],
    ) -> ConsumerManagerInterface:
        exchange_config = self._config.rabbitmq.exchange
        queue_name = f"{exchange_config.name}.{self._worker_type}"

        queue_config = RMQQueueConfig(
            name=queue_name,
            durable=True,
        )

        manager_config = RMQConsumerManagerConfig(
            exchange=exchange_config,
            queue=queue_config,
            consumer=self._config.rabbitmq.consumer,
        )

        return RMQConsumerManager(
            config=manager_config,
            connection_manager=self._infra.rmq_connection_manager,
            consumer=consumer,
            publishers=publishers,
        )
