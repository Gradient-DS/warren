"""
Default worker runner for the distributed processing framework.

``DefaultWorkerRunner`` wires the pubsub backend (RabbitMQ or Kafka,
selected by ``config.backend`` via :mod:`warren.runtime.backends`),
MongoDB, and Redis for any worker type defined in a ``PipelineSpec``.
All worker-specific decisions are driven by the ``WorkerSpec`` — no
branching on worker type or backend name.

Subclasses can override ``_wrap_worker()`` to intercept the worker
after factory creation (e.g. for failure injection in tests), or
``_create_resolvers()`` to customize document resolution strategies.
"""

from typing import TYPE_CHECKING, cast

import logging
import os
from functools import partial

from basics.logging import get_logger
from basics.logging_utils import summarize_exception_chain

from warren.common import MessageConsumerInterface
from warren.pubsub.common import (
    ConsumerManagerInterface,
    PublisherInterface,
)
from warren.pubsub.rabbitmq.config import RMQExchangeConfig
from warren.pubsub.routing import observer_exchange, observer_route_func
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
    UnknownLocationTypeError,
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


if TYPE_CHECKING:
    from warren.storage.documents.location import DocumentCloudLocation


module_logger: logging.Logger = get_logger(__name__)


async def _resolve_cloud(
    location: object,
    *,
    by_provider: dict[str, ResolveDocumentFunc],
) -> bytes:
    """Dispatch a cloud document location to the appropriate provider resolver.

    :param location: A DocumentCloudLocation whose ``provider`` field
        identifies the target backend.
    :param by_provider: Mapping of provider name to resolver function.
    :return: Raw document bytes from the resolved provider.
    :raises UnknownLocationTypeError: If no resolver is registered for
        the location's provider.
    """
    cloud_location = cast("DocumentCloudLocation", location)
    resolver = by_provider.get(cloud_location.provider)
    if resolver is None:
        msg = (
            f"No cloud resolver registered for provider '{cloud_location.provider}'. "
            f"Registered providers: {list(by_provider)}"
        )
        raise UnknownLocationTypeError(msg)
    return await resolver(cloud_location)


class DefaultWorkerRunner(WorkerRunnerBase):
    """Concrete runner that wires RMQ + MongoDB + Redis for a worker.

    All worker-specific decisions are driven by the ``WorkerSpec`` —
    no branching on worker type name.

    Creates infrastructure from ``RuntimeConfig`` and builds default
    components in ``setup()``. Inject custom components to override
    defaults (e.g. for testing).

    :param config: runtime infrastructure configuration.
    :param worker_name: unique worker instance identifier.
    :param worker_type: name of the worker type (used for queue naming).
    :param worker_spec: spec defining collections, factory, exchange
        wiring (binding_key, publish), and flags.
    :param exchange: the pipeline's exchange (from ``PipelineSpec.exchange``)
        this worker consumes from and publishes to.
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
        exchange: RMQExchangeConfig,
        document_fetcher: GetDocumentFunc | None = None,
        document_store: DocumentStoreInterface | None = None,
        results_stores: dict[str, ResultsStoreInterface] | None = None,
    ) -> None:
        super().__init__(name=worker_name)
        self._worker_type = worker_type
        self._worker_name = worker_name
        self._config = config
        self._worker_spec: WorkerSpec = worker_spec
        self._exchange = exchange
        self._document_fetcher = document_fetcher
        self._document_store = document_store
        self._results_stores = results_stores
        self._infra: RuntimeInfra | None = None
        self._worker: MessageConsumerInterface | None = None

    async def setup(self) -> None:
        """Wire connections, stores, worker, publishers, and consumer.

        1. Set up infrastructure (MongoDB, Redis, RabbitMQ)
        2. Build default stores/fetcher for any not injected
        3. Create worker via spec's factory (async)
        4. Wrap worker (subclass hook, e.g. for failure injection)
        5. Run worker-level setup (start owned async resources)
        6. Create publishers for downstream routing
        7. Create and set up the consumer manager
        """
        with self._exception_wrapping("Infrastructure setup (RabbitMQ/MongoDB/Redis)"):
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
            self._consumer_manager = self._create_consumer_manager(
                self._worker,
                self._create_data_publisher(),
                self._create_control_publisher(),
                self._create_observer_publisher(),
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

        cloud_by_provider: dict[str, ResolveDocumentFunc] = {}

        try:
            from google.cloud.storage import Client as GCSClient

            from warren.storage.documents.resolve_gcs import (
                resolve_gcs,
            )

            cloud_by_provider["gcs"] = partial(resolve_gcs, client=GCSClient())
        except ImportError:
            # Optional 'gcs' extra. The 'cloud' resolver is only attempted,
            # never required — pipelines that never resolve cloud documents
            # run fine without it, so we degrade gracefully rather than
            # raise. Selecting a 'cloud' document location without the
            # resolver later fails with UnknownLocationTypeError.
            module_logger.debug(
                "google-cloud-storage not installed — GCS cloud resolver "
                'disabled; install with: pip install "warren[gcs]"'
            )
        except Exception as exc:
            module_logger.warning(
                "Could not initialise GCS client — GCS cloud resolver "
                f"disabled: {summarize_exception_chain(exc)}"
            )

        try:
            import boto3

            from warren.storage.documents.resolve_s3 import resolve_s3

            s3_client = boto3.client(
                "s3",
                endpoint_url=os.environ.get("S3_ENDPOINT_URL") or None,
                region_name=os.environ.get("S3_REGION") or None,
            )
            cloud_by_provider["s3"] = partial(resolve_s3, client=s3_client)
        except ImportError:
            # Optional 'S3' extra. Degrade gracefully — pipelines without S3
            # documents run fine. Selecting an S3 location without the resolver
            # later fails with UnknownLocationTypeError.
            module_logger.debug(
                "boto3 not installed — S3 cloud resolver "
                'disabled; install with: pip install "warren[s3]"'
            )
        except Exception as exc:
            module_logger.warning(
                "Could not initialise S3 client — S3 cloud resolver "
                f"disabled: {summarize_exception_chain(exc)}"
            )

        if cloud_by_provider:
            resolvers["cloud"] = partial(_resolve_cloud, by_provider=cloud_by_provider)

        try:
            import httpx

            from warren.storage.documents.resolve_http import resolve_http

            resolvers["url"] = partial(
                resolve_http, client=httpx.AsyncClient(timeout=60.0)
            )
        except ImportError:
            # Optional 'http' extra. The 'url' resolver is only attempted,
            # never required — pipelines that never resolve URL documents
            # run fine without it, so we degrade gracefully rather than
            # raise. Selecting a 'url' document location without the
            # resolver later fails with UnknownLocationTypeError.
            module_logger.debug(
                "httpx not installed — HTTP(S) URL resolver "
                'disabled; install with: pip install "warren[http]"'
            )
        except Exception as exc:
            module_logger.warning(
                "Could not initialise HTTP(S) URL resolver — "
                f"disabled: {summarize_exception_chain(exc)}"
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
            worker_type=self._worker_type,
            stores=stores,
            mongo_client=self._infra.mongo_client,
            redis_client=self._infra.redis_client,
            database_name=self._config.mongodb.database,
            accepts=self._worker_spec.accepts,
            produces=self._worker_spec.produces,
            get_document_func=get_document_func,
            document_store=document_store,
        )
        return await self._worker_spec.factory(context)

    def _create_data_publisher(self) -> PublisherInterface | None:
        """The worker's downstream publisher, or ``None`` if it's terminal."""
        publish = self._worker_spec.publish
        if publish is None:
            return None
        return backends.create_publisher(
            self._config,
            self._infra.pubsub_connection_manager,
            exchange=self._exchange,
            route=publish.route,
            route_func=publish.route_func,
        )

    def _create_control_publisher(self) -> PublisherInterface:
        """Single publisher for lifecycle envelopes (soft/hard-failure).

        Publishes to the **observer** exchange so the retry/status workers pick
        them up. For a fanout/topic pipeline the observer exchange is the data
        exchange itself; for a direct pipeline it's the derived fanout observer
        exchange.
        """
        observer = observer_exchange(self._exchange)
        return backends.create_publisher(
            self._config,
            self._infra.pubsub_connection_manager,
            exchange=observer,
            route_func=observer_route_func(observer),
        )

    def _create_observer_publisher(self) -> PublisherInterface | None:
        """Echoes successful results to the observer exchange — direct only.

        When the data exchange can't be observed in place (direct), workers also
        publish their result to the derived fanout observer exchange so the
        status worker sees every stage. For fanout/topic the data publish is
        already observable, so this returns ``None`` (no echo, no doubling).
        """
        observer = observer_exchange(self._exchange)
        if observer.name == self._exchange.name:
            return None
        return backends.create_publisher(
            self._config,
            self._infra.pubsub_connection_manager,
            exchange=observer,
            route_func=observer_route_func(observer),
        )

    def _create_consumer_manager(
        self,
        consumer: MessageConsumerInterface,
        data_publisher: PublisherInterface | None,
        control_publisher: PublisherInterface,
        observer_publisher: PublisherInterface | None,
    ) -> ConsumerManagerInterface:
        return backends.create_consumer_manager(
            self._config,
            self._infra.pubsub_connection_manager,
            exchange=self._exchange,
            worker_type=self._worker_type,
            binding_key=self._worker_spec.binding_key,
            consumer=consumer,
            data_publisher=data_publisher,
            control_publisher=control_publisher,
            observer_publisher=observer_publisher,
        )
