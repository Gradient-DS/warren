"""
Runner for the retry worker.

Manages the RetryWorker lifecycle: creates infrastructure connections,
builds the retry store and publisher, sets up the consumer, schedules
pending retries on startup, and cancels timers on shutdown.

Accepts ``RuntimeConfig`` and manages its own infrastructure. Custom
components can be injected to override the defaults (e.g. for testing).
"""

from collections.abc import Callable

from basics.logging_utils import summarize_exception_chain

from warren.common import MessageConsumerInterface
from warren.pubsub.common import (
    ConsumerManagerInterface,
    PublisherInterface,
)
from warren.pubsub.rabbitmq.aio_pika.consumer import (
    RMQConsumerManager,
)
from warren.pubsub.rabbitmq.aio_pika.publisher import (
    RMQPublisher,
)
from warren.pubsub.rabbitmq.config import (
    RMQConsumerConfig,
    RMQConsumerManagerConfig,
    RMQExchangeConfig,
    RMQQueueConfig,
)
from warren.retry_management.retry_worker import (
    RetryWorker,
)
from warren.runtime.config import RuntimeConfig
from warren.runtime.infrastructure import (
    RuntimeInfra,
    close_runtime_infrastructure,
    create_runtime_infrastructure,
)
from warren.storage.cache.redis import RedisDictCache
from warren.storage.cached_document_store import (
    CachedDocumentStore,
)
from warren.storage.document_store.interface import (
    DocumentStoreInterface,
)
from warren.storage.document_store.mongodb import (
    MongoDBDocumentStore,
)
from warren.workers.runners import (
    ConsumerManagerFactory,
    WorkerRunnerBase,
)


RETRY_WORKER_TYPE: str = "retry_worker"


class RetryWorkerRunner(WorkerRunnerBase):
    """Runs a RetryWorker with its lifecycle hooks.

    Creates infrastructure from ``RuntimeConfig`` and builds default
    components in ``setup()``. Inject custom components to override
    defaults (e.g. for testing).

    :param config: runtime infrastructure configuration.
    :param worker_name: unique identifier for this retry worker.
    :param retry_store: optional override for the retry envelope store.
        Default: ``CachedDocumentStore`` wrapping MongoDB + Redis.
    :param republish_publisher: optional override for the exchange
        publisher. Default: ``RMQPublisher`` targeting the processing
        exchange.
    :param consumer_manager_factory: optional override for the consumer
        manager factory. Default: factory creating ``RMQConsumerManager``
        on the retry queue.
    :param message_key_func: optional function to extract a composite
        key from a message dict. Passed to ``RetryWorker``.
    """

    def __init__(
        self,
        config: RuntimeConfig,
        worker_name: str,
        *,
        retry_store: DocumentStoreInterface | None = None,
        republish_publisher: PublisherInterface | None = None,
        consumer_manager_factory: ConsumerManagerFactory | None = None,
        message_key_func: Callable[[dict], str] | None = None,
    ) -> None:
        super().__init__(name=worker_name)
        self._worker_name = worker_name
        self._config = config
        self._retry_store = retry_store
        self._republish_publisher = republish_publisher
        self._consumer_manager_factory = consumer_manager_factory
        self._message_key_func = message_key_func
        self._infra: RuntimeInfra | None = None
        self._retry_worker: RetryWorker | None = None

    async def setup(self) -> None:
        """Create infrastructure, build defaults, wire the worker.

        1. Create infrastructure (MongoDB, Redis, RabbitMQ)
        2. Build default retry store, publisher, consumer factory
           for any components not injected
        3. Set up the republish publisher
        4. Create the RetryWorker
        5. Create and set up the consumer manager
        6. Schedule pending retries from the store
        """
        with self._exception_wrapping("Infrastructure setup (RabbitMQ/MongoDB/Redis)"):
            self._infra = await create_runtime_infrastructure(self._config)

        if self._retry_store is None:
            with self._exception_wrapping("Retry store creation"):
                self._retry_store = await self._create_default_retry_store()

        if self._republish_publisher is None:
            self._republish_publisher = self._create_default_publisher()

        if self._consumer_manager_factory is None:
            self._consumer_manager_factory = self._create_default_consumer_factory()

        with self._exception_wrapping("Republish publisher setup"):
            await self._republish_publisher.setup()

        self._retry_worker = RetryWorker(
            worker_name=self._worker_name,
            retry_store=self._retry_store,
            republish_publisher=self._republish_publisher,
            message_key_func=self._message_key_func,
        )

        with self._exception_wrapping("Consumer manager setup"):
            self._consumer_manager = self._consumer_manager_factory(
                self._retry_worker,
            )
            await self._consumer_manager.setup()

        with self._exception_wrapping("Scheduling pending retries"):
            await self._retry_worker.schedule_pending()
        self._mark_setup_succeeded()

    async def _on_teardown(self) -> None:
        if self._retry_worker is not None:
            try:
                await self._retry_worker.shutdown()
            except Exception as exc:
                self._log.warning(
                    f"Retry worker shutdown failed: {summarize_exception_chain(exc)}"
                )

        if self._republish_publisher is not None:
            try:
                await self._republish_publisher.teardown()
            except Exception as exc:
                self._log.warning(
                    f"Publisher teardown failed: {summarize_exception_chain(exc)}"
                )

        if self._infra is not None:
            try:
                await close_runtime_infrastructure(self._infra)
            except Exception as exc:
                self._log.warning(
                    f"Infrastructure teardown failed: {summarize_exception_chain(exc)}"
                )

    async def _create_default_retry_store(self) -> DocumentStoreInterface:
        retry_cfg = self._config.retry
        mongo_store = MongoDBDocumentStore(
            client=self._infra.mongo_client,
            database_name=self._config.mongodb.database,
            collection_name=retry_cfg.collection_name,
            doc_id_field=RetryWorker.REQUIRED_DOC_ID_FIELD,
        )
        await mongo_store.setup()

        cache = RedisDictCache(
            client=self._infra.redis_client,
            base_key=f"retry:{retry_cfg.collection_name}",
        )
        return CachedDocumentStore(mongo_store, cache)

    def _create_default_publisher(self) -> PublisherInterface:
        exchange_cfg = self._config.rabbitmq.exchange
        return RMQPublisher(
            connection_manager=self._infra.rmq_connection_manager,
            exchange_config=RMQExchangeConfig(
                name=exchange_cfg.name,
                type=exchange_cfg.type,
                durable=exchange_cfg.durable,
            ),
        )

    def _create_default_consumer_factory(self) -> ConsumerManagerFactory:
        exchange_cfg = self._config.rabbitmq.exchange
        consumer_cfg = self._config.rabbitmq.consumer

        manager_config = RMQConsumerManagerConfig(
            exchange=RMQExchangeConfig(
                name=exchange_cfg.name,
                type=exchange_cfg.type,
                durable=exchange_cfg.durable,
            ),
            queue=RMQQueueConfig(
                name=f"{exchange_cfg.name}.{RETRY_WORKER_TYPE}",
                durable=True,
            ),
            consumer=RMQConsumerConfig(
                prefetch_count=consumer_cfg.prefetch_count,
                on_shutdown_timeout=consumer_cfg.on_shutdown_timeout,
            ),
        )

        def factory(
            consumer: MessageConsumerInterface,
        ) -> ConsumerManagerInterface:
            return RMQConsumerManager(
                config=manager_config,
                connection_manager=self._infra.rmq_connection_manager,
                consumer=consumer,
            )

        return factory
