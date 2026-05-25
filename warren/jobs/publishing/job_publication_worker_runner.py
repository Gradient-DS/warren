"""
Runner for the job publication worker.

Manages the JobPublicationWorker lifecycle: creates infrastructure,
wires the consumer, and delegates document publishing to an
application-specific ``JobDocumentsPublisher``.

Accepts ``RuntimeConfig`` and manages its own infrastructure. The
documents publisher is created via an injected factory that receives
the runner's shared RMQ publisher and infrastructure connections,
so the application can build its stores from the same MongoDB client
and publish through the same RMQ channel.
"""

from typing import Optional, Dict, Callable, AsyncIterable

from abc import abstractmethod

from document_processing.distributed.warren.common import MessageConsumerInterface
from document_processing.distributed.warren.jobs.publishing.job_documents_publisher import (
    JobDocumentsPublisher,
)
from document_processing.distributed.warren.jobs.publishing.job_publication_worker import (
    JobPublicationWorker,
)
from document_processing.distributed.warren.pubsub.common import (
    ConsumerManagerInterface,
    PublisherInterface,
)
from document_processing.distributed.warren.pubsub.rabbitmq.config import (
    RMQConsumerConfig,
    RMQConsumerManagerConfig,
    RMQExchangeConfig,
    RMQQueueConfig,
)
from document_processing.distributed.warren.pubsub.rabbitmq.aio_pika.consumer import (
    RMQConsumerManager,
)
from document_processing.distributed.warren.pubsub.rabbitmq.aio_pika.publisher import (
    RMQPublisher,
)
from document_processing.distributed.warren.runtime.config import RuntimeConfig
from document_processing.distributed.warren.runtime.infrastructure import (
    RuntimeInfra,
    close_runtime_infrastructure,
    create_runtime_infrastructure,
)
from document_processing.distributed.warren.workers.runners import (
    ConsumerManagerFactory,
    WorkerRunnerBase,
)

PUBLICATION_WORKER_TYPE: str = "publication"


class DocumentsPublisherFactoryFunc:
    """Factory protocol for creating application-specific publishers.

    Called by ``JobPublicationWorkerRunner.setup()`` after infrastructure
    is created. The factory receives the runner's shared resources and
    is responsible for creating all components the publisher needs
    (stores, adapters, etc.).

    :param publisher: RMQ publisher for downstream messages, shared
        with the consumer manager. Already created but not yet set up
        — setup happens when the consumer manager starts.
    :param infra: runtime connections (MongoDB, Redis, RabbitMQ).
        Use ``infra.mongo_client`` to create stores that share the
        runner's connection rather than opening a second one.
    :param config: runtime configuration. Use
        ``config.mongodb.database`` for store database names and any
        other infrastructure settings the publisher needs.
    :param worker_name: unique worker instance name, useful for
        naming the publisher and log context.
    """

    @abstractmethod
    async def __call__(
        self,
        publisher: PublisherInterface,
        infra: RuntimeInfra,
        config: RuntimeConfig,
        worker_name: str,
    ) -> JobDocumentsPublisher: ...


class JobPublicationWorkerRunner(WorkerRunnerBase):
    """Runs a JobPublicationWorker with its lifecycle hooks.

    Creates infrastructure from ``RuntimeConfig``, then calls the
    injected ``documents_publisher_factory`` to build the
    application-specific publisher. The factory receives the runner's
    shared RMQ publisher and infrastructure so it can create stores
    from the same connections.

    :param config: runtime infrastructure configuration.
    :param worker_name: unique identifier for this worker instance.
    :param documents_publisher_factory: async callable matching
        ``DocumentsPublisherFactoryFunc``. Called in ``setup()``
        after infrastructure is created.
    :param consumer_manager_factory: optional override for the consumer
        manager factory. Default: factory creating ``RMQConsumerManager``
        on the publication queue.
    :param create_source_generator: optional callable that receives
        the message ``data`` dict and returns an ``AsyncIterable``
        of document sources. When ``None``, defaults to iterating
        ``data["items"]``.
    """

    def __init__(
        self,
        config: RuntimeConfig,
        worker_name: str,
        *,
        documents_publisher_factory: DocumentsPublisherFactoryFunc,
        consumer_manager_factory: ConsumerManagerFactory | None = None,
        create_source_generator: Optional[
            Callable[[Dict], AsyncIterable]
        ] = None,
    ) -> None:
        super().__init__(name=worker_name)
        self._worker_name = worker_name
        self._config = config
        self._documents_publisher_factory = documents_publisher_factory
        self._consumer_manager_factory = consumer_manager_factory
        self._create_source_generator = create_source_generator
        self._infra: RuntimeInfra | None = None
        self._publisher: PublisherInterface | None = None
        self._documents_publisher: JobDocumentsPublisher | None = None

    async def setup(self) -> None:
        """Create infrastructure, build publisher, wire the worker.

        1. Create infrastructure (MongoDB, Redis, RabbitMQ)
        2. Create RMQ publisher for downstream messages
        3. Call documents publisher factory with shared infrastructure
        4. Build default consumer factory if not injected
        5. Create the JobPublicationWorker and consumer manager
        """
        self._infra = await create_runtime_infrastructure(self._config)
        self._publisher = self._create_default_publisher()

        self._documents_publisher = await self._documents_publisher_factory(
            self._publisher, self._infra, self._config, self._worker_name,
        )

        if self._consumer_manager_factory is None:
            self._consumer_manager_factory = (
                self._create_default_consumer_factory()
            )

        worker = JobPublicationWorker(
            self._worker_name,
            documents_publisher=self._documents_publisher,
            create_source_generator=self._create_source_generator,
        )

        self._consumer_manager = self._consumer_manager_factory(worker)
        await self._consumer_manager.setup()
        self._mark_setup_succeeded()

    async def _on_teardown(self) -> None:
        if self._publisher is not None:
            await self._publisher.teardown()

        if self._infra is not None:
            await close_runtime_infrastructure(self._infra)

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
                name=f"{exchange_cfg.name}.{PUBLICATION_WORKER_TYPE}",
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
                publishers=[self._publisher] if self._publisher else [],
                publish_hard_failures=False,
            )

        return factory
