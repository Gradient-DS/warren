"""
Runner for the job publication worker.

Manages the JobPublicationWorker lifecycle: creates infrastructure,
wires the consumer, and delegates document publishing to the injected
``JobDocumentsPublisher``.

Accepts ``RuntimeConfig`` and manages its own infrastructure. The
``documents_publisher`` is always required — it is application-specific
and has no sensible default.
"""

from typing import Optional, Dict, Callable, AsyncIterable

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


class JobPublicationWorkerRunner(WorkerRunnerBase):
    """Runs a JobPublicationWorker with its lifecycle hooks.

    Creates infrastructure from ``RuntimeConfig`` and builds default
    consumer factory in ``setup()``. The ``documents_publisher`` is
    always required — it carries application-specific adapters and
    stores with no sensible default.

    :param config: runtime infrastructure configuration.
    :param worker_name: unique identifier for this worker instance.
    :param documents_publisher: publisher harness for the
        load -> register -> publish flow.
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
        documents_publisher: JobDocumentsPublisher,
        consumer_manager_factory: ConsumerManagerFactory | None = None,
        create_source_generator: Optional[
            Callable[[Dict], AsyncIterable]
        ] = None,
    ) -> None:
        super().__init__(name=worker_name)
        self._worker_name = worker_name
        self._config = config
        self._documents_publisher = documents_publisher
        self._consumer_manager_factory = consumer_manager_factory
        self._create_source_generator = create_source_generator
        self._infra: RuntimeInfra | None = None
        self._publisher: PublisherInterface | None = None

    async def setup(self) -> None:
        """Create infrastructure, build defaults, wire the worker.

        1. Create infrastructure (MongoDB, Redis, RabbitMQ)
        2. Build default consumer factory if not injected
        3. Create the JobPublicationWorker
        4. Create and set up the consumer manager
        """
        self._infra = await create_runtime_infrastructure(self._config)

        if self._consumer_manager_factory is None:
            self._publisher = self._create_default_publisher()
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
