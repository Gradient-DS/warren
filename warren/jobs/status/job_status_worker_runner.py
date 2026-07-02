"""
Runner for the job status worker.

Manages the JobStatusWorker lifecycle: creates infrastructure, builds
stores and consumer, observes all messages on the fanout exchange and
records per-document processing results.

Accepts ``RuntimeConfig`` and manages its own infrastructure. Custom
components can be injected to override the defaults (e.g. for testing).
"""

from basics.logging_utils import summarize_exception_chain

from warren.common import MessageConsumerInterface
from warren.jobs.status.job_status_worker import (
    JobStatusWorker,
)
from warren.pubsub.common import (
    ConsumerManagerInterface,
    PublisherInterface,
)
from warren.pubsub.rabbitmq.config import (
    RMQExchangeConfig,
)
from warren.pubsub.routing import (
    observer_binding_key,
    observer_route_func,
)
from warren.runtime import backends
from warren.runtime.config import RuntimeConfig
from warren.runtime.infrastructure import (
    RuntimeInfra,
    close_runtime_infrastructure,
    create_runtime_infrastructure,
)
from warren.storage.job_results.interface import (
    JobResultsStoreInterface,
)
from warren.storage.job_results.mongodb import (
    MongoDBJobResultsStore,
)
from warren.storage.jobs.interface import (
    JobStoreInterface,
)
from warren.storage.jobs.mongodb import (
    MongoDBJobStore,
)
from warren.workers.runners import (
    ConsumerManagerFactory,
    WorkerRunnerBase,
)


JOB_STATUS_WORKER_TYPE: str = "job_status"


class JobStatusWorkerRunner(WorkerRunnerBase):
    """Runs a JobStatusWorker with its lifecycle hooks.

    Creates infrastructure from ``RuntimeConfig`` and builds default
    components in ``setup()``. Inject custom components to override
    defaults (e.g. for testing).

    :param config: runtime infrastructure configuration.
    :param worker_name: unique identifier for this worker instance.
    :param job_store: optional override for the job definitions store.
        Default: ``MongoDBJobStore``.
    :param job_results_store: optional override for the per-document
        results store. Default: ``MongoDBJobResultsStore``.
    :param consumer_manager_factory: optional override for the consumer
        manager factory. Default: factory creating ``RMQConsumerManager``
        on the job status queue with a publisher for the
        ``job-completed`` signal.
    """

    def __init__(
        self,
        config: RuntimeConfig,
        worker_name: str,
        *,
        exchange: RMQExchangeConfig,
        job_store: JobStoreInterface | None = None,
        job_results_store: JobResultsStoreInterface | None = None,
        consumer_manager_factory: ConsumerManagerFactory | None = None,
    ) -> None:
        super().__init__(name=worker_name)
        self._worker_name = worker_name
        self._config = config
        self._exchange = exchange
        self._job_store = job_store
        self._job_results_store = job_results_store
        self._consumer_manager_factory = consumer_manager_factory
        self._infra: RuntimeInfra | None = None
        self._publisher: PublisherInterface | None = None

    async def setup(self) -> None:
        """Create infrastructure, build defaults, wire the worker.

        1. Create infrastructure (MongoDB, Redis, RabbitMQ)
        2. Build default stores and consumer factory for any not injected
        3. Create the JobStatusWorker
        4. Create and set up the consumer manager
        """
        with self._exception_wrapping("Infrastructure setup (RabbitMQ/MongoDB/Redis)"):
            self._infra = await create_runtime_infrastructure(self._config)

        if self._job_store is None:
            with self._exception_wrapping("Job store creation"):
                self._job_store = await self._create_default_job_store()

        if self._job_results_store is None:
            with self._exception_wrapping("Job results store creation"):
                self._job_results_store = await self._create_default_job_results_store()

        if self._consumer_manager_factory is None:
            self._publisher = self._create_default_publisher()
            self._consumer_manager_factory = self._create_default_consumer_factory()

        worker = JobStatusWorker(
            worker_name=self._worker_name,
            job_store=self._job_store,
            job_results_store=self._job_results_store,
        )

        with self._exception_wrapping("Consumer manager setup"):
            self._consumer_manager = self._consumer_manager_factory(worker)
            await self._consumer_manager.setup()
        self._mark_setup_succeeded()

    async def _on_teardown(self) -> None:
        if self._publisher is not None:
            try:
                await self._publisher.teardown()
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

    async def _create_default_job_store(self) -> JobStoreInterface:
        store = MongoDBJobStore(
            client=self._infra.mongo_client,
            database_name=self._config.mongodb.database,
        )
        await store.setup()
        return store

    async def _create_default_job_results_store(
        self,
    ) -> JobResultsStoreInterface:
        store = MongoDBJobResultsStore(
            client=self._infra.mongo_client,
            database_name=self._config.mongodb.database,
        )
        await store.setup()
        return store

    def _create_default_publisher(self) -> PublisherInterface:
        return backends.create_publisher(
            self._config,
            self._infra.pubsub_connection_manager,
            exchange=self._exchange,
            route_func=observer_route_func(self._exchange),
        )

    def _create_default_consumer_factory(self) -> ConsumerManagerFactory:
        def factory(
            consumer: MessageConsumerInterface,
        ) -> ConsumerManagerInterface:
            return backends.create_consumer_manager(
                self._config,
                self._infra.pubsub_connection_manager,
                exchange=self._exchange,
                worker_type=JOB_STATUS_WORKER_TYPE,
                binding_key=observer_binding_key(self._exchange),
                consumer=consumer,
                data_publisher=self._publisher,
                publish_hard_failures=False,
            )

        return factory
