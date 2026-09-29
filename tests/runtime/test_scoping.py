import asyncio

from tests.runtime.test_runner_wiring import _NoopWorker
from tests.storage.scoping_doubles import MongoClient, RedisClient
from warren.common import MessageConsumerInterface
from warren.pubsub.common import ConsumerManagerInterface, PublisherInterface
from warren.pubsub.memory.connection import MemoryConnectionManager
from warren.pubsub.memory.consumer import MemoryConsumerManager
from warren.pubsub.rabbitmq.config import RMQExchangeConfig
from warren.retry_management.retry_worker_runner import RetryWorkerRunner
from warren.runtime.config import RuntimeConfig
from warren.runtime.infrastructure import RuntimeInfra
from warren.runtime.runner import DefaultWorkerRunner
from warren.runtime.spec import WorkerFactoryContext, WorkerSpec
from warren.storage.documents.interface import ResolveDocumentFunc
from warren.storage.documents.location import DocumentLocation
from warren.storage.scoping import current_scope


class Runner(DefaultWorkerRunner):
    def _create_resolvers(self) -> dict[str, ResolveDocumentFunc]:
        async def resolve(location: DocumentLocation) -> bytes:
            return b"payload"

        return {"test": resolve}

    def _create_control_publisher(self) -> None:
        return None

    def _create_consumer_manager(
        self,
        worker: MessageConsumerInterface,
        data_publisher: PublisherInterface | None,
        control_publisher: PublisherInterface | None,
        observer_publisher: PublisherInterface | None,
    ) -> ConsumerManagerInterface:
        return MemoryConsumerManager(
            self._infra.pubsub_connection_manager,
            worker,
            exchange=self._exchange,
            queue_name="scopes.worker",
        )


def test_runtime_scoping_reaches_stores_and_factory_context() -> None:
    async def run() -> None:
        contexts: list[WorkerFactoryContext] = []

        async def factory(context: WorkerFactoryContext) -> MessageConsumerInterface:
            contexts.append(context)
            return _NoopWorker(context.worker_name)

        config = RuntimeConfig.model_validate(
            {"scoping": {"enabled": True, "database_prefix": "test_"}}
        )
        mongo, redis = MongoClient(), RedisClient()
        connection = MemoryConnectionManager()
        await connection.setup()
        infra = RuntimeInfra(mongo, redis, connection)
        exchange = RMQExchangeConfig(name="scopes")
        runner = Runner(
            config,
            "worker",
            worker_type="worker",
            exchange=exchange,
            infra=infra,
            worker_spec=WorkerSpec(
                collections={"write": "results"},
                factory=factory,
                needs_document_store=True,
                needs_document_fetcher=True,
            ),
        )
        await runner.setup()
        context = contexts[0]
        assert context.scoped_database is not None
        assert not mongo.databases
        token = current_scope.set("a")
        try:
            assert context.current_scope == "a"
            assert context.scoped_database() is mongo["test_a"]
            await context.stores["write"].store({}, "item")
            await context.document_store.insert({"doc_id": "item"})
            await context.get_document_func(
                "item", DocumentLocation(location_type="test")
            )
            assert set(mongo.databases) == {"test_a"}
            assert all(key.startswith("s:a:") for key in redis.entries)
        finally:
            current_scope.reset(token)
        retry_runner = RetryWorkerRunner(
            config, "retry", exchange=exchange, infra=infra
        )
        store = await retry_runner._create_default_retry_store()
        redis.entries.clear()
        await store.insert({"retry_key": "key", "scope": "a"})
        assert mongo[config.mongodb.database]["retries"].rows[0]["scope"] == "a"
        assert not redis.entries
        await runner.teardown()
        await connection.teardown()

    asyncio.run(run())
