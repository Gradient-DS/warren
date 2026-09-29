import asyncio
import json
from unittest.mock import AsyncMock

import pytest

from warren.jobs.publishing.job_documents_publisher import JobDocumentsPublisher
from warren.jobs.publishing.job_publication_worker_runner import (
    JobPublicationWorkerRunner,
)
from warren.pubsub.rabbitmq.config import RMQExchangeConfig
from warren.pubsub.routing import LANE_FIELD, MessageFieldRouter, observer_exchange
from warren.runtime.config import RuntimeConfig


class _DocumentsPublisher(JobDocumentsPublisher):
    async def _load_document(self, source: dict) -> dict:
        return source

    async def _register_document(self, job_id: str, doc_data: dict) -> str:
        return "doc-1"

    def _create_message(
        self, job_id: str, doc_id: str, doc_data: dict, job_parameters: dict
    ) -> dict:
        return {**doc_data, "job_id": job_id, "doc_id": doc_id}

    def _get_source_id(self, source: dict) -> str:
        return "source-1"


@pytest.mark.parametrize("exchange_type", ["fanout", "topic", "direct"])
@pytest.mark.parametrize("routing", ["omitted", "none", "custom", "custom_default"])
def test_documents_use_publication_route_func(exchange_type: str, routing: str) -> None:
    async def scenario() -> None:
        exchange = RMQExchangeConfig(name="documents", type=exchange_type)
        message = {"data_type": "raw_document"}
        kwargs = {}
        if routing.startswith("custom"):
            kwargs["route_func"] = MessageFieldRouter(
                prefix_field=LANE_FIELD, default_prefix="bulk"
            )
            if routing == "custom":
                message[LANE_FIELD] = "interactive"
            expected_key = f"{message.get(LANE_FIELD, 'bulk')}.raw_document"
        else:
            if routing == "none":
                kwargs["route_func"] = None
            expected_key = "" if exchange_type == "fanout" else "raw_document"

        documents_publisher = None
        queue = None

        async def factory(publisher, infra, config, worker_name):
            nonlocal documents_publisher, queue
            queue = infra.pubsub_connection_manager.broker.bind(
                exchange, "downstream", expected_key
            )
            documents_publisher = _DocumentsPublisher(
                publisher=publisher, tracker=AsyncMock(), job_store=AsyncMock()
            )
            return documents_publisher

        runner = JobPublicationWorkerRunner(
            RuntimeConfig(backend="memory"),
            "publication-1",
            exchange=observer_exchange(exchange),
            publish_exchange=exchange,
            documents_publisher_factory=factory,
            **kwargs,
        )

        async def sources():
            yield message

        try:
            await runner.setup()
            result = await documents_publisher.publish_job("job-1", sources())
            assert result == {"published": 1, "failed": 0, "total": 1}
            assert queue.qsize() == 1
            delivery = queue.get_nowait()
            assert delivery.routing_key == expected_key
            assert json.loads(delivery.body) == {
                **message,
                "job_id": "job-1",
                "doc_id": "doc-1",
            }
        finally:
            await runner.teardown()

    asyncio.run(scenario())
