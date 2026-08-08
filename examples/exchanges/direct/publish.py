"""
Publish the synthetic documents through a **job-defined route**.

Builds a ``RoutingPlan`` (parse → chunk → embed), validates it against the
deployed workers' declared capabilities (`validate_routing_plan`), then
publishes each document with the plan in ``job_parameters``. A
``RoutingPlanRouter`` on the publisher sends the initial messages to the plan's
entry node; each worker forwards along the plan from there.

Usage:
    python -m examples.exchanges.direct.publish \
        --job-name direct-001 --config-file examples/exchanges/direct/config.yaml
"""

import argparse
import asyncio
import logging
from pathlib import Path

from basics.logging import get_logger
from pymongo import AsyncMongoClient

from examples.exchanges.data import FAKE_DOCUMENTS
from examples.exchanges.direct.pipeline_spec import PIPELINE
from runtime_scripts.lib.logging_setup import configure_logging, resolve_log_level
from warren.pubsub.rabbitmq.aio_pika.connection import RMQConnectionManager
from warren.pubsub.rabbitmq.aio_pika.publisher import RMQPublisher
from warren.pubsub.routing import RoutingPlan, RoutingPlanRouter
from warren.runtime.config import RuntimeConfig
from warren.runtime.validation import build_capability_registry, validate_routing_plan
from warren.storage.jobs.mongodb import MongoDBJobStore


module_logger: logging.Logger = get_logger(__name__)

DEFAULT_CONFIG_PATH: Path = Path(__file__).parent / "config.yaml"

# The path this job should take through the deployed workers.
ROUTING_PLAN = RoutingPlan(
    entry=["document_parser"],
    edges={
        "document_parser": ["text_chunker"],
        "text_chunker": ["embedding_generator"],
        "embedding_generator": [],
    },
)


async def _publish(config: RuntimeConfig, job_name: str) -> str:
    # Fail fast if the plan references unknown workers or type-incompatible hops.
    validate_routing_plan(
        ROUTING_PLAN,
        build_capability_registry(PIPELINE),
        entry_data_type="pdf_document",
    )

    mongo_client = AsyncMongoClient(host=config.mongodb.host, port=config.mongodb.port)
    job_store = MongoDBJobStore(
        client=mongo_client, database_name=config.mongodb.database
    )
    await job_store.setup()
    job_id = await job_store.create_job(
        final_data_type=PIPELINE.final_data_type,
        num_documents=len(FAKE_DOCUMENTS),
        metadata={"job_name": job_name},
    )

    exchange = PIPELINE.exchange
    connection_manager = RMQConnectionManager(config.rabbitmq.connection)
    publisher = RMQPublisher(
        connection_manager=connection_manager,
        exchange_config=exchange,
        route_func=RoutingPlanRouter(),
    )
    try:
        await connection_manager.setup()
        await publisher.setup()
        plan = ROUTING_PLAN.model_dump()
        for doc_id in FAKE_DOCUMENTS:
            await publisher(
                {
                    "data_type": "pdf_document",
                    "data": {"doc_id": doc_id},
                    "job_id": job_id,
                    "origin": {"type": "publisher", "name": "routed-publisher"},
                    "job_parameters": {"routing": plan},
                }
            )
        module_logger.info(f"Job {job_id}: published {len(FAKE_DOCUMENTS)} documents")
    finally:
        await publisher.teardown()
        await connection_manager.teardown()
        await mongo_client.close()

    return job_id


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish documents on a defined route")
    parser.add_argument("--job-name", required=True, help="Human-readable job name.")
    parser.add_argument(
        "--config-file", type=Path, default=DEFAULT_CONFIG_PATH, help="Config YAML."
    )
    parser.add_argument("--debug", action="store_true", default=False)
    return parser.parse_args()


def main() -> None:
    global module_logger
    args = _parse_args()
    configure_logging(debug=args.debug)
    module_logger = get_logger(__name__, log_level=resolve_log_level(debug=args.debug))
    config = RuntimeConfig.from_yaml(args.config_file)
    asyncio.run(_publish(config, args.job_name))


if __name__ == "__main__":
    main()
