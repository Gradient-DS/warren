"""
Publish PDF URLs into the ``examples/rag`` pipeline.

Each URL becomes one small ``pdf_document`` message; the parser worker
downloads and parses it. Defaults to two arXiv papers so the example runs
with no arguments — pass ``--url`` (repeatable) to use your own.

Usage:
    export OPENAI_API_KEY=sk-...        # needed by the embedding *worker*
    python -m examples.rag.publish_jobs \
        --job-name rag-001 \
        --config-file examples/rag/config.yaml

    # your own PDFs:
    python -m examples.rag.publish_jobs --job-name rag-002 \
        --url https://example.com/a.pdf --url https://example.com/b.pdf
"""

import argparse
import asyncio
import logging
from pathlib import Path

from basics.logging import get_logger
from pymongo import AsyncMongoClient

from examples.rag.pipeline_spec import PIPELINE
from runtime_scripts.lib.logging_setup import configure_logging, resolve_log_level
from warren.constants import PUBLISHER_ORIGIN_TYPE
from warren.pubsub.rabbitmq.aio_pika.connection import RMQConnectionManager
from warren.pubsub.rabbitmq.aio_pika.publisher import RMQPublisher
from warren.pubsub.routing import observer_route_func
from warren.runtime.config import RuntimeConfig
from warren.storage.jobs.mongodb import MongoDBJobStore


module_logger: logging.Logger = get_logger(__name__)

DEFAULT_CONFIG_PATH: Path = Path(__file__).parent / "config.yaml"

# Two arXiv papers, so the example runs out of the box.
DEFAULT_URLS: list[str] = [
    "https://arxiv.org/pdf/1706.03762",  # Attention Is All You Need
    "https://arxiv.org/pdf/2103.15348",  # LayoutParser
]


def _doc_id(url: str) -> str:
    """Derive a readable doc_id from a URL (its last path segment)."""
    return url.rstrip("/").rsplit("/", 1)[-1] or url


async def _publish(
    config: RuntimeConfig,
    job_name: str,
    urls: list[str],
) -> str:
    mongo_client = AsyncMongoClient(host=config.mongodb.host, port=config.mongodb.port)
    job_store = MongoDBJobStore(
        client=mongo_client, database_name=config.mongodb.database
    )
    await job_store.setup()
    job_id = await job_store.create_job(
        final_data_type=PIPELINE.final_data_type,
        num_documents=len(urls),
        metadata={"job_name": job_name},
    )

    connection_manager = RMQConnectionManager(config.rabbitmq.connection)
    publisher = RMQPublisher(
        connection_manager=connection_manager,
        exchange_config=PIPELINE.exchange,
        route_func=observer_route_func(PIPELINE.exchange),
    )
    try:
        await connection_manager.setup()
        await publisher.setup()
        for url in urls:
            await publisher(
                {
                    "data_type": "pdf_document",
                    "data": {"doc_id": _doc_id(url), "url": url},
                    "job_id": job_id,
                    "origin": {"type": PUBLISHER_ORIGIN_TYPE, "name": "rag-publisher"},
                }
            )
        module_logger.info(f"Job {job_id}: published {len(urls)} PDF URL(s)")
    finally:
        await publisher.teardown()
        await connection_manager.teardown()
        await mongo_client.close()

    return job_id


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publish PDF URLs into the rag pipeline"
    )
    parser.add_argument("--job-name", required=True, help="Human-readable job name.")
    parser.add_argument(
        "--url",
        action="append",
        dest="urls",
        metavar="URL",
        help="PDF URL to process (repeatable). Defaults to two arXiv papers.",
    )
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
    urls = args.urls or DEFAULT_URLS
    asyncio.run(_publish(config, args.job_name, urls))


if __name__ == "__main__":
    main()
