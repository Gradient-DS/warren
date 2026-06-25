"""
Publish real PDF files into the ``examples/rag`` pipeline.

Each PDF under ``--pdf-dir`` becomes one ``pdf_document`` message carrying a
**location** (a ``path`` ``DocumentLocation``), not the bytes. The parser
worker fetches the bytes through the framework's document fetcher from that
location — so the broker only ever moves small messages around.

Usage:
    export OPENAI_API_KEY=sk-...        # needed by the embedding *worker*
    python -m examples.rag.publish_jobs \
        --job-name pdf-001 \
        --pdf-dir examples/rag/documents \
        --config-file examples/rag/config.yaml
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
from warren.storage.documents.location import DocumentPathLocation
from warren.storage.jobs.mongodb import MongoDBJobStore


module_logger: logging.Logger = get_logger(__name__)

DEFAULT_CONFIG_PATH: Path = Path(__file__).parent / "config.yaml"
DEFAULT_PDF_DIR: Path = Path(__file__).parent / "documents"


def _discover_pdfs(pdf_dir: Path) -> list[Path]:
    pdfs = sorted(p for p in pdf_dir.glob("*.pdf"))
    if not pdfs:
        msg = f"No .pdf files found in {pdf_dir}"
        raise SystemExit(msg)
    return pdfs


async def _publish(
    config: RuntimeConfig,
    job_name: str,
    pdfs: list[Path],
) -> str:
    mongo_client = AsyncMongoClient(host=config.mongodb.host, port=config.mongodb.port)
    job_store = MongoDBJobStore(
        client=mongo_client, database_name=config.mongodb.database
    )
    await job_store.setup()
    job_id = await job_store.create_job(
        final_data_type=PIPELINE.final_data_type,
        num_documents=len(pdfs),
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
        for pdf in pdfs:
            doc_id = pdf.stem
            # Absolute path so the worker resolves it regardless of its CWD.
            location = DocumentPathLocation(relative_path=str(pdf.resolve()))
            await publisher(
                {
                    "data_type": "pdf_document",
                    "data": {
                        "doc_id": doc_id,
                        "document_location": location.model_dump(),
                    },
                    "job_id": job_id,
                    "origin": {"type": PUBLISHER_ORIGIN_TYPE, "name": "pdf-publisher"},
                }
            )
        module_logger.info(f"Job {job_id}: published {len(pdfs)} PDF(s)")
    finally:
        await publisher.teardown()
        await connection_manager.teardown()
        await mongo_client.close()

    return job_id


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish PDFs into the pdf pipeline")
    parser.add_argument("--job-name", required=True, help="Human-readable job name.")
    parser.add_argument(
        "--pdf-dir",
        type=Path,
        default=DEFAULT_PDF_DIR,
        help=f"Directory of .pdf files to publish (default: {DEFAULT_PDF_DIR}).",
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
    pdfs = _discover_pdfs(args.pdf_dir)
    asyncio.run(_publish(config, args.job_name, pdfs))


if __name__ == "__main__":
    main()
