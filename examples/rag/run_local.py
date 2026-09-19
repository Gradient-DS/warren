"""
Run the RAG example in a single process, with no infrastructure.

    pip install -e ".[examples]"
    export OPENAI_API_KEY=...
    python -m examples.rag.run_local

Same workers and the same pipeline spec as the Docker quickstart. The only
differences are ``backend: memory`` in the config and that every worker
shares this process. Nothing is persisted: when the script exits, the job
and its results are gone.
"""

import argparse
import asyncio
import logging
from pathlib import Path

from basics.logging import get_logger

from examples.rag.pipeline_spec import PIPELINE
from examples.rag.sources import DEFAULT_URLS, doc_id_for
from runtime_scripts.lib.logging_setup import configure_logging, resolve_log_level
from warren.constants import PUBLISHER_ORIGIN_TYPE
from warren.pubsub.routing import observer_route_func
from warren.runtime import backends
from warren.runtime.config import RuntimeConfig
from warren.runtime.in_process import create_in_process_runners, run_in_process
from warren.runtime.infrastructure import (
    close_runtime_infrastructure,
    create_runtime_infrastructure,
)
from warren.storage.memory_registry import MemoryStoreRegistry


module_logger: logging.Logger = get_logger(__name__)

CONFIG_PATH = Path(__file__).parent / "config.memory.yaml"


async def run(urls: list[str]) -> bool:
    config = RuntimeConfig.from_yaml(CONFIG_PATH)
    infra = await create_runtime_infrastructure(config)
    stores = MemoryStoreRegistry()
    try:
        runners = await create_in_process_runners(
            config, PIPELINE, infra=infra, stores=stores
        )
        job_store = stores.job_store()
        job_id = await job_store.create_job(
            final_data_type=PIPELINE.final_data_type,
            num_documents=len(urls),
            metadata={"job_name": "rag-local"},
        )

        async def publish_and_wait() -> None:
            publisher = backends.create_publisher(
                config,
                infra.pubsub_connection_manager,
                exchange=PIPELINE.exchange,
                route_func=observer_route_func(PIPELINE.exchange),
            )
            await publisher.setup()
            for url in urls:
                await publisher(
                    {
                        "data_type": "pdf_document",
                        "data": {"doc_id": doc_id_for(url), "url": url},
                        "job_id": job_id,
                        "origin": {"type": PUBLISHER_ORIGIN_TYPE, "name": "rag-local"},
                    }
                )
            module_logger.info(f"Job {job_id}: published {len(urls)} PDF URL(s)")
            # Completion is a stored fact written by the job-status worker;
            # there is no event to await, so poll.
            while True:
                if (await job_store.get_status(job_id))["completed"]:
                    return
                await asyncio.sleep(0.5)

        await run_in_process(runners, until=publish_and_wait)

        status = await job_store.get_status(job_id)
        module_logger.info(f"Job {job_id} status: {status}")
        for stage in await stores.job_results_store().get_stage_counts(job_id):
            module_logger.info(
                f"  {stage['data_type']:<22} ok={stage['succeeded']} "
                f"soft_failed={stage['soft_failed']} hard_failed={stage['hard_failed']}"
            )
        return bool(status["completed"]) and not status["with_failures"]
    finally:
        await close_runtime_infrastructure(infra)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("urls", nargs="*", default=DEFAULT_URLS, help="PDF URLs")
    parser.add_argument("--debug", action="store_true", default=False)
    args = parser.parse_args()

    global module_logger
    configure_logging(debug=args.debug)
    module_logger = get_logger(__name__, log_level=resolve_log_level(debug=args.debug))

    raise SystemExit(0 if asyncio.run(run(args.urls)) else 1)


if __name__ == "__main__":
    main()
