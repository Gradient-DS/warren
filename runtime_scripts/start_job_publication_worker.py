r"""
Job publication worker launcher.

Starts a ``JobPublicationWorkerRunner`` that consumes job messages and
publishes their documents into the processing pipeline.

Unlike other launchers, this one requires application-specific wiring
for the ``documents_publisher`` (which carries adapters, stores, etc.).
The ``--pipeline-spec`` flag is used to load a factory function from
the pipeline that builds the publisher.

Usage::

    python -m document_processing.distributed.runtime_scripts.start_job_publication_worker \
        --config-file ./pipeline/config.yaml \
        --publisher-factory my_pipeline.publishers:create_publisher
"""

import argparse
import asyncio
import importlib
import logging
import sys
from functools import partial
from pathlib import Path

from basics.logging import get_logger
from basics.logging_utils import summarize_exception_chain

from document_processing.distributed.runtime_scripts.lib.cli import (
    add_common_args,
)
from document_processing.distributed.runtime_scripts.lib.logging_setup import (
    configure_logging,
    resolve_log_level,
)
from document_processing.distributed.runtime_scripts.lib.runner import run
from document_processing.distributed.warren.exceptions import WarrenError
from document_processing.distributed.warren.jobs.publishing.job_publication_worker_runner import (
    JobPublicationWorkerRunner,
)

module_logger: logging.Logger = get_logger(__name__)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Start a job publication worker",
    )
    parser.add_argument(
        "--publisher-factory",
        type=str,
        required=True,
        help=(
            "Dotted path to a factory function that creates the "
            "documents publisher, e.g. my.module:create_publisher. "
            "The factory receives (config: RuntimeConfig, "
            "worker_name: str) and returns a JobDocumentsPublisher."
        ),
    )
    add_common_args(parser)
    return parser.parse_args()


def describe_config(
    publisher_factory: str,
    config_file: str | None,
    worker_name: str | None,
    debug: bool,
    logger: logging.Logger,
) -> None:
    """Log input configuration before any resolution or work."""
    logger.info("Configuration:")
    logger.info(f"  publisher_factory: {publisher_factory}")
    logger.info(f"  config_file: {config_file}")
    logger.info(f"  worker_name: {worker_name}")
    logger.info(f"  debug: {debug}")


def _load_publisher_factory(factory_path: str) -> callable:
    """Import a publisher factory from ``module.path:func_name``."""
    if ":" not in factory_path:
        raise ValueError(
            f"Publisher factory must be module.path:func_name, "
            f"got: {factory_path}"
        )
    module_path, func_name = factory_path.rsplit(":", 1)
    mod = importlib.import_module(module_path)
    if not hasattr(mod, func_name):
        raise AttributeError(
            f"Module {module_path} has no attribute '{func_name}'"
        )
    return getattr(mod, func_name)


async def start_job_publication_worker(
    *,
    publisher_factory: str,
    config_file: str | None = None,
    worker_name: str | None = None,
    debug: bool = False,
    logger: logging.Logger | None = None,
) -> None:
    """Start a job publication worker.

    :param publisher_factory: dotted path to the publisher factory
        function (``module:func``). The factory receives
        ``(config, worker_name)`` and returns a
        ``JobDocumentsPublisher``.
    :param config_file: path to RuntimeConfig YAML.
    :param worker_name: unique worker instance name.
    :param debug: enable DEBUG logging.
    :param logger: optional logger override.
    """
    log = logger or module_logger

    describe_config(
        publisher_factory=publisher_factory,
        config_file=config_file,
        worker_name=worker_name,
        debug=debug,
        logger=log,
    )

    try:
        pub_factory_func = _load_publisher_factory(publisher_factory)
    except Exception as e:
        raise WarrenError(
            f"Unable to load publisher factory: {publisher_factory}"
        ) from e

    resolved_config = (
        Path(config_file) if config_file else Path("./pipeline/config.yaml")
    )

    def runner_factory(config, wn):
        publisher = pub_factory_func(config, wn)
        return JobPublicationWorkerRunner(
            config, wn,
            documents_publisher=publisher,
        )

    await run(
        runner_factory_func=runner_factory,
        config_file=resolved_config,
        worker_name=worker_name,
        worker_name_prefix="publication-worker",
        debug=debug,
        logger=log,
    )


def main() -> None:
    global module_logger
    args = _parse_args()
    configure_logging(debug=args.debug)
    module_logger = get_logger(
        __name__, log_level=resolve_log_level(debug=args.debug)
    )

    try:
        asyncio.run(
            start_job_publication_worker(**vars(args), logger=module_logger)
        )
    except Exception as e:
        module_logger.error(
            f"Start job publication worker failed: "
            f"{summarize_exception_chain(e)}"
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
