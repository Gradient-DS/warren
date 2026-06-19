r"""
Job publication worker launcher.

Starts a ``JobPublicationWorkerRunner`` that consumes job messages and
publishes their documents into the processing pipeline.

Requires a ``--publisher-factory`` pointing to an async factory function
matching ``DocumentsPublisherFactoryFunc``. The runner calls this factory
in ``setup()`` with the shared RMQ publisher, infrastructure, config,
and worker name.

Usage::

    python -m runtime_scripts.start_job_publication_worker \
        --config-file ./pipeline/config.yaml \
        --publisher-factory my_pipeline.publishers.factory:create_multi_type_publisher
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

from runtime_scripts.lib.cli import (
    add_common_args,
)
from runtime_scripts.lib.logging_setup import (
    configure_logging,
    resolve_log_level,
)
from runtime_scripts.lib.pipeline import (
    DEFAULT_PIPELINE_DIR,
    load_pipeline,
    resolve_config_path,
    resolve_default_exchange,
)
from runtime_scripts.lib.runner import run
from warren.exceptions import WarrenError
from warren.jobs.publishing.job_publication_worker_runner import (
    DocumentsPublisherFactoryFunc,
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
            "Dotted path to an async factory function matching "
            "DocumentsPublisherFactoryFunc, e.g. "
            "my.module:create_multi_type_publisher. "
            "The factory receives (publisher, infra, config, "
            "worker_name) and returns a JobDocumentsPublisher."
        ),
    )
    parser.add_argument(
        "--pipeline-spec",
        type=str,
        default=None,
        help=(
            "Pipeline spec location (same format as start_worker). Needed to "
            "resolve the exchange to publish on (default_exchange). "
            f"Default path: {DEFAULT_PIPELINE_DIR}"
        ),
    )
    add_common_args(parser)
    return parser.parse_args()


def describe_config(
    publisher_factory: str,
    pipeline_spec: str | None,
    config_file: str | None,
    worker_name: str | None,
    debug: bool,
    logger: logging.Logger,
) -> None:
    """Log input configuration before any resolution or work."""
    logger.info("Configuration:")
    logger.info(f"  publisher_factory: {publisher_factory}")
    logger.info(f"  pipeline_spec: {pipeline_spec}")
    logger.info(f"  config_file: {config_file}")
    logger.info(f"  worker_name: {worker_name}")
    logger.info(f"  debug: {debug}")


def _load_publisher_factory(factory_path: str) -> DocumentsPublisherFactoryFunc:
    """Import a publisher factory from ``module.path:func_name``."""
    if ":" not in factory_path:
        msg = f"Publisher factory must be module.path:func_name, got: {factory_path}"
        raise ValueError(msg)
    module_path, func_name = factory_path.rsplit(":", 1)
    mod = importlib.import_module(module_path)
    if not hasattr(mod, func_name):
        msg = f"Module {module_path} has no attribute '{func_name}'"
        raise AttributeError(msg)
    return getattr(mod, func_name)


async def start_job_publication_worker(
    *,
    publisher_factory: str,
    pipeline_spec: str | None = None,
    config_file: str | None = None,
    worker_name: str | None = None,
    debug: bool = False,
    logger: logging.Logger | None = None,
) -> None:
    """Start a job publication worker.

    :param publisher_factory: dotted path to the publisher factory
        function (``module:func``) matching
        ``DocumentsPublisherFactoryFunc``.
    :param pipeline_spec: pipeline spec location (see ``--pipeline-spec``).
        Used to resolve the exchange the worker publishes on.
    :param config_file: path to RuntimeConfig YAML.
    :param worker_name: unique worker instance name.
    :param debug: enable DEBUG logging.
    :param logger: optional logger override.
    """
    log = logger or module_logger

    describe_config(
        publisher_factory=publisher_factory,
        pipeline_spec=pipeline_spec,
        config_file=config_file,
        worker_name=worker_name,
        debug=debug,
        logger=log,
    )

    try:
        pub_factory_func = _load_publisher_factory(publisher_factory)
    except Exception as e:
        msg = f"Unable to load publisher factory: {publisher_factory}"
        raise WarrenError(msg) from e

    spec_str = pipeline_spec or DEFAULT_PIPELINE_DIR
    try:
        pipeline, pipeline_dir = load_pipeline(spec_str, log)
    except Exception as e:
        msg = f"Unable to load pipeline spec from: {spec_str}"
        raise WarrenError(msg) from e

    exchange = resolve_default_exchange(pipeline)
    resolved_config = resolve_config_path(
        Path(config_file) if config_file else None, pipeline_dir
    )

    runner_factory = partial(
        JobPublicationWorkerRunner,
        exchange=exchange,
        documents_publisher_factory=pub_factory_func,
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
    module_logger = get_logger(__name__, log_level=resolve_log_level(debug=args.debug))

    try:
        asyncio.run(start_job_publication_worker(**vars(args), logger=module_logger))
    except Exception as e:
        module_logger.error(
            f"Start job publication worker failed: {summarize_exception_chain(e)}"
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
