r"""
Generic processing-worker launcher.

Loads a ``PipelineSpec``, creates a ``DefaultWorkerRunner`` (or a custom
runner class), and runs a single worker type until shutdown.

Usage::

    # All defaults: ./pipeline/pipeline_spec.py:PIPELINE
    python -m document_processing.distributed.runtime_scripts.start_worker \
        --worker-type parser

    # Explicit directory
    python -m document_processing.distributed.runtime_scripts.start_worker \
        --pipeline-spec ./my_pipeline \
        --worker-type parser

    # Explicit file + variable
    python -m document_processing.distributed.runtime_scripts.start_worker \
        --pipeline-spec ./my_pipeline/alt_spec.py:EXPERIMENTAL \
        --worker-type parser

    # Installed Python module
    python -m document_processing.distributed.runtime_scripts.start_worker \
        --pipeline-spec gradient_pipelines.cool_pipeline \
        --worker-type parser

    # Installed Python module + variable
    python -m document_processing.distributed.runtime_scripts.start_worker \
        --pipeline-spec gradient_pipelines.cool_pipeline:EXPERIMENTAL \
        --worker-type parser
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
from document_processing.distributed.runtime_scripts.lib.pipeline import (
    DEFAULT_PIPELINE_DIR,
    DEFAULT_SPEC_VAR,
    load_pipeline,
    resolve_config_path,
)
from document_processing.distributed.runtime_scripts.lib.runner import run
from document_processing.distributed.warren.exceptions import WarrenError
from document_processing.distributed.warren.runtime.runner import (
    DefaultWorkerRunner,
)

module_logger: logging.Logger = get_logger(__name__)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Start a processing worker",
    )
    parser.add_argument(
        "--pipeline-spec",
        type=str,
        default=None,
        help=(
            "Pipeline spec location: [<file path>|<module path>][:<var name>]. "
            "Can be a directory (looks for pipeline_spec.py inside), "
            "a .py file, or a dotted module path. "
            "Append :<var name> to override the variable "
            f"(default: {DEFAULT_SPEC_VAR}). "
            f"Default path: {DEFAULT_PIPELINE_DIR}"
        ),
    )
    parser.add_argument(
        "--worker-type",
        required=True,
        help="Type of worker to start (must exist in the pipeline spec).",
    )
    parser.add_argument(
        "--runner",
        type=str,
        default=None,
        help=(
            "Dotted path to custom runner class, e.g. my.module:MyRunner. "
            "Default: DefaultWorkerRunner."
        ),
    )
    parser.add_argument(
        "--list-workers",
        action="store_true",
        default=False,
        help="Print available worker types and exit.",
    )
    add_common_args(parser)
    return parser.parse_args()


def _load_runner_class(runner_path: str) -> type:
    """Import a runner class from ``module.path:ClassName``.

    :raises ValueError: if the path format is invalid.
    :raises ImportError: if the module cannot be imported.
    :raises AttributeError: if the class is not found in the module.
    """
    if ":" not in runner_path:
        raise ValueError(
            f"Runner path must be module.path:ClassName, got: {runner_path}"
        )
    module_path, class_name = runner_path.rsplit(":", 1)
    mod = importlib.import_module(module_path)
    if not hasattr(mod, class_name):
        raise AttributeError(
            f"Module {module_path} has no attribute '{class_name}'"
        )
    return getattr(mod, class_name)


def describe_config(
    pipeline_spec: str | None,
    worker_type: str,
    worker_name: str | None,
    config_file: Path | None,
    runner: str | None,
    list_workers: bool,
    debug: bool,
    logger: logging.Logger,
) -> None:
    """Log input configuration before any resolution or work."""
    logger.info("Configuration:")
    logger.info(f"  pipeline_spec: {pipeline_spec}")
    logger.info(f"  worker_type: {worker_type}")
    logger.info(f"  worker_name: {worker_name}")
    logger.info(f"  config_file: {config_file}")
    logger.info(f"  runner: {runner}")
    logger.info(f"  list_workers: {list_workers}")
    logger.info(f"  debug: {debug}")


async def start_worker(
    *,
    pipeline_spec: str | None = None,
    worker_type: str,
    worker_name: str | None = None,
    config_file: Path | None = None,
    runner: str | None = None,
    list_workers: bool = False,
    debug: bool = False,
    logger: logging.Logger | None = None,
) -> None:
    """Load a pipeline spec, resolve the worker, and run it.

    :param pipeline_spec: pipeline spec location
        (see ``--pipeline-spec`` for format). Defaults to
        ``./pipeline``.
    :param worker_type: worker type to start (must exist in the
        pipeline spec).
    :param worker_name: unique worker instance name. Defaults to
        ``<worker_type>-<uuid8>``.
    :param config_file: path to RuntimeConfig YAML. Defaults to
        ``config.yaml`` in the pipeline spec directory.
    :param runner: dotted path to a custom runner class
        (``module:ClassName``). Defaults to ``DefaultWorkerRunner``.
    :param list_workers: if True, log available worker types and
        return.
    :param debug: enable DEBUG logging.
    :param logger: optional logger override.
    """
    log = logger or module_logger

    describe_config(
        pipeline_spec=pipeline_spec,
        worker_type=worker_type,
        worker_name=worker_name,
        config_file=config_file,
        runner=runner,
        list_workers=list_workers,
        debug=debug,
        logger=log,
    )

    spec_str = pipeline_spec or DEFAULT_PIPELINE_DIR

    try:
        pipeline, pipeline_dir = load_pipeline(spec_str, log)
    except Exception as e:
        raise WarrenError(
            f"Unable to load pipeline spec from: {spec_str}"
        ) from e

    if list_workers:
        worker_names = "\n  ".join(pipeline.workers.keys())
        log.info(f"Available worker types:\n  {worker_names}")
        return

    if worker_type not in pipeline.workers:
        valid_types = ", ".join(pipeline.workers.keys())
        raise WarrenError(
            f"Unknown worker type '{worker_type}'. "
            f"Valid types: {valid_types}"
        )

    runner_class = DefaultWorkerRunner
    if runner is not None:
        try:
            runner_class = _load_runner_class(runner)
        except Exception as e:
            raise WarrenError(
                f"Unable to load runner class: {runner}"
            ) from e

    try:
        resolved_config_path = resolve_config_path(config_file, pipeline_dir)
    except Exception as e:
        raise WarrenError(
            f"Unable to resolve config path from: "
            f"config_file={config_file}, pipeline_dir={pipeline_dir}"
        ) from e

    runner_factory_func = partial(
        runner_class,
        worker_type=worker_type,
        worker_spec=pipeline.workers[worker_type],
    )

    await run(
        runner_factory_func=runner_factory_func,
        config_file=resolved_config_path,
        worker_name=worker_name,
        worker_name_prefix=worker_type,
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
        asyncio.run(start_worker(**vars(args), logger=module_logger))
    except Exception as e:
        module_logger.error(
            f"Start worker failed: {summarize_exception_chain(e)}"
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
