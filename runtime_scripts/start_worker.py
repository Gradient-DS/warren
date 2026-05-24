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
import importlib.util
import logging
import sys
import uuid
from pathlib import Path

from basics.logging import get_logger
from basics.logging_utils import summarize_exception_chain

from document_processing.distributed.runtime_scripts._lib.logging_setup import (
    configure_logging,
    resolve_log_level,
)
from document_processing.distributed.warren.exceptions import WarrenError
from document_processing.distributed.warren.runtime.config import RuntimeConfig
from document_processing.distributed.warren.runtime.runner import DefaultWorkerRunner
from document_processing.distributed.warren.runtime.spec import PipelineSpec

module_logger: logging.Logger = get_logger(__name__)

DEFAULT_PIPELINE_DIR: str = "./pipeline"
DEFAULT_SPEC_MODULE: str = "pipeline_spec"
DEFAULT_SPEC_VAR: str = "PIPELINE"


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
        "--worker-name",
        default=None,
        help="Unique worker name (default: <worker-type>-<uuid8>).",
    )
    parser.add_argument(
        "--config-file",
        type=Path,
        default=None,
        help=(
            "Path to RuntimeConfig YAML. "
            "Default: config.yaml in the pipeline spec directory. "
            "Required when --pipeline-spec is a dotted module path."
        ),
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
        "--debug",
        action="store_true",
        default=False,
        help="Enable DEBUG logging (default: INFO).",
    )
    parser.add_argument(
        "--list-workers",
        action="store_true",
        default=False,
        help="Print available worker types and exit.",
    )
    return parser.parse_args()


def _split_var_suffix(spec_str: str) -> tuple[str, str]:
    """Split ``path_or_module:VAR`` into ``(path_or_module, var_name)``.

    :return: tuple of (location, variable name).
    """
    if ":" in spec_str:
        location, var_name = spec_str.rsplit(":", 1)
        return location, var_name
    return spec_str, DEFAULT_SPEC_VAR


def _import_file(file_path: Path) -> object:
    """Import a .py file as a module and return it.

    :raises ImportError: if the file cannot be loaded.
    """
    spec = importlib.util.spec_from_file_location(
        file_path.stem, file_path,
    )
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot create module spec from {file_path}")

    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _resolve_pipeline_file(directory: Path) -> Path:
    """Find ``pipeline_spec.py`` inside a directory (non-recursive).

    :return: path to the spec file.
    :raises FileNotFoundError: if no pipeline_spec.py exists in the
        directory.
    """
    candidate = directory / f"{DEFAULT_SPEC_MODULE}.py"
    if candidate.is_file():
        return candidate
    raise FileNotFoundError(
        f"No {DEFAULT_SPEC_MODULE}.py in directory: {directory}"
    )


def _load_pipeline(
    spec_str: str,
    logger: logging.Logger,
) -> tuple[PipelineSpec, Path | None]:
    """Resolve a pipeline spec from the given location string.

    Resolution order:

    1. Split off ``:VAR`` suffix (default: ``PIPELINE``).
    2. If the location exists as a directory -> look for
       ``pipeline_spec.py`` inside, import it.
    3. If it exists as a file -> import it directly.
    4. Otherwise -> try ``importlib.import_module(location)``.
    5. Read ``VAR`` from the loaded module.

    :return: tuple of (PipelineSpec, resolved_dir) where
        ``resolved_dir`` is the directory the spec was found in
        (None for dotted-module imports).
    :raises FileNotFoundError: if directory/file does not exist or
        contains no pipeline_spec.py.
    :raises ImportError: if dotted module cannot be imported.
    :raises AttributeError: if the variable is not found in the module.
    :raises TypeError: if the variable is not a PipelineSpec.
    """
    location, var_name = _split_var_suffix(spec_str)

    resolved_dir: Path | None = None
    path = Path(location)

    if path.is_dir():
        spec_file = _resolve_pipeline_file(path)
        logger.info(
            f"Loading pipeline spec from directory: {path} "
            f"(file: {spec_file.name}, var: {var_name})"
        )
        mod = _import_file(spec_file)
        resolved_dir = path

    elif path.is_file():
        logger.info(
            f"Loading pipeline spec from file: {path} (var: {var_name})"
        )
        mod = _import_file(path)
        resolved_dir = path.parent

    else:
        logger.info(
            f"Loading pipeline spec from module: {location} "
            f"(var: {var_name})"
        )
        mod = importlib.import_module(location)

    module_name = getattr(mod, "__name__", location)

    if not hasattr(mod, var_name):
        raise AttributeError(
            f"Module {module_name} has no attribute '{var_name}'"
        )

    pipeline = getattr(mod, var_name)

    if not isinstance(pipeline, PipelineSpec):
        raise TypeError(
            f"{var_name} in {module_name} is "
            f"{type(pipeline).__name__}, expected PipelineSpec"
        )

    return pipeline, resolved_dir


def _resolve_config_path(
    config_file: Path | None,
    pipeline_dir: Path | None,
) -> Path:
    """Determine the config file path."""
    if config_file is not None:
        return config_file

    if pipeline_dir is not None:
        candidate = pipeline_dir / "config.yaml"
        if candidate.is_file():
            return candidate
        raise FileNotFoundError(
            f"No config.yaml in pipeline directory {pipeline_dir} "
            f"and no --config-file specified"
        )

    raise ValueError(
        "--config-file is required when pipeline spec is loaded "
        "from a Python module (no directory to infer config.yaml from)"
    )


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


async def run(
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
    """Load a pipeline spec, create a runner, and run a worker.

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
        pipeline, pipeline_dir = _load_pipeline(spec_str, log)
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

    try:
        resolved_config_path = _resolve_config_path(config_file, pipeline_dir)
        config = RuntimeConfig.from_yaml(resolved_config_path)
    except Exception as e:
        raise WarrenError(
            f"Unable to load config from: {config_file or pipeline_dir}"
        ) from e

    log.info(f"Loaded config from: {resolved_config_path}")

    resolved_worker_name = (
        worker_name or f"{worker_type}-{uuid.uuid4().hex[:8]}"
    )

    runner_class = DefaultWorkerRunner
    if runner is not None:
        try:
            runner_class = _load_runner_class(runner)
        except Exception as e:
            raise WarrenError(
                f"Unable to load runner class: {runner}"
            ) from e

    worker_runner = runner_class(
        worker_type=worker_type,
        worker_name=resolved_worker_name,
        config=config,
        pipeline=pipeline,
    )

    try:
        try:
            await worker_runner.setup()
        except Exception as e:
            raise WarrenError(
                f"Worker setup failed for: {resolved_worker_name}"
            ) from e

        try:
            await worker_runner.run()
        except Exception as e:
            raise WarrenError(
                f"Worker run failed for: {resolved_worker_name}"
            ) from e
    finally:
        await worker_runner.teardown()


def main() -> None:
    global module_logger
    args = _parse_args()
    configure_logging(debug=args.debug)
    module_logger = get_logger(
        __name__, log_level=resolve_log_level(debug=args.debug)
    )

    try:
        asyncio.run(run(**vars(args), logger=module_logger))
    except Exception as e:
        module_logger.error(
            f"Start worker failed: {summarize_exception_chain(e)}"
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
