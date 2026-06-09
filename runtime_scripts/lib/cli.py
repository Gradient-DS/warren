"""
Common CLI argument helpers for launcher scripts.
"""

import argparse
from pathlib import Path


def add_common_args(parser: argparse.ArgumentParser) -> None:
    """Add arguments shared across all launcher scripts.

    Adds: --config-file, --worker-name, --debug.
    """
    parser.add_argument(
        "--config-file",
        type=Path,
        default=None,
        help=("Path to RuntimeConfig YAML. Default: ./pipeline/config.yaml"),
    )
    parser.add_argument(
        "--worker-name",
        default=None,
        help="Unique worker name (default: auto-generated).",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        default=False,
        help="Enable DEBUG logging (default: INFO).",
    )
