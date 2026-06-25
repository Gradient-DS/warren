"""
Document source discovery utilities.

Provides functions to discover documents from local directories or GCS
buckets. Used by publisher scripts to build the source list for job
submission.
"""

import logging
from collections.abc import Sequence
from pathlib import Path

from basics.logging import get_logger

from warren.exceptions import (
    OptionalDependencyError,
)


module_logger: logging.Logger = get_logger(__name__)


def discover_local_pdfs(pdf_dir: Path, max_docs: int = 0) -> Sequence[Path]:
    """Find PDF files in a local directory.

    :param pdf_dir: directory to search for PDFs.
    :param max_docs: maximum number of PDFs to return (0 = all).
    :return: sorted list of PDF file paths.
    :raises FileNotFoundError: if the directory doesn't exist.
    :raises ValueError: if no PDFs are found.
    """
    if not pdf_dir.is_dir():
        msg = f"PDF directory not found: {pdf_dir}"
        raise FileNotFoundError(msg)

    pdfs = sorted(pdf_dir.glob("*.pdf"))
    if not pdfs:
        msg = f"No PDF files found in {pdf_dir}"
        raise ValueError(msg)

    if max_docs > 0:
        pdfs = pdfs[:max_docs]

    module_logger.info("Discovered %d PDFs in %s", len(pdfs), pdf_dir)
    return pdfs


def discover_gcs_pdfs(
    bucket_name: str,
    prefix: str = "pdfs/",
    max_docs: int = 0,
) -> Sequence[str]:
    """List PDF objects in a GCS bucket.

    :param bucket_name: GCS bucket name.
    :param prefix: object key prefix (e.g., ``"pdfs/"``).
    :param max_docs: maximum number of objects to return (0 = all).
    :return: sorted list of object keys.
    :raises OptionalDependencyError: if ``google-cloud-storage`` is
        not installed.
    :raises ValueError: if no PDFs are found.
    """
    try:
        from google.cloud import storage
    except ImportError as e:
        msg = "google-cloud-storage"
        raise OptionalDependencyError(
            msg,
            install_hint='pip install "warren[gcs]"',
        ) from e

    client = storage.Client()
    blobs = client.list_blobs(bucket_name, prefix=prefix)
    keys = sorted(blob.name for blob in blobs if blob.name.lower().endswith(".pdf"))

    if not keys:
        msg = f"No PDF objects found in gs://{bucket_name}/{prefix}"
        raise ValueError(msg)

    if max_docs > 0:
        keys = keys[:max_docs]

    module_logger.info(
        "Discovered %d PDFs in gs://%s/%s", len(keys), bucket_name, prefix
    )
    return keys
