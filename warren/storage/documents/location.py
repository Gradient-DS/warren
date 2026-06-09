"""
Document location models for the distributed processing system.

Discriminated union of Pydantic models representing where a document
lives. Locations are pure data — no I/O, no behavior. Resolution is
handled by resolver functions (see resolvers.py).

Deserialization from a dict uses the ``location_type`` discriminator::

    from pydantic import TypeAdapter
    adapter = TypeAdapter(AnyDocumentLocation)
    location = adapter.validate_python({"location_type": "path", "relative_path": "data/test.pdf"})
"""

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field


class DocumentLocation(BaseModel):
    """Base class for document locations.

    Subclasses declare a ``location_type`` literal that serves as the
    Pydantic discriminator for polymorphic deserialization.
    """

    model_config = ConfigDict(frozen=True)
    location_type: str


class DocumentPathLocation(DocumentLocation):
    """Document on the local filesystem.

    :param relative_path: Path relative to a configurable base directory.
    """

    location_type: Literal["path"] = "path"
    relative_path: str


class DocumentURLLocation(DocumentLocation):
    """Document at an HTTP(S) URL.

    :param url: Full URL to the document.
    """

    location_type: Literal["url"] = "url"
    url: str


class DocumentCloudLocation(DocumentLocation):
    """Document in cloud object storage (S3, GCS).

    :param provider: Cloud provider identifier.
    :param bucket: Bucket or container name.
    :param key: Object key (path within the bucket).
    :param region: Optional region for the bucket.
    """

    location_type: Literal["cloud"] = "cloud"
    provider: Literal["s3", "gcs"]
    bucket: str
    key: str
    region: str | None = None


AnyDocumentLocation = Annotated[
    DocumentPathLocation | DocumentURLLocation | DocumentCloudLocation,
    Field(discriminator="location_type"),
]
"""Discriminated union type for deserializing any DocumentLocation from a dict."""
