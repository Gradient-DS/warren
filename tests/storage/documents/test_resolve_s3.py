"""Unit tests for the S3 document resolver."""

import asyncio

import pytest

from warren.storage.documents.interface import DocumentNotFoundError
from warren.storage.documents.location import DocumentCloudLocation
from warren.storage.documents.resolve_s3 import resolve_s3


class _Body:
    def __init__(self, data: bytes) -> None:
        self._data = data

    def read(self) -> bytes:
        return self._data


class _FakeS3:
    def __init__(self, data: bytes) -> None:
        self._data = data

    def get_object(self, *, Bucket: str, Key: str) -> dict:  # noqa: N803
        return {"Body": _Body(self._data)}


class _MissingS3:
    def get_object(self, *, Bucket: str, Key: str) -> dict:  # noqa: N803
        from botocore.exceptions import ClientError

        raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")


def test_resolve_s3_returns_bytes() -> None:
    loc = DocumentCloudLocation(provider="s3", bucket="b", key="k")
    data = asyncio.run(resolve_s3(loc, client=_FakeS3(b"hello")))
    assert data == b"hello"


def test_resolve_s3_missing_raises_not_found() -> None:
    loc = DocumentCloudLocation(provider="s3", bucket="b", key="missing")
    with pytest.raises(DocumentNotFoundError):
        asyncio.run(resolve_s3(loc, client=_MissingS3()))
