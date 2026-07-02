"""_resolve_cloud dispatches by provider."""

import asyncio

import pytest

from warren.runtime.runner import _resolve_cloud
from warren.storage.documents.interface import UnknownLocationTypeError
from warren.storage.documents.location import DocumentCloudLocation


def test_dispatch_to_registered_provider() -> None:
    async def fake_gcs(_loc: object) -> bytes:
        return b"gcs-bytes"

    loc = DocumentCloudLocation(provider="gcs", bucket="b", key="k")
    out = asyncio.run(_resolve_cloud(loc, by_provider={"gcs": fake_gcs}))
    assert out == b"gcs-bytes"


def test_unregistered_provider_raises() -> None:
    async def fake_gcs(_loc: object) -> bytes:
        return b""

    loc = DocumentCloudLocation(provider="s3", bucket="b", key="k")
    with pytest.raises(UnknownLocationTypeError):
        asyncio.run(_resolve_cloud(loc, by_provider={"gcs": fake_gcs}))
