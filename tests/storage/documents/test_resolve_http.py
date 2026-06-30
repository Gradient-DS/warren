"""Unit tests for the HTTP(S) document resolver."""

import asyncio

import httpx
import pytest

from warren.storage.documents.interface import DocumentNotFoundError
from warren.storage.documents.location import DocumentURLLocation
from warren.storage.documents.resolve_http import resolve_http


def _client(handler) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def test_resolve_http_returns_bytes() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"hello")

    loc = DocumentURLLocation(url="https://example.com/doc.pdf")

    async def run() -> bytes:
        async with _client(handler) as client:
            return await resolve_http(loc, client=client)

    assert asyncio.run(run()) == b"hello"


def test_resolve_http_404_raises_not_found() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404)

    loc = DocumentURLLocation(url="https://example.com/missing.pdf")

    async def run() -> None:
        async with _client(handler) as client:
            await resolve_http(loc, client=client)

    with pytest.raises(DocumentNotFoundError):
        asyncio.run(run())


def test_resolve_http_5xx_propagates() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503)

    loc = DocumentURLLocation(url="https://example.com/flaky.pdf")

    async def run() -> None:
        async with _client(handler) as client:
            await resolve_http(loc, client=client)

    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(run())
