"""Unit tests for the HTTP(S) document resolver."""

import asyncio

import httpx
import pytest

from warren.storage.documents.interface import DocumentNotFoundError
from warren.storage.documents.location import DocumentURLLocation
from warren.storage.documents.resolve_http import build_client, resolve_http


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


def test_resolve_http_follows_redirects_by_default() -> None:
    """A 307 to the real file resolves, rather than failing the document.

    This is the regression that motivated build_client: httpx defaults
    follow_redirects to False, so 91 of 96 download failures in one
    500-document corpus run were 307s and 303s whose redirect target was
    the PDF being asked for.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/permalink":
            return httpx.Response(
                307, headers={"Location": "https://cdn.example.com/real.pdf"}
            )
        return httpx.Response(200, content=b"%PDF-1.7")

    loc = DocumentURLLocation(url="https://example.com/permalink")

    async def run() -> bytes:
        async with build_client(transport=httpx.MockTransport(handler)) as client:
            return await resolve_http(loc, client=client)

    assert asyncio.run(run()) == b"%PDF-1.7"


def test_build_client_follows_redirects_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("HTTP_FOLLOW_REDIRECTS", raising=False)
    assert build_client().follow_redirects is True


@pytest.mark.parametrize("raw", ["false", "FALSE", "0", "no", "off", " Off "])
def test_build_client_redirects_can_be_disabled(
    monkeypatch: pytest.MonkeyPatch, raw: str
) -> None:
    monkeypatch.setenv("HTTP_FOLLOW_REDIRECTS", raw)
    assert build_client().follow_redirects is False


@pytest.mark.parametrize("raw", ["true", "TRUE", "1", "yes", "on"])
def test_build_client_redirects_can_be_enabled_explicitly(
    monkeypatch: pytest.MonkeyPatch, raw: str
) -> None:
    monkeypatch.setenv("HTTP_FOLLOW_REDIRECTS", raw)
    assert build_client().follow_redirects is True


def test_build_client_empty_env_falls_back_to_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("HTTP_FOLLOW_REDIRECTS", "   ")
    assert build_client().follow_redirects is True


def test_build_client_rejects_an_unparseable_flag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A typo must not read as False and silently drop redirect following."""
    monkeypatch.setenv("HTTP_FOLLOW_REDIRECTS", "flase")
    with pytest.raises(ValueError, match="HTTP_FOLLOW_REDIRECTS"):
        build_client()


def test_build_client_reads_timeout_and_max_redirects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("HTTP_TIMEOUT_S", "12.5")
    monkeypatch.setenv("HTTP_MAX_REDIRECTS", "3")
    client = build_client()
    assert client.timeout.read == 12.5
    assert client.max_redirects == 3


def test_build_client_arguments_win_over_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("HTTP_FOLLOW_REDIRECTS", "false")
    assert build_client(follow_redirects=True).follow_redirects is True


def test_resolve_http_surfaces_the_redirect_when_following_is_off() -> None:
    """The old behaviour is still reachable, and still loses the document."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            307, headers={"Location": "https://cdn.example.com/real.pdf"}
        )

    loc = DocumentURLLocation(url="https://example.com/permalink")

    async def run() -> None:
        async with build_client(
            follow_redirects=False, transport=httpx.MockTransport(handler)
        ) as client:
            await resolve_http(loc, client=client)

    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(run())
