"""Unit tests for job-scoped document byte caching.

Regression tests for the staleness bug where re-submitting the same
``doc_id`` with updated content (a new job) served bytes cached by a
previous job: the cache key was ``doc:{doc_id}`` with no job scoping.
"""

import asyncio

from warren.storage.documents.fetcher import (
    CachedDocumentFetcher,
    build_document_cache_key,
)
from warren.storage.documents.location import DocumentPathLocation
from warren.storage.results.binary import BinaryResultsStore


class _FakeBytesCache:
    """Minimal in-memory CacheInterface[bytes] double (get/set only)."""

    def __init__(self) -> None:
        self.data: dict[str, bytes] = {}

    async def get(self, key: str) -> bytes | None:
        return self.data.get(key)

    async def set(self, key: str, value: bytes, ttl_seconds: int | None = None) -> None:
        self.data[key] = value


class _FakeDocumentStore:
    """Minimal DocumentStoreInterface double for BinaryResultsStore writes."""

    async def insert(self, doc: dict, overwrite_existing: bool = False) -> str:
        return "result-1"

    def get_document_type(self) -> str:
        return "raw_bytes"


class _CountingResolver:
    """Resolver double that returns a mutable payload and counts calls."""

    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.calls = 0

    async def __call__(self, location) -> bytes:
        self.calls += 1
        return self.payload


_LOCATION = DocumentPathLocation(relative_path="data/doc.pdf")


def _fetcher(cache: _FakeBytesCache, resolver: _CountingResolver):
    return CachedDocumentFetcher(cache=cache, resolvers={"path": resolver})


def test_build_document_cache_key_without_job_id_is_legacy_key() -> None:
    assert build_document_cache_key("doc-1") == "doc:doc-1"


def test_build_document_cache_key_with_job_id_is_job_scoped() -> None:
    assert build_document_cache_key("doc-1", "job-a") == "doc:doc-1:job-a"


def test_retry_same_job_hits_cache() -> None:
    resolver = _CountingResolver(b"original")
    fetcher = _fetcher(_FakeBytesCache(), resolver)

    async def run() -> tuple[bytes, bytes]:
        first = await fetcher("doc-1", _LOCATION, job_id="job-a")
        second = await fetcher("doc-1", _LOCATION, job_id="job-a")
        return first, second

    first, second = asyncio.run(run())
    assert first == b"original"
    assert second == b"original"
    assert resolver.calls == 1


def test_new_job_same_doc_id_fetches_fresh_bytes() -> None:
    resolver = _CountingResolver(b"original")
    fetcher = _fetcher(_FakeBytesCache(), resolver)

    async def run() -> tuple[bytes, bytes]:
        first = await fetcher("doc-1", _LOCATION, job_id="job-a")
        resolver.payload = b"updated"  # content changed at the source
        second = await fetcher("doc-1", _LOCATION, job_id="job-b")
        return first, second

    first, second = asyncio.run(run())
    assert first == b"original"
    assert second == b"updated", "new job must never see the old job's bytes"
    assert resolver.calls == 2


def test_without_job_id_falls_back_to_shared_legacy_key() -> None:
    resolver = _CountingResolver(b"original")
    cache = _FakeBytesCache()
    fetcher = _fetcher(cache, resolver)

    async def run() -> bytes:
        await fetcher("doc-1", _LOCATION)
        return await fetcher("doc-1", _LOCATION)

    assert asyncio.run(run()) == b"original"
    assert resolver.calls == 1
    assert list(cache.data) == ["doc:doc-1"]


def test_binary_results_store_prepopulates_job_scoped_key() -> None:
    """WebFetch-style write-through must land on the fetcher's key.

    ``BinaryResultsStore.store_bytes(job_id=...)`` and a subsequent
    fetch with the same ``job_id`` must agree on the cache key, so the
    downstream worker hits the pre-populated bytes without a resolver
    round-trip.
    """
    cache = _FakeBytesCache()
    store = BinaryResultsStore(document_store=_FakeDocumentStore(), cache=cache)
    resolver = _CountingResolver(b"must-not-be-fetched")
    fetcher = _fetcher(cache, resolver)

    async def run() -> bytes:
        await store.store_bytes(b"rendered html", doc_id="doc-1", job_id="job-a")
        return await fetcher("doc-1", _LOCATION, job_id="job-a")

    assert asyncio.run(run()) == b"rendered html"
    assert resolver.calls == 0
    assert list(cache.data) == ["doc:doc-1:job-a"]
