"""``created_at`` must be a BSON Date — a TTL index over a string deletes
nothing, silently, for ever.

The deterministic property is the *stored type*: MongoDB's TTL monitor
deletes documents whose indexed field is a BSON Date (or an array holding
one) and skips every other type without logging a word. Asserting actual
expiry would mean waiting on the TTL monitor, which sweeps once a minute
and emits no signal — flaky as a unit test.

The default tests run against in-memory doubles, as the rest of this suite
does. The round-trip through a real MongoDB is opt-in: set
``WARREN_TEST_MONGO_URI`` (e.g. ``mongodb://localhost:27017``) to run it.
"""

import asyncio
import os
import uuid
from collections.abc import AsyncGenerator
from datetime import datetime

import pytest

from warren.storage.results.binary import BinaryResultsStore
from warren.storage.results.default import DefaultResultsStore
from warren.storage.results.interface import ResultDoc


_MONGO_URI_ENV = "WARREN_TEST_MONGO_URI"


class _RecordingDocumentStore:
    """Minimal DocumentStoreInterface double that keeps what was inserted."""

    def __init__(self) -> None:
        self.docs: list[dict] = []

    async def insert(self, doc: dict, overwrite_existing: bool = False) -> str:
        self.docs.append(doc)
        return f"result-{len(self.docs)}"

    def query(self, params: dict) -> AsyncGenerator[dict, None]:
        async def run() -> AsyncGenerator[dict, None]:
            for doc in self.docs:
                if all(doc.get(key) == value for key, value in params.items()):
                    yield dict(doc)

        return run()

    def get_document_type(self) -> str:
        return "test_results"

    def get_doc_id_field(self) -> str:
        return "result_id"

    async def has_unique_index(self, index_spec) -> bool:
        return True


def _default_store(document_store: _RecordingDocumentStore) -> DefaultResultsStore:
    return DefaultResultsStore(document_store=document_store, cache=None)


def test_a_stored_result_records_a_bson_date() -> None:
    document_store = _RecordingDocumentStore()
    store = _default_store(document_store)

    asyncio.run(store.store(result={"x": 1}, doc_id="doc-1", job_id="job-1"))

    raw = document_store.docs[0]
    assert isinstance(raw["created_at"], datetime), (
        "created_at was stored as "
        f"{type(raw['created_at']).__name__}; MongoDB's TTL monitor deletes "
        "only BSON Dates and skips every other type without a word"
    )


def test_the_result_doc_surfaces_it_as_a_datetime() -> None:
    store = _default_store(_RecordingDocumentStore())

    async def run() -> ResultDoc:
        await store.store(result={"x": 1}, doc_id="doc-1", job_id="job-1")
        return await store.get_result(doc_id="doc-1", job_id="job-1")

    doc = asyncio.run(run())

    assert isinstance(doc.created_at, datetime)


def test_a_stored_byte_payload_records_a_bson_date() -> None:
    document_store = _RecordingDocumentStore()
    store = BinaryResultsStore(document_store=document_store, cache=None)

    asyncio.run(store.store_bytes(b"payload", doc_id="doc-1", job_id="job-1"))

    raw = document_store.docs[0]
    assert isinstance(raw["created_at"], datetime), (
        "created_at was stored as "
        f"{type(raw['created_at']).__name__}; MongoDB's TTL monitor deletes "
        "only BSON Dates and skips every other type without a word"
    )


@pytest.mark.skipif(
    not os.environ.get(_MONGO_URI_ENV),
    reason=f"{_MONGO_URI_ENV} is not set; the BSON round-trip needs a real MongoDB",
)
def test_mongodb_reads_created_at_back_as_a_date() -> None:
    from pymongo import AsyncMongoClient

    from warren.storage.results.factories import create_default_results_store

    uri = os.environ[_MONGO_URI_ENV]
    database_name = f"warren_test_{uuid.uuid4().hex}"
    collection_name = "created_at_probe"

    async def run() -> object:
        client = AsyncMongoClient(uri)
        try:
            store = await create_default_results_store(
                collection_name=collection_name,
                mongo_client=client,
                database_name=database_name,
            )
            await store.store(result={"x": 1}, doc_id="doc-1", job_id="job-1")
            collection = client[database_name][collection_name]
            raw = await collection.find_one({"doc_id": "doc-1"})
            return raw["created_at"]
        finally:
            await client.drop_database(database_name)
            await client.close()

    created_at = asyncio.run(run())

    assert isinstance(created_at, datetime), (
        "created_at came back from MongoDB as "
        f"{type(created_at).__name__}; MongoDB's TTL monitor deletes "
        "only BSON Dates and skips every other type without a word"
    )
