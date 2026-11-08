import asyncio
from unittest.mock import AsyncMock

import pytest

from tests.storage.job_store_doubles import AggregateCollection, MongoClient
from warren.storage.job_results.mongodb import MongoDBJobResultsStore


@pytest.mark.parametrize(("rows", "expected"), [([], 0), ([{"total": 2}], 2)])
def test_hard_failure_count_matches_the_partial_index(rows: list[dict], expected: int) -> None:
    client = MongoClient()
    collection = AggregateCollection(rows)
    client["test"].collections["job_results"] = collection
    store = MongoDBJobResultsStore(client, database_name="test")

    async def scenario() -> None:
        await store.setup()
        assert await store.count_hard_failed_docs("j") == expected

    asyncio.run(scenario())
    index = collection.indexes["job_id_1_doc_id_1"]
    assert index == {
        "key": [("job_id", 1), ("doc_id", 1)],
        "partialFilterExpression": {"hard_failure": {"$exists": True}},
    }
    assert collection.pipelines == [
        [
            {"$match": {"job_id": "j", **index["partialFilterExpression"]}},
            {"$group": {"_id": "$doc_id"}},
            {"$count": "total"},
        ]
    ]


@pytest.mark.parametrize("doc_ids", [[], ["own"]])
def test_completion_queries_restrict_document_membership(doc_ids: list[str]) -> None:
    client = MongoClient()
    collection = AggregateCollection([])
    collection.count_documents = AsyncMock(return_value=0)
    client["test"].collections["job_results"] = collection
    store = MongoDBJobResultsStore(client, database_name="test")

    async def run() -> None:
        assert await store.count_completed_docs("j", "final", doc_ids=doc_ids) == 0
        assert await store.count_hard_failed_docs("j", doc_ids=doc_ids) == 0

    asyncio.run(run())
    collection.count_documents.assert_awaited_once_with(
        {"job_id": "j", "doc_id": {"$in": doc_ids}, "data_type": "final", "success": True}
    )
    assert collection.pipelines[0][0] == {
        "$match": {"job_id": "j", "doc_id": {"$in": doc_ids}, "hard_failure": {"$exists": True}}
    }
