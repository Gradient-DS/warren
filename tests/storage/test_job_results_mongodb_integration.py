"""Integration test for ``MongoDBJobResultsStore`` aggregations.

Runs against a real MongoDB on localhost:27017 and skips when none is
reachable, the same opt-in pattern ``test_topic_routing_integration.py``
uses for RabbitMQ. Aggregation expressions cannot be checked without a
server, and ``get_stage_counts`` shipped with ``soft_failed`` always 0
because nothing exercised it.
"""

import asyncio
import uuid

import pytest
from pymongo import AsyncMongoClient

from warren.storage.job_results.mongodb import MongoDBJobResultsStore


def _run_or_skip(scenario) -> None:
    async def wrapper() -> None:
        client = AsyncMongoClient("localhost", 27017, serverSelectionTimeoutMS=500)
        try:
            await client.admin.command("ping")
        except Exception:  # noqa: BLE001 - any failure means "no MongoDB", skip
            await client.close()
            pytest.skip("no MongoDB reachable on localhost:27017")
        database = f"warren_test_{uuid.uuid4().hex[:8]}"
        try:
            await scenario(client, database)
        finally:
            await client.drop_database(database)
            await client.close()

    asyncio.run(wrapper())


def test_stage_counts_count_soft_and_hard_failures() -> None:
    async def scenario(client: AsyncMongoClient, database: str) -> None:
        store = MongoDBJobResultsStore(client, database_name=database)
        await store.setup()
        await store.record_success("j", "text", "d1", "parser", "p-0")
        await store.record_soft_failure(
            "j", "text", "d2", "parser", "p-0", "chunker", "c-0", 1, "boom"
        )
        await store.record_hard_failure(
            "j", "text", "d3", "parser", "p-0", "chunker", "c-0", "dead"
        )

        assert await store.get_stage_counts("j") == [
            {
                "data_type": "text",
                "total": 3,
                "succeeded": 1,
                "soft_failed": 1,
                "hard_failed": 1,
            }
        ]

    _run_or_skip(scenario)


def test_unique_errors_use_last_soft_failure_or_hard_failure() -> None:
    async def scenario(client: AsyncMongoClient, database: str) -> None:
        store = MongoDBJobResultsStore(client, database_name=database)
        await store.setup()
        await store.record_soft_failure(
            "j", "text", "d2", "parser", "p-0", "chunker", "c-0", 1, "boom"
        )
        await store.record_soft_failure(
            "j", "text", "d2", "parser", "p-0", "chunker", "c-0", 2, "boom again"
        )
        await store.record_hard_failure(
            "j", "text", "d3", "parser", "p-0", "chunker", "c-0", "dead"
        )

        errors = await store.get_unique_errors("j")

        assert sorted(e["error"] for e in errors) == ["boom again", "dead"]
        assert all(e["count"] == 1 and e["data_type"] == "text" for e in errors)

    _run_or_skip(scenario)
