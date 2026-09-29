from typing import Self

import copy
from collections.abc import AsyncIterator

from bson import ObjectId
from pymongo import ReplaceOne
from pymongo.results import BulkWriteResult


class MongoClient:
    def __init__(self) -> None:
        self.rows: list[dict] = []
        self.bulk_calls = 0
        self.single_calls = 0
        self.query_calls = 0

    def __getitem__(self, key: str) -> Self:
        return self

    async def create_index(self, keys: object, *, unique: bool) -> None:
        pass

    async def index_information(self) -> dict:
        return {
            "unique": {
                "key": [(k, 1) for k in ("doc_id", "job_id", "part_idx")],
                "unique": True,
            }
        }

    def _replace(self, query: dict, row: dict) -> tuple[dict, bool]:
        for index, existing in enumerate(self.rows):
            if all(existing.get(key) == value for key, value in query.items()):
                replacement = copy.deepcopy(row)
                replacement.setdefault("_id", existing["_id"])
                assert replacement["_id"] == existing["_id"]
                self.rows[index] = replacement
                return copy.deepcopy(replacement), False
        replacement = copy.deepcopy(row)
        replacement.setdefault("_id", ObjectId())
        self.rows.append(replacement)
        return copy.deepcopy(replacement), True

    async def find_one_and_replace(
        self, query: dict, row: dict, *, upsert: bool, return_document: bool
    ) -> dict:
        assert upsert
        self.single_calls += 1
        return self._replace(query, row)[0]

    async def bulk_write(
        self, operations: list[ReplaceOne], *, ordered: bool
    ) -> BulkWriteResult:
        assert ordered is False
        self.bulk_calls += 1
        upserted = []
        for index, operation in enumerate(operations):
            assert isinstance(operation, ReplaceOne)
            assert operation._upsert is True
            assert set(operation._filter) == {"doc_id", "job_id", "part_idx"}
            row, inserted = self._replace(operation._filter, operation._doc)
            if inserted:
                upserted.append({"index": index, "_id": row["_id"]})
        return BulkWriteResult({"upserted": upserted}, acknowledged=True)

    def find(self, params: dict) -> "Cursor":
        self.query_calls += 1
        return Cursor(
            [
                copy.deepcopy(row)
                for row in self.rows
                if all(row.get(key) == value for key, value in params.items())
            ]
        )


class Cursor:
    def __init__(self, rows: list[dict]) -> None:
        self.rows = rows

    def __aiter__(self) -> AsyncIterator[dict]:
        async def iterate() -> AsyncIterator[dict]:
            for row in self.rows:
                yield row

        return iterate()


class RedisClient:
    def __init__(self) -> None:
        self.entries: dict[str, bytes] = {}
        self.ttls: dict[str, int | None] = {}
        self.pipeline_calls = 0
        self.execute_calls = 0

    async def set(self, key: str, value: bytes, *, ex: int | None) -> None:
        self.entries[key] = value
        self.ttls[key] = ex

    async def get(self, key: str) -> bytes | None:
        return self.entries.get(key)

    def pipeline(self) -> "Pipeline":
        self.pipeline_calls += 1
        return Pipeline(self)


class Pipeline:
    def __init__(self, client: RedisClient) -> None:
        self.client = client
        self.pending: list[tuple[str, bytes, int | None]] = []

    async def set(self, key: str, value: bytes, *, ex: int | None) -> None:
        self.pending.append((key, value, ex))

    async def execute(self) -> None:
        self.client.execute_calls += 1
        for key, value, ex in self.pending:
            await self.client.set(key, value, ex=ex)
