import asyncio
import copy

from pymongo.results import DeleteResult, UpdateResult

from tests.storage.results.doubles import MongoClient as ResultCollection
from tests.storage.results.doubles import RedisClient as ResultRedis


class Collection(ResultCollection):
    def __init__(self) -> None:
        super().__init__()
        self.indexes: dict[str, dict] = {}
        self.index_calls = 0

    async def create_index(
        self, keys: str | list[tuple[str, int]], **options: object
    ) -> str:
        await asyncio.sleep(0)
        self.index_calls += 1
        if isinstance(keys, str):
            keys = [(keys, 1)]
        name = str(keys)
        self.indexes[name] = {"key": keys, **options}
        return name

    async def index_information(self) -> dict:
        return copy.deepcopy(self.indexes)

    async def insert_one(self, row: dict) -> None:
        self.rows.append(copy.deepcopy(row))

    async def find_one(
        self, query: dict, projection: dict | None = None
    ) -> dict | None:
        return next(
            (copy.deepcopy(row) for row in self.rows if self.matches(row, query)), None
        )

    @staticmethod
    def matches(row: dict, query: dict) -> bool:
        return all(row.get(key) == value for key, value in query.items())

    async def count_documents(self, query: dict, *, limit: int = 0) -> int:
        count = sum(self.matches(row, query) for row in self.rows)
        return min(count, limit) if limit else count

    async def update_one(
        self, query: dict, update: dict, *, upsert: bool = False
    ) -> UpdateResult:
        row = next((row for row in self.rows if self.matches(row, query)), None)
        found = row is not None
        if row is None and upsert:
            row = copy.deepcopy(query)
            self.rows.append(row)
        if row is not None:
            row.update(copy.deepcopy(update.get("$set", {})))
            for key in update.get("$unset", {}):
                row.pop(key, None)
            for key, value in update.get("$push", {}).items():
                row.setdefault(key, []).append(value)
        return UpdateResult({"n": int(found), "nModified": int(found)}, True)

    async def delete_one(self, query: dict) -> DeleteResult:
        for index, row in enumerate(self.rows):
            if self.matches(row, query):
                self.rows.pop(index)
                return DeleteResult({"n": 1}, True)
        return DeleteResult({"n": 0}, True)

    async def delete_many(self, query: dict) -> DeleteResult:
        before = len(self.rows)
        self.rows = [row for row in self.rows if not self.matches(row, query)]
        return DeleteResult({"n": before - len(self.rows)}, True)


class Database:
    def __init__(self, name: str) -> None:
        self.name = name
        self.collections: dict[str, Collection] = {}

    def __getitem__(self, name: str) -> Collection:
        return self.collections.setdefault(name, Collection())

    async def list_collection_names(self) -> list[str]:
        return list(self.collections)

    async def drop_collection(self, name: str) -> None:
        self.collections.pop(name, None)


class MongoClient:
    def __init__(self) -> None:
        self.databases: dict[str, Database] = {}

    def __getitem__(self, name: str) -> Database:
        return self.databases.setdefault(name, Database(name))

    async def drop_database(self, name: str) -> None:
        self.databases.pop(name, None)


class RedisClient(ResultRedis):
    async def delete(self, *keys: str) -> int:
        count = 0
        for key in keys:
            count += self.entries.pop(key, None) is not None
        return count

    async def exists(self, key: str) -> int:
        return int(key in self.entries)
