import copy

from pymongo.errors import OperationFailure
from pymongo.results import UpdateResult


class MongoCollection:
    def __init__(self) -> None:
        self.indexes: dict[str, dict] = {}
        self.dropped: list[str] = []
        self.writes: list[dict] = []

    async def index_information(self) -> dict:
        return copy.deepcopy(self.indexes)

    async def create_index(
        self, keys: str | list[tuple[str, int]], **options: object
    ) -> str:
        if isinstance(keys, str):
            keys = [(keys, 1)]
        spec = {"key": keys, **options}
        for name, existing in self.indexes.items():
            if existing["key"] == keys:
                if existing != spec:
                    msg = "index options conflict"
                    raise OperationFailure(msg, code=85)
                return name
        name = "_".join(f"{key}_{direction}" for key, direction in keys)
        self.indexes[name] = spec
        return name

    async def drop_index(self, name: str) -> None:
        self.dropped.append(name)
        del self.indexes[name]

    async def update_one(
        self, query: dict, update: dict, *, upsert: bool
    ) -> UpdateResult:
        assert upsert
        self.writes.append(copy.deepcopy({**query, **update["$set"]}))
        return UpdateResult({"n": 1, "nModified": 1}, acknowledged=True)

    async def insert_one(self, row: dict) -> None:
        self.writes.append(copy.deepcopy(row))


class MongoDatabase:
    def __init__(self) -> None:
        self.collections: dict[str, MongoCollection] = {}

    def __getitem__(self, name: str) -> MongoCollection:
        return self.collections.setdefault(name, MongoCollection())


class MongoClient:
    def __init__(self) -> None:
        self.databases: dict[str, MongoDatabase] = {}

    def __getitem__(self, name: str) -> MongoDatabase:
        return self.databases.setdefault(name, MongoDatabase())


class JobCollection(MongoCollection):
    def __init__(self) -> None:
        super().__init__()
        self.rows: dict[str, dict] = {}
        self.update_filters: list[dict] = []

    async def insert_one(self, row: dict) -> None:
        self.rows[row["job_id"]] = copy.deepcopy(row)

    async def find_one(
        self, query: dict, projection: dict | None = None
    ) -> dict | None:
        return copy.deepcopy(self.rows.get(query["job_id"]))

    async def update_one(
        self, query: dict, update: dict, *, upsert: bool = False
    ) -> UpdateResult:
        assert not upsert
        self.update_filters.append(copy.deepcopy(query))
        row = self.rows.get(query["job_id"])
        matches = row is not None and (
            "status.completed_at" not in query
            or row["status"].get("completed_at") == query["status.completed_at"]
        )
        if matches:
            for key, value in update["$set"].items():
                parent, _, child = key.partition(".")
                if child:
                    row[parent][child] = value
                else:
                    row[parent] = value
        return UpdateResult({"n": int(matches), "nModified": int(matches)}, True)


class Cursor:
    def __init__(self, rows: list[dict]) -> None:
        self.rows = rows

    async def to_list(self) -> list[dict]:
        return copy.deepcopy(self.rows)


class AggregateCollection(MongoCollection):
    def __init__(self, rows: list[dict]) -> None:
        super().__init__()
        self.rows = rows
        self.pipelines: list[list[dict]] = []

    async def aggregate(self, pipeline: list[dict]) -> Cursor:
        self.pipelines.append(copy.deepcopy(pipeline))
        return Cursor(self.rows)
