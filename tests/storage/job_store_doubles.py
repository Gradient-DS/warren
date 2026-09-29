import copy

from pymongo.errors import OperationFailure


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

    async def update_one(self, query: dict, update: dict, *, upsert: bool) -> None:
        assert upsert
        self.writes.append(copy.deepcopy({**query, **update["$set"]}))

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
