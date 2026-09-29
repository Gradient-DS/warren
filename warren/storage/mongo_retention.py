"""Configure TTL indexes on job records."""

from typing import TYPE_CHECKING

from pymongo.errors import OperationFailure


if TYPE_CHECKING:
    from pymongo.asynchronous.collection import AsyncCollection


async def configure_ttl_index(
    collection: "AsyncCollection", field: str, seconds: int | None
) -> None:
    """Replace conflicting indexes; remove TTL indexes when disabled."""
    indexes = await collection.index_information()
    keys = [(field, 1)]
    for name, spec in indexes.items():
        if spec["key"] != keys:
            continue
        if seconds is None and "expireAfterSeconds" not in spec:
            continue
        options = {key: value for key, value in spec.items() if key not in {"v", "key"}}
        if seconds is not None and options == {"expireAfterSeconds": seconds}:
            return
        try:
            await collection.drop_index(name)
        except OperationFailure as exc:
            if exc.code != 27:  # Another runner may have dropped the same index.
                raise
    if seconds is not None:
        await collection.create_index(field, expireAfterSeconds=seconds)
