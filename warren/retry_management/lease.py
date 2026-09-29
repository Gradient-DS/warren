"""Exclusive scheduling lease for a retry collection."""

from typing import Protocol

from datetime import UTC, datetime

from pymongo import AsyncMongoClient
from pymongo.errors import DuplicateKeyError
from pymongo.write_concern import WriteConcern


class RetryLeaseInterface(Protocol):
    async def acquire_or_renew(
        self, holder_id: str, now: float, expires_at: float
    ) -> bool:
        """Claim an expired lease or extend this holder's lease."""
        ...


class MongoRetryLease:
    def __init__(
        self,
        client: AsyncMongoClient,
        *,
        database_name: str,
        collection_name: str,
    ) -> None:
        self._collection = client[database_name][
            f"{collection_name}_lease"
        ].with_options(write_concern=WriteConcern(w="majority"))

    async def acquire_or_renew(
        self, holder_id: str, now: float, expires_at: float
    ) -> bool:
        try:
            result = await self._collection.update_one(
                {
                    "_id": "retry_worker",
                    "$or": [
                        {"holder_id": holder_id},
                        {"expires_at": {"$lte": datetime.fromtimestamp(now, UTC)}},
                    ],
                },
                {
                    "$set": {
                        "holder_id": holder_id,
                        "expires_at": datetime.fromtimestamp(expires_at, UTC),
                    }
                },
                upsert=True,
            )
        except DuplicateKeyError:
            # The fixed _id prevents an upsert while another holder owns the lease.
            return False
        return result.matched_count > 0 or result.upserted_id is not None
