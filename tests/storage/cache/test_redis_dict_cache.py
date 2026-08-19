"""``RedisDictCache`` must survive the values the results store actually caches.

``DefaultResultsStore`` caches the very dict it inserts into MongoDB, and since
0.2.4 that dict carries ``created_at`` as a ``datetime``. Plain ``json.dumps``
refuses a ``datetime``, and every caller of the cache swallows the failure —
``RedisCacheBase.set`` turns it into ``CacheOperationError`` and
``DefaultResultsStore._cache_set`` logs it as a warning — so the cache would
have gone quietly dead.
"""

import json
from datetime import UTC, datetime

import pytest

from warren.storage.cache.redis import RedisDictCache
from warren.storage.results.interface import ResultDoc


_MOMENT = datetime(2026, 8, 19, 15, 5, 7, tzinfo=UTC)


def _cache() -> RedisDictCache:
    return RedisDictCache(client=None, base_key="results:test")


def test_serialize_renders_a_datetime_as_iso_text() -> None:
    cache = _cache()

    payload = cache._serialize({"doc_id": "doc-1", "created_at": _MOMENT})

    assert json.loads(payload.decode("utf-8"))["created_at"] == _MOMENT.isoformat()


def test_a_cached_result_doc_round_trips_back_to_a_datetime() -> None:
    cache = _cache()
    doc = {
        "doc_id": "doc-1",
        "part_idx": 0,
        "job_id": "job-1",
        "result": {"x": 1},
        "created_at": _MOMENT,
    }

    restored = cache._deserialize(cache._serialize(doc))

    assert isinstance(restored["created_at"], str)
    assert ResultDoc(**restored).created_at == _MOMENT


def test_serialize_still_refuses_values_json_cannot_render() -> None:
    cache = _cache()

    with pytest.raises(TypeError, match="not JSON serializable"):
        cache._serialize({"result": object()})
