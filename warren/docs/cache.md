# Cache Interface Design

## Overview

This document describes the design of the async cache interface for the distributed document processing pipeline. The cache is intended to work alongside the document store (see `document_store/interface.py`) to cache data that is stored/updated in document stores.

All cache operations are async to avoid blocking the event loop in the async worker pipeline.

## Use Cases

1. **Dict caching**: Most workers cache and retrieve Dict data (document metadata, extracted text, processing results)
2. **Binary caching**: Some workers need to cache binary data (loaded PDFs, generated files)
3. **Hierarchical keys**: Data is often organized hierarchically, e.g., `<doc_id>.<chunk_idx>` for text chunks

## Interface Design

### Generic Async Protocol

The interface uses a generic type parameter `T` to support different value types. All methods are `async def` to ensure non-blocking I/O:

```python
class CacheInterface(Protocol[T]):
    async def get(self, key: str) -> Optional[T]: ...
    async def set(
        self, key: str, value: T, ttl_seconds: Optional[int] = None
    ) -> None: ...

    # ... etc
```

Python 3.9+ compatible syntax using `TypeVar` and `Generic`.

### Key Design Decisions

#### 1. `get()` returns `Optional[T]` instead of raising

Unlike document stores where a missing document is an error, cache misses are **expected** and common. The cache-aside pattern is:
```python
value = await cache.get(key)
if value is None:
    value = await store.get_document(key)
    await cache.set(key, value)
```

#### 2. Implementations have a `base_key` (namespace)

Similar to `collection_name` in `MongoDBDocumentStore`, cache implementations should accept a `base_key` parameter that namespaces all keys. This allows multiple caches to share the same Redis instance without key collisions.

#### 3. Hierarchical key support via prefix operations

Keys can be hierarchical using ":" as separator: `<doc_id>:<chunk_idx>` (e.g., "doc123:0", "doc123:1").

**IMPORTANT**: Prefix operations match literally with a wildcard appended. To match only sub-keys, include the trailing separator:
- `get_by_key_prefix("doc123")` → matches doc123, doc123:0, doc1234:0 (may be too broad!)
- `get_by_key_prefix("doc123:")` → matches doc123:0, doc123:1 only (sub-keys only)

Examples:
```python
# Store chunks with hierarchical keys
await cache.set("doc123:0", chunk0)
await cache.set("doc123:1", chunk1)

# Retrieve all chunks for doc123 (note trailing colon)
all_chunks = await cache.get_by_key_prefix("doc123:")

# Invalidate all chunks when document is reprocessed
await cache.delete_by_key_prefix("doc123:")
```

#### 4. Optional TTL per operation

Different data has different staleness tolerances. Per-key TTL provides flexibility. Implementations should have a default TTL when `None` is passed.

#### 5. Generic `CacheOperationError` exception

Any operation can raise `CacheOperationError` with optional `message` and `operation` parameters:
```python
raise CacheOperationError("Connection refused", operation="get")
# → "Cache operation 'get' failed: Connection refused"

raise CacheOperationError(operation="set")
# → "Cache operation 'set' failed"

raise CacheOperationError()
# → "Cache operation failed"
```

#### 6. `get_or_set` as standalone async function

Implements the cache-aside pattern. Defined as a function (not method) so users can use `functools.partial`. The factory parameter is async (`Callable[[], Awaitable[T]]`):
```python
from functools import partial

my_cache: CacheInterface[Dict] = ...
get_or_set_cached = partial(get_or_set, my_cache)

value = await get_or_set_cached("doc123", lambda: load_document("doc123"))
```

## Interface Methods

| Method | Return | Description |
|--------|--------|-------------|
| `async get(key)` | `Optional[T]` | Get value or None if not found |
| `async set(key, value, ttl_seconds)` | `None` | Store value with optional TTL |
| `async delete(key)` | `bool` | Delete key, returns True if existed |
| `async exists(key)` | `bool` | Check if key exists |
| `async get_by_key_prefix(key_prefix)` | `Dict[str, T]` | Get all items matching prefix |
| `async set_many(items, ttl_seconds)` | `None` | Store multiple key-value pairs |
| `async delete_by_key_prefix(key_prefix)` | `int` | Delete all items matching prefix, returns count |
| `async clear()` | `None` | Clear all entries in this cache namespace |

## Implementation Guide: Redis

### Architecture

```
CacheInterface[T] (Protocol)
       ↓
RedisCacheBase[T] (Abstract base class, derives from Base)
       ↓
   ┌───┴───┐
   ↓       ↓
DictCache  BinaryCache
(JSON)     (raw bytes)
```

### Base Class Responsibilities

`RedisCacheBase[T]` handles:

1. **Redis connection**: Accept an async Redis client instance (injected, not created internally)
2. **Key namespacing**: Prefix all keys with `base_key` + separator (e.g., `chunks:doc123:0`)
3. **TTL management**: Default TTL in constructor, per-operation override
4. **Serialization hooks**: Abstract methods `_serialize(value: T) -> bytes` and `_deserialize(data: bytes) -> T` (sync — CPU-only, no I/O)
5. **Error handling**: Catch Redis exceptions and wrap in `CacheOperationError`

### Constructor Pattern

```python
class RedisCacheBase(Base, ABC, CacheInterface[T]):
    def __init__(
        self,
        client: redis.asyncio.Redis,  # Async client, injected
        *,
        base_key: str,
        default_ttl_seconds: Optional[int] = None,
        key_separator: str = ":",
        name: Optional[str] = None,
    ) -> None: ...
```

### Specializations

**DictCache** (for JSON-serializable data):
```python
class RedisDictCache(RedisCacheBase[Dict]):
    def _serialize(self, value: Dict) -> bytes:
        return json.dumps(value).encode("utf-8")

    def _deserialize(self, data: bytes) -> Dict:
        return json.loads(data.decode("utf-8"))
```

**BinaryCache** (for raw bytes like PDFs):
```python
class RedisBinaryCache(RedisCacheBase[bytes]):
    def _serialize(self, value: bytes) -> bytes:
        return value  # Pass-through

    def _deserialize(self, data: bytes) -> bytes:
        return data  # Pass-through
```

### Redis Operations Mapping

| Interface Method | Redis Command(s) |
|-----------------|------------------|
| `get(key)` | `await GET` |
| `set(key, value, ttl)` | `await SET` with `EX` option |
| `delete(key)` | `await DEL` |
| `exists(key)` | `await EXISTS` |
| `get_by_key_prefix(prefix)` | `await SCAN` + `await MGET` |
| `set_many(items, ttl)` | `PIPELINE` with multiple `SET` + `await execute()` |
| `delete_by_key_prefix(prefix)` | `await SCAN` + `await DEL` |
| `clear()` | `await SCAN` + `await DEL` (for base_key namespace only) |

### Key Format

Full Redis key format: `{base_key}{namespace_separator}{key}`

The namespace separator (default ":") separates the base_key from user keys.
User keys can be hierarchical using ":" as hierarchy separator.

Example with `base_key="chunks"`, `namespace_separator=":"`:
- `await cache.set("doc123:0", chunk)` → Redis key: `chunks:doc123:0`
- `await cache.get_by_key_prefix("doc123:")` → Scans for `chunks:doc123:*` (sub-keys only)
- `await cache.get_by_key_prefix("doc123")` → Scans for `chunks:doc123*` (may match doc1234 too!)

### Error Handling

Wrap Redis exceptions in `CacheOperationError`. The original exception is preserved
in the chain via `from e`, so no need to duplicate the message:
```python
from redis.exceptions import RedisError


async def get(self, key: str) -> Optional[T]:
    try:
        data = await self._client.get(self._full_key(key))
        if data is None:
            return None
        return self._deserialize(data)
    except RedisError as e:
        raise CacheOperationError(operation="get") from e
```

## File Structure

```
warren/storage/
├── cache/
│   ├── __init__.py
│   ├── interface.py      # CacheInterface protocol, CacheOperationError, get_or_set
│   ├── design.md         # This document
│   └── redis.py          # Async Redis implementation (redis.asyncio)
├── document_store/
│   ├── __init__.py
│   ├── interface.py      # DocumentStoreInterface protocol (async)
│   └── mongodb.py        # Async MongoDB implementation (pymongo.AsyncMongoClient)
└── results/
    ├── __init__.py
    ├── interface.py       # ResultsStoreInterface protocol (async)
    ├── default.py         # DefaultResultsStore (async, composes doc store + cache)
    └── factories.py       # Async factory function
```

## Related Files

- `warren/storage/document_store/interface.py` - Document store protocol
- `warren/storage/document_store/mongodb.py` - Async MongoDB implementation
- `warren/common.py` - Worker definitions
- `warren/workers/workers.py` - Worker implementations

## Dependencies

Declared in `pyproject.toml`:
```
redis>=4.0.0       # includes redis.asyncio
pymongo==4.10.1    # includes AsyncMongoClient
```

Note: `motor` is deprecated (May 2025) in favor of pymongo's built-in async API.

## Future Considerations

1. **Atomic get_or_set**: Could use Redis Lua scripts for true atomicity
2. **Cache statistics**: Hit/miss ratios, latency tracking
3. **Compression**: For large values, consider compression before storage
4. **Connection pooling**: Redis client should use connection pooling for concurrent access
