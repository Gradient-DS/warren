"""Storage: the store errors, request scoping and the helpers that the stores share.

Each store type is in its own subpackage, such as `warren.storage.jobs` and
`warren.storage.results`.

## Errors

- `ResourceNotFoundError`
- `ResourceAlreadyExistsError`
- `TransientStoreError`

## Retry on transient errors

- `run_with_transient_retry`
- `classify_transient`
- `classify_transient_methods`

## Scopes

- `current_scope`
- `get_current_scope`
- `require_scope`
- `valid_scope`
- `scope_fields`
- `scoped_cache_key`
- `ScopedDatabase`
- `ScopeClients`
- `erase_scope`

## Shared stores and helpers

- `CachedDocumentStore`
- `MemoryStoreRegistry`
- `configure_ttl_index`
"""

from warren.storage.cached_document_store import CachedDocumentStore
from warren.storage.exceptions import (
    ResourceAlreadyExistsError,
    ResourceNotFoundError,
    TransientStoreError,
)
from warren.storage.memory_registry import MemoryStoreRegistry
from warren.storage.mongo_errors import (
    classify_transient,
    classify_transient_methods,
)
from warren.storage.mongo_retention import configure_ttl_index
from warren.storage.retry import run_with_transient_retry
from warren.storage.scoping import (
    ScopeClients,
    ScopedDatabase,
    current_scope,
    erase_scope,
    get_current_scope,
    require_scope,
    scope_fields,
    scoped_cache_key,
    valid_scope,
)


__all__ = [
    "CachedDocumentStore",
    "MemoryStoreRegistry",
    "ResourceAlreadyExistsError",
    "ResourceNotFoundError",
    "ScopeClients",
    "ScopedDatabase",
    "TransientStoreError",
    "classify_transient",
    "classify_transient_methods",
    "configure_ttl_index",
    "current_scope",
    "erase_scope",
    "get_current_scope",
    "require_scope",
    "run_with_transient_retry",
    "scope_fields",
    "scoped_cache_key",
    "valid_scope",
]
