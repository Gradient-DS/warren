# DefaultResultsStore Design

## Introduction

In distributed document processing pipelines, workers process documents and produce results that need to be stored and retrieved efficiently. For example:

- A **PDF parser worker** processes a document and produces a single Markdown result
- A **chunking worker** splits a document into multiple parts, each stored separately
- A **downstream worker** needs to retrieve all chunks for a document to continue processing

The `ResultsStoreInterface` provides a unified API for storing and retrieving these results, with optional caching for efficient repeated access (e.g., when a job needs to be re-run).

## Overview

`DefaultResultsStore` implements `ResultsStoreInterface` for storing and retrieving document processing results.

**Dependencies**:
- `DocumentStoreInterface`: Persistence layer (injected)
- `CacheInterface[Dict]`: Optional caching layer (injected)

## Design Decisions

### 1. Query by Business Keys (Not Internal IDs)

All retrieval uses business keys `(doc_id, part_idx, job_id)`, not internal storage IDs.

**Rationale**:
- Callers know their document ID, part index, and job ID from processing context
- Internal storage IDs are implementation details of the document store
- Simplifies the API - no need to track result IDs

### 2. Document Structure

```python
{
    "doc_id": "original-doc-id",  # Required, for document association
    "part_idx": 0,  # Part index (0 for single-part results)
    "job_id": None,  # Optional (None if no job grouping)
    "result": {...},  # The actual processing result
    "result_metadata": {...},  # Optional metadata about processing
    "created_at": "ISO-8601-timestamp",  # Audit field
    "result_id": "store-generated-id",  # Added after storage, from doc_id_field
}
```

ID generation is delegated to the injected `DocumentStoreInterface`.

### 3. ResultDoc Model

Results are returned as `ResultDoc` Pydantic models (not raw dicts):

```python
class ResultDoc(BaseModel):
    doc_id: str
    part_idx: int = 0
    job_id: Optional[str] = None
    result: Dict
    result_metadata: Optional[Dict] = None
    created_at: Optional[str] = None
    result_id: Optional[str] = None
```

### 4. Part Index Normalization

- `part_idx` is always stored as an `int` (never `None`)
- When callers pass `part_idx=None`, it is normalized to `0`
- Single-part results have `part_idx=0`
- Multi-part results have `part_idx=0, 1, 2, ...`

This simplifies querying and ensures consistency between cache keys and storage.

### 5. Cache Strategy

- Cache stores the **complete document**, not just the result field
- This ensures consistency between cache and store retrieval
- Cache failures are silently ignored (with TODO for warning logs)

**Cache key format** (ordered by hierarchy: job -> doc -> part):

```
result:<result_type>:job:<job_id or ->:doc:<doc_id>:part:<part_idx>
```

**None representation**:
- `job_id` (string): Use `-` for None (None is a value, not a wildcard)
- `part_idx` (int): Always an integer (0 for single-part results)

**Examples**:
- `result:parsed_md:job:abc123:doc:doc456:part:0` (single result with job)
- `result:parsed_md:job:-:doc:doc456:part:0` (no job_id - this is a concrete value)
- `result:chunks:job:abc123:doc:doc456:part:3` (chunked result, part 3)

**Prefix scanning** (always includes job):
- Results for doc without job: `result:<result_type>:job:-:doc:<doc_id>:`
- Results for doc with specific job: `result:<result_type>:job:<job_id>:doc:<doc_id>:`

### 6. Uniqueness Constraint

**Problem**: Without a unique constraint on `(doc_id, job_id, part_idx)`, duplicates could be created.

**Solution**: Require a unique composite index on `(doc_id, job_id, part_idx)` in the document store. This:
- Enforces uniqueness at the database level
- Makes `overwrite_existing=True` work correctly (upsert by business keys)
- Enables efficient combined queries

The constructor validates this requirement via `has_unique_index()`.

## Interface

### ResultsStoreInterface

```python
class ResultsStoreInterface(Protocol):
    def store(
        self,
        result: Dict,
        doc_id: str,  # Required
        part_idx: Optional[int] = None,  # Normalized to 0 if None
        job_id: Optional[str] = None,
        result_metadata: Optional[Dict] = None,
        do_cache: bool = True,
        overwrite_existing: bool = True,
    ) -> str: ...

    def get_result(
        self,
        doc_id: str,
        part_idx: Optional[int] = None,  # Normalized to 0 if None
        job_id: Optional[str] = None,
    ) -> ResultDoc: ...

    def stream_doc_processing_results(
        self,
        doc_id: str,
        job_id: Optional[str] = None,
        try_cache: bool = True,
    ) -> Generator[ResultDoc, None, None]: ...
```

### DocumentStoreInterface Extensions

```python
# Type alias for index specification
IndexSpec = Union[str, Tuple[str, ...]]


def get_document_type(self) -> str:
    """Returns the document type identifier for this store."""
    ...


def get_doc_id_field(self) -> str:
    """Returns the field name used for document IDs."""
    ...


def has_unique_index(self, index_spec: IndexSpec) -> bool:
    """Check if a unique index exists on the given fields."""
    ...
```

## Implementation Details

### Constructor

```python
def __init__(
    self,
    document_store: DocumentStoreInterface,
    cache: Optional[CacheInterface[Dict]] = None,
    result_type: Optional[str] = None,
) -> None:
```

**Parameters**:
- `document_store`: Injected storage backend. Must have unique index on `(doc_id, job_id, part_idx)`.
- `cache`: Optional cache for read-through/write-through caching
- `result_type`: Type identifier for cache keys. If None, uses `document_store.get_document_type()`.

**Validation**: Raises `ValueError` if the document store lacks the required unique index.

### Method Behavior

**store()**:
1. Normalize `part_idx` (None → 0)
2. Build document with all fields + `created_at` timestamp
3. Insert via document store
4. Add `result_id` from insert return value
5. Optionally cache the complete document
6. Return the document ID

**get_result()**:
1. Normalize `part_idx` (None → 0)
2. Try cache first (if available)
3. On cache miss, query document store
4. Add `result_id` from doc_id_field
5. Populate cache on miss
6. Return `ResultDoc`

**stream_doc_processing_results()**:
1. If `try_cache=True`, try cache prefix scan first
2. If no cache results, query document store and cache each result
3. Yield `ResultDoc` for each result
4. Raise `DocumentProcessingResultsNotFound` if no results

## Resolved Design Decisions

1. **Query by business keys**: All retrieval uses `(doc_id, part_idx, job_id)`, not internal IDs
2. **Return type**: Methods return `ResultDoc` (Pydantic model), not raw dicts
3. **Cache stores full documents**: Ensures consistency between cache and store retrieval
4. **Cache key format**: `result:<result_type>:job:<job_id or ->:doc:<doc_id>:part:<part_idx>` (hierarchy: job -> doc -> part)
5. **None representation**: `job_id` uses `-`; `part_idx` is always an int (normalized from None to 0)
6. **job_id=None semantics**: None is a concrete value (maps to `-`), not a wildcard for "all jobs"
7. **part_idx normalization**: `None` → `0` at storage and query time; always stored as int
8. **ID generation**: Delegated to injected `DocumentStoreInterface`
9. **result_id extraction**: Uses `get_doc_id_field()` to extract storage ID into `result_id`
10. **Uniqueness enforcement**: Validated at construction via `has_unique_index()`, raises `ValueError` if missing
11. **Streaming cache**: Controlled by `try_cache` parameter, cache-first when enabled; caches on read from store
12. **Cache failures**: Silently ignored to not break main flow (TODO: add warning logs)
