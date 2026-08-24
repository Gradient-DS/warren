# Document Fetching Design

## Problem

The distributed processing system lacks a formal way to register documents for processing, and workers have no location-agnostic mechanism to fetch document bytes. This creates several concrete problems:

### 1. No document registry

There is no formal record of what documents exist, where they live, and what format they are. Currently, the only way to make a document available to workers is to store its raw bytes directly in a results store — conflating input documents with processing outputs.

### 2. Documents are not results

Raw document bytes (PDFs, images, HTML) are *inputs* to the pipeline, not *outputs* of processing. Storing them in `DefaultResultsStore` (designed for processing results with composite keys like `(doc_id, job_id, part_idx)`) is a semantic mismatch. Documents should be registered as first-class entities with their own identity and metadata.

### 3. Location-agnostic document resolution

Workers should not know *where* documents live. In production, documents may reside on the local filesystem, at HTTP(S) URLs, or in cloud object storage (S3, GCS). The current approach hardcodes local file reading and byte storage. Adding a new document source should not require modifying workers.

### 4. Caching for reprocessing

When a worker fails and retries, or when the same document is reprocessed in a different job, fetching bytes from the origin (filesystem, HTTP, cloud) is wasteful. The system needs a binary cache so that resolved document bytes are available immediately on subsequent access. The existing `DefaultResultsStore` uses `RedisDictCache` (JSON-based), which cannot serialize bytes — binary payloads are never cached. See dev note [04-03-2026] "DefaultResultsStore cache does not support binary payloads".

### 5. Multi-worker access to the same document

In a fanout topology, multiple workers may need the original document bytes — not just the parser. For example, a future OCR worker or a metadata extraction worker. A shared, cached document fetching mechanism serves all workers without each needing its own storage integration.

### 6. Document lifecycle tracking (future)

A document registry provides the foundation for tracking document lifecycle: registered, processing, completed, failed. While lifecycle tracking is out of scope for this design, a registry is a prerequisite for it. Without a registry, there is no single source of truth about which documents are in the system and what state they are in.

## Design Decisions

1. **Document registration is job-agnostic.** A document exists independently of any job. The same document can be processed by multiple jobs with different parameters. The registry uses `doc_id` as the unique identifier — `job_id` lives only on messages and processing results.

2. **Locations are pure data.** `DocumentLocation` models describe *where* a document lives, with no I/O or behavior. Resolution is handled by separate resolver functions, keeping data and behavior decoupled.

3. **Resolver dispatch is injectable.** The fetcher receives a mapping of `location_type -> resolver function`. New location types are added by registering a resolver — no existing code changes.

4. **Cache key is `doc:<doc_id>:<job_id>`.** Document registration is job-agnostic, but the byte cache is job-scoped: re-submitting the same `doc_id` with updated content (a new job) must never serve bytes cached by a previous job, while retries within a job still hit the cache. Callers that pass no `job_id` fall back to the legacy shared `doc:<doc_id>` key (and accept its cross-job staleness).

5. **Default cache TTL is 24 hours.** Indefinite caching is not feasible for production workloads. A 24h default balances reprocessing speed with memory pressure. TTL is configurable via the factory function.

6. **Messages are trusted.** Workers use the location data from the message directly. The document registry exists for auditing and lifecycle tracking, not as a runtime lookup for workers.

7. **`resolve_path` supports a configurable base directory.** Relative paths in `DocumentPathLocation` are resolved against a base directory, bound via `functools.partial` at wiring time.

## Design

### 1. DocumentLocation — Location as Pure Data

A discriminated union of Pydantic models representing *where* a document lives. Locations are pure data — no I/O, no behavior.

```python
class DocumentLocation(BaseModel):
    """Where a document lives."""

    model_config = ConfigDict(frozen=True)
    location_type: str  # Pydantic discriminator


class DocumentPathLocation(DocumentLocation):
    """Document on the local filesystem."""

    location_type: Literal["path"] = "path"
    relative_path: str


class DocumentURLLocation(DocumentLocation):
    """Document at an HTTP(S) URL."""

    location_type: Literal["url"] = "url"
    url: str


class DocumentCloudLocation(DocumentLocation):
    """Document in cloud object storage (S3, GCS)."""

    location_type: Literal["cloud"] = "cloud"
    provider: Literal["s3", "gcs"]
    bucket: str
    key: str
    region: Optional[str] = None
```

For deserialization from a dict (e.g., from a message or document store), use a discriminated union type:

```python
AnyDocumentLocation = Annotated[
    Union[DocumentPathLocation, DocumentURLLocation, DocumentCloudLocation],
    Field(discriminator="location_type"),
]
```

Usage: `TypeAdapter(AnyDocumentLocation).validate_python(location_dict)`.

**File**: `storage/documents/location.py`

### 2. Document Registry — MongoDB Document Store

A `MongoDBDocumentStore` collection (`"documents"`) that records what documents exist. Each document entry contains identity, format, and location:

```python
{
    "doc_id": "report-123",
    "format": "pdf",
    "location": {
        "location_type": "path",
        "relative_path": "data/test_pdfs/report-123.pdf",
    },
}
```

This uses the existing `MongoDBDocumentStore` with:
- `doc_id_field="doc_id"`
- `unique_indexes=[("doc_id",)]`

The publisher creates entries here instead of storing bytes in a results store.

No new code needed for the store itself — `MongoDBDocumentStore` already supports this. We only need to create the instance with the right configuration.

### 3. Message Structure

The RMQ message `data` field carries the document identity, format, and location:

```python
{
    "data_type": "raw_document",
    "data": {
        "doc_id": "report-123",
        "format": "pdf",
        "location": {
            "location_type": "path",
            "relative_path": "data/test_pdfs/report-123.pdf"
        }
    },
    "job_id": "e2e-001",
    ...
}
```

The `ParserWorker` extracts the location from `msg.data["location"]`, deserializes it to the correct `DocumentLocation` subclass, and passes it to the `GetDocumentFunc`.

### 4. GetDocumentFunc — Cache-Enabled Document Fetching

A protocol for fetching document bytes by location, with transparent caching:

```python
class GetDocumentFunc(Protocol):
    async def __call__(
        self,
        doc_id: str,
        document_location: DocumentLocation,
    ) -> bytes: ...
```

**File**: `storage/documents/interface.py`

### 5. ResolveDocumentFunc — Location-Type Resolver

A type alias for a function that fetches bytes from a specific location type:

```python
ResolveDocumentFunc = Callable[[DocumentLocation], Awaitable[bytes]]
```

Each resolver is a standalone async function (or a `functools.partial` with bound config). No `self`, no state — just a function that takes a location and returns bytes. One resolver per `location_type`.

**File**: `storage/documents/interface.py`

### 6. CachedDocumentFetcher — Implementation

A concrete implementation that composes:
- A `RedisBinaryCache` (already exists, currently unused) for caching raw bytes
- An injectable resolver dispatch table for location-type-specific fetching

```python
class CachedDocumentFetcher(Base):
    """Fetches document bytes with cache-aside pattern.

    Uses RedisBinaryCache for caching and a dispatch table of resolver
    functions for location-type-specific fetching.
    """

    def __init__(
        self,
        *,
        cache: CacheInterface[bytes],
        resolvers: Mapping[str, ResolveDocumentFunc],
        name: Optional[str] = None,
    ) -> None: ...

    async def __call__(
        self,
        doc_id: str,
        document_location: DocumentLocation,
        *,
        job_id: str | None = None,
    ) -> bytes:
        cache_key = self._build_cache_key(doc_id, job_id)
        return await get_or_set(
            self._cache,
            cache_key,
            factory=lambda: self._resolve(document_location),
        )

    def _build_cache_key(self, doc_id: str, job_id: str | None) -> str:
        if job_id is None:
            return f"doc:{doc_id}"
        return f"doc:{doc_id}:{job_id}"

    async def _resolve(self, document_location: DocumentLocation) -> bytes:
        resolver = self._resolvers.get(document_location.location_type)
        if resolver is None:
            raise ValueError(
                f"No resolver for location type: {document_location.location_type}"
            )
        return await resolver(document_location)
```

**Cache key format**: `doc:<doc_id>:<job_id>` (job-scoped; `doc:<doc_id>` when no `job_id` is passed). The `RedisBinaryCache` base_key (e.g., `"documents"`) provides namespace isolation.

**File**: `storage/documents/fetcher.py`

### 7. Resolver Functions

Simple async functions, one per location type. Injected into the fetcher, not hardcoded:

```python
# Path resolver — base_dir bound via functools.partial at wiring time
async def resolve_path(location: DocumentLocation, *, base_dir: Path = Path()) -> bytes:
    """Read document bytes from the local filesystem.

    :param location: Must be a DocumentPathLocation.
    :param base_dir: Base directory prepended to relative_path.
    """
    path_location = cast(DocumentPathLocation, location)
    path = base_dir / path_location.relative_path
    return await asyncio.to_thread(path.read_bytes)


# URL resolver (future)
async def resolve_url(location: DocumentLocation) -> bytes: ...


# Cloud resolver (future)
async def resolve_cloud(location: DocumentLocation) -> bytes: ...
```

The dispatch table is assembled at wiring time:

```python
from functools import partial

resolvers = {
    "path": partial(resolve_path, base_dir=Path("/data/documents")),
}
```

**File**: `storage/documents/resolvers.py`

### 8. Factory Function

A convenience factory that assembles the `CachedDocumentFetcher` with its dependencies:

```python
def create_cached_document_fetcher(
    *,
    redis_client: Redis,
    resolvers: Mapping[str, ResolveDocumentFunc],
    cache_base_key: str = "documents",
    default_ttl_seconds: int = 86400,  # 24 hours
) -> CachedDocumentFetcher:
    cache = RedisBinaryCache(
        client=redis_client,
        base_key=cache_base_key,
        default_ttl_seconds=default_ttl_seconds,
    )
    return CachedDocumentFetcher(cache=cache, resolvers=resolvers)
```

**File**: `storage/documents/factories.py`

## Changes to Existing Code

### ParserWorker

Replace the `read_store` dependency with `get_document_func`:

```python
# Before
def __init__(self, ..., read_store: ResultsStoreInterface, ...):
    self._read_store = read_store

# After
def __init__(self, ..., get_document_func: GetDocumentFunc, ...):
    self._get_document_func = get_document_func
```

In `process()`:

```python
# Before
raw_doc = await self._read_store.get_result(doc_id=doc_id, job_id=job_id)
raw_bytes: bytes = raw_doc.result["raw_bytes"]

# After
location_dict = msg.data["location"]
location = TypeAdapter(AnyDocumentLocation).validate_python(location_dict)
raw_bytes = await self._get_document_func(doc_id, location)
```

**File**: `workers/parser_worker.py`

### publish_jobs.py

Replace results store usage with document store registration:

```python
# Before: store bytes in results store
raw_store = await create_default_results_store(...)
raw_bytes = pdf_path.read_bytes()
await raw_store.store(result={"raw_bytes": raw_bytes, ...}, ...)

# After: register document in document store
doc_store = MongoDBDocumentStore(
    client=mongo_client,
    database_name=mongo_cfg.database,
    collection_name="documents",
    doc_id_field="doc_id",
    unique_indexes=[("doc_id",)],
)
await doc_store.setup()

await doc_store.insert({
    "doc_id": doc_id,
    "format": "pdf",
    "location": DocumentPathLocation(relative_path=str(pdf_path)).model_dump(),
})

message = {
    "data_type": "raw_document",
    "data": {
        "doc_id": doc_id,
        "format": "pdf",
        "location": DocumentPathLocation(relative_path=str(pdf_path)).model_dump(),
    },
    "job_id": job_id,
    ...
}
```

**File**: `e2e_test/real/publish_jobs.py`

### WorkerSpec and WorkerFactory

Add `needs_document_fetcher` flag to `WorkerSpec` and broaden the factory signature:

```python
WorkerFactory = Callable[
    [str, Dict[str, ResultsStoreInterface], Optional[GetDocumentFunc]],
    ConsumeMessageFunc,
]


@dataclass(frozen=True)
class WorkerSpec:
    collections: Dict[str, str]
    factory: WorkerFactory
    terminal: bool = False
    needs_document_fetcher: bool = False
```

The runner creates the `CachedDocumentFetcher` once and passes it to factories whose spec has `needs_document_fetcher=True`, and `None` otherwise.

**Files**: `e2e_test/spec.py`, `e2e_test/runner.py`

### E2E Real Scenario

Update `_create_parser` to accept and inject the fetcher:

```python
# Before
def _create_parser(worker_id, stores):
    return ParserWorker(
        ..., read_store=stores["read"], write_store=stores["write"], ...
    )

# After
def _create_parser(worker_id, stores, get_document_func):
    return ParserWorker(
        ..., get_document_func=get_document_func, write_store=stores["write"], ...
    )
```

The parser `WorkerSpec` no longer needs a `"read"` collection:

```python
# Before
"parser": WorkerSpec(
    collections={"read": "raw_documents", "write": "parsed_documents"},
    factory=_create_parser,
)

# After
"parser": WorkerSpec(
    collections={"write": "parsed_documents"},
    factory=_create_parser,
    needs_document_fetcher=True,
)
```

**File**: `e2e_test/real/scenario.py`

### Cleanup

- Remove `"raw_documents"` from `ScenarioSpec.result_collections`
- The `DefaultResultsStore` for raw documents is no longer needed
- `publish_jobs.py` no longer imports `create_default_results_store`

## Package Structure

```
distributed/storage/documents/
    __init__.py
    design.md           # This document
    location.py          # DocumentLocation hierarchy + AnyDocumentLocation union
    interface.py         # GetDocumentFunc protocol, ResolveDocumentFunc type alias
    fetcher.py           # CachedDocumentFetcher implementation
    resolvers.py         # resolve_path (resolve_url, resolve_cloud: future)
    factories.py         # create_cached_document_fetcher
```

## Application: E2E Test Flow

### Publisher (publish_jobs.py)

```
1. Discover PDFs in directory
2. Create MongoDBDocumentStore for "documents" collection
3. For each PDF:
   a. Register document in MongoDB "documents" collection
      {doc_id, format, location: {location_type: "path", relative_path: ...}}
   b. Publish RMQ message
      {data_type: "raw_document", data: {doc_id, format, location: {...}}, job_id, ...}
```

No bytes are stored by the publisher. The document registry records *where* the
document is, not what it contains.

### Runner (runner.py)

```
1. Setup connections (MongoDB, Redis, RMQ)
2. Create results stores for each worker's "read"/"write" collections
3. Create CachedDocumentFetcher:
   a. Instantiate RedisBinaryCache (base_key="documents", default_ttl=24h)
   b. Register resolvers: {"path": partial(resolve_path, base_dir=...)}
   c. Assemble CachedDocumentFetcher
4. Create worker via factory:
   - Parser factory receives get_document_func (needs_document_fetcher=True)
   - Chunker/embedder factories receive None
5. Start consumer loop
```

### ParserWorker

```
1. Receive message: {data_type: "raw_document", data: {doc_id, format, location: {...}}}
2. should_process(): Check data_type, format support, OCR flag
3. process():
   a. Deserialize location dict -> DocumentPathLocation (via AnyDocumentLocation)
   b. Call get_document_func(doc_id, location, job_id=job_id)
      - CachedDocumentFetcher checks Redis binary cache for key "doc:<doc_id>:<job_id>"
      - On miss: dispatch to resolve_path -> read file -> cache in Redis -> return bytes
      - On hit: return cached bytes directly
   c. Dispatch to format processor (PdfProcessor)
   d. Store parse result in write_store (unchanged)
   e. Publish downstream message (unchanged)
```

### Retry within the same job

```
1. ParserWorker calls get_document_func(doc_id, location, job_id=job_id)
2. CachedDocumentFetcher finds bytes in Redis for "doc:<doc_id>:<job_id>" -> returns immediately
3. No filesystem/network read needed
```

### Re-submission of the same doc_id (new job, possibly updated content)

```
1. ParserWorker calls get_document_func(doc_id, location, job_id=<new job_id>)
2. Key "doc:<doc_id>:<new job_id>" is a miss -> resolver fetches fresh bytes
3. Stale bytes cached by the previous job are never served
```
