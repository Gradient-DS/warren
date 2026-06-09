# Retry Management — Design Document

**Date:** 11-03-2026 (design), 12-03-2026 (updated to reflect implementation)
**Authors:** @visionscaper (Freddy), Claude

---

## Problem Statement

When a worker encounters a transient failure (downstream service offline, temporary resource
exhaustion, rate limiting), the message should be retried after a delay. Workers raise
`SoftFailureException` to signal retry intent, but the system needs infrastructure for:
persistence, scheduled republishing, exponential backoff, and recovery after crashes.

Goals:
- Decouple retry scheduling from processing workers (workers should not block waiting for
  retry delays).
- Persist messages awaiting retry so they survive process restarts.
- Allow workers to influence retry parameters (delay, max retries, backoff strategy) based
  on domain knowledge.
- Use the existing fanout exchange for retry routing so all observers (future JobManager)
  can see failures.

## Architecture

```
                      Processing Exchange (fanout)
                     /        |          |         \
              [Parser]   [Chunker]  [Embedder]  [RetryWorker]
                              |                      |
                    SoftFailureException        Filters for
                              |              data_type: "soft-failure"
                    ConsumerManager                  |
                    wraps in envelope          +-----------+-----------+
                              |                |                       |
                    {data_type: "soft-failure"  Persist to         Schedule timer
                     data: <failed message>}    CachedDocumentStore (call_later)
                              |                                        |
                    Publishes through                             Timer fires
                    downstream publishers                              |
                    (same exchange)                          Fetch from store
                              |                             Republish to exchange
                              +------>  reaches all queues   Delete from store
                                        including RetryWorker
```

All workers share a single fanout exchange. Soft-failure envelopes are published through the
same downstream publishers that carry normal results. The RetryWorker is just another consumer
on the exchange, self-selecting for `data_type: "soft-failure"` messages.

## Design Decisions

1. **Single fanout exchange, no separate retry exchange** — soft failures are wrapped in
   an envelope (`data_type: "soft-failure"`) and published through the regular downstream
   publishers on the processing exchange. This means a future JobManagerWorker can observe
   failures on the same exchange without additional wiring. The RetryWorker has its own queue
   on this exchange (`jobs.retry_worker`) and filters for soft-failure messages.

2. **`asyncio.call_later` + persistence** — the RetryWorker schedules republishing via
   `asyncio.call_later`. On process restart, pending retries are loaded from the persistent
   store and remaining delays are rescheduled.

3. **RabbitMQ per-delay queues rejected** — per-message TTL with dead-letter routing requires
   creating queues per delay value. Impractical when different messages need different delays
   and backoff strategies.

4. **`CachedDocumentStore` as generic component** — conforms to the `DocumentStoreInterface`
   protocol and composes an injected `DocumentStoreInterface` (backing store) with a
   `CacheInterface[Dict]` (cache layer). Provides write-through on insert, read-through on
   get. Not retry-specific; usable anywhere persistence + fast cached retrieval is needed.

5. **Identity via `{job_id}:{doc_id}:{part_idx}`** — consistent with `DefaultResultsStore`
   key structure. Key construction is extracted into `build_message_key` in
   `workers/messages.py`, shared by `DefaultResultsStore` and `RetryWorker`.

6. **Worker sets retry parameters, consumer manager applies policy** — `SoftFailureException`
   carries the worker's retry intent (`retry_after`, `retry_max`, `backoff_base`, `jitter`).
   The consumer manager provides defaults via `RetryConfig`, applies caps, and calculates
   backoff with jitter.

7. **Delete from store after successful republish** — the RetryWorker removes the persisted
   message only after the publisher confirms successful republishing. On failure, the message
   stays in the store for recovery on next startup.

8. **On restart: load only retry scheduling metadata** — full message bodies stay in the store
   and are fetched on-demand when the timer fires. Keeps memory usage low during recovery.

9. **Soft-failure envelope wrapping** — the consumer manager wraps the entire failed message
   (with updated `retry` metadata) in an envelope:
   ```json
   {
       "data_type": "soft-failure",
       "data": { "...failed message with retry metadata..." },
       "job_id": "...",
       "origin": { "...origin metadata..." }
   }
   ```
   The RetryWorker unwraps `message["data"]` to get the original message for republishing.

## Components

### 1. SoftFailureException

**File:** `warren/common.py`

Workers raise `SoftFailureException` to signal retry intent with optional parameters:

```python
class SoftFailureException(Exception):
    def __init__(
        self,
        reason: str,
        *,
        retry_after: Optional[int] = None,
        retry_max: Optional[int] = None,
        backoff_base: Optional[float] = None,
        jitter: Optional[bool] = None,
        cause: Optional[Exception] = None,
    ) -> None: ...
```

- `retry_after` — suggested initial delay in seconds. Defaults to `RetryConfig.initial_delay`.
- `retry_max` — suggested max retry attempts. Capped by `RetryConfig.max_retries_cap`.
- `backoff_base` — base for exponential backoff: `delay = retry_after * backoff_base^(attempt-1)`.
- `jitter` — multiply delay by `random.uniform(0.5, 1.5)`.
- `cause` — sets `__cause__` for exception chaining (used internally by consumer manager
  when wrapping `PublishFailureException`).

### 2. Message Identity Utilities

**File:** `warren/workers/messages.py`

Standalone utility functions shared by `DefaultResultsStore`, `RetryWorker`, `RMQConsumerManager`,
and `RMQPublisher`.

```python
def build_message_key(
    job_id: Optional[str],
    doc_id: Optional[str],
    part_idx: int = 0,
) -> str:
    """Returns 'job:{job_id}:doc:{doc_id}:part:{part_idx}'."""

def build_message_key_prefix(
    doc_id: Optional[str],
    job_id: Optional[str],
) -> str:
    """Returns 'job:{job_id}:doc:{doc_id}:' for prefix queries."""

def extract_message_identity(message: Dict) -> str:
    """Returns 'job={job_id} doc={doc_id} part={part_idx}' for logging."""
```

`DefaultResultsStore._build_cache_key` and `_build_cache_prefix` delegate to these functions.

### 3. RetryConfig

**File:** `warren/pubsub/rabbitmq/aio_pika/consumer.py`

Configuration for retry behavior, owned by the consumer manager:

```python
class RetryConfig(BaseModel):
    initial_delay: int = 30       # Default delay when worker doesn't specify
    max_retries: int = 5          # Default max retries when worker doesn't specify
    backoff_base: float = 2.0     # Exponential backoff base
    jitter: bool = True           # Add random jitter to delays
    max_delay_cap: int = 300      # Maximum delay in seconds (caps backoff)
    max_retries_cap: int = 10     # Maximum retries allowed (overrides worker request)
    fallback_requeue_delay: float = 2.0  # Delay before nack+requeue when no publishers
```

### 4. Consumer Manager Retry Handling

**File:** `warren/pubsub/rabbitmq/aio_pika/consumer.py`

The consumer manager's `_handle_soft_failure` method builds the retry envelope and publishes
through the regular downstream publishers (not a separate retry publisher).

#### Flow

1. **No publishers configured** — sleep for `fallback_requeue_delay`, then `nack(requeue=True)`.
   Prevents tight retry loops when retry infrastructure is not wired up.

2. **With publishers** — build retry envelope and publish:
   a. Read existing `retry` state from the message (may be a re-retry).
   b. Increment count.
   c. Resolve delay via `_resolve_retry_after()` (worker intent → config defaults → caps).
   d. Resolve max via `_resolve_retry_max()` (worker intent capped by system limit).
   e. If `new_count > retry_max`: delegate to `_handle_hard_failure()` (message rejected).
   f. Update `body["retry"]` with new metadata (count, after, reason, max).
   g. Wrap in soft-failure envelope: `{data_type: "soft-failure", data: body, job_id, origin}`.
   h. Publish via downstream publishers (fanout exchange).
   i. Ack original message.

#### Retry parameter resolution

```python
def _resolve_retry_after(self, error: SoftFailureException, attempt: int) -> int:
    base = error.retry_after or self._retry_config.initial_delay
    exp_base = error.backoff_base or self._retry_config.backoff_base
    use_jitter = error.jitter if error.jitter is not None else self._retry_config.jitter
    delay = base * (exp_base ** (attempt - 1))
    if use_jitter:
        delay *= random.uniform(0.5, 1.5)
    return min(int(delay), self._retry_config.max_delay_cap)

def _resolve_retry_max(self, error: SoftFailureException) -> int:
    requested = error.retry_max or self._retry_config.max_retries
    return min(requested, self._retry_config.max_retries_cap)
```

### 5. RetryWorker

**File:** `warren/retry_management/retry_worker.py`

Consumes from the processing exchange, filters for `data_type: "soft-failure"` messages,
persists them, and schedules delayed republishing.

```python
class RetryWorker(AsyncProcessingWorkerBase):
    REQUIRED_DOC_ID_FIELD: str = "retry_key"

    def __init__(
        self,
        worker_id: str,
        *,
        retry_store: DocumentStoreInterface,
        republish_publisher: PublisherInterface,
        message_key_func: Optional[Callable[[Dict], str]] = None,
    ) -> None: ...
```

Constructor validates that the store's `doc_id_field` matches `"retry_key"`.

#### `__call__` — persist and schedule

1. Filter: skip messages where `data_type != "soft-failure"` (return None).
2. Unwrap: `failed_message = message["data"]`.
3. Extract retry metadata: `delay = failed_message["retry"]["after"]`.
4. Build and persist envelope (see structure below).
5. Schedule `asyncio.call_later(delay, _republish)`.
6. Return None (no downstream publish from consumer manager).

#### Persisted envelope structure

```json
{
    "retry_key": "job:abc123:doc:doc456:part:0",
    "message": { "...unwrapped failed message with retry metadata..." },
    "fire_at": 1741704030.0,
    "retry_after_seconds": 30
}
```

#### `_republish` — fetch, publish, delete

1. Fetch envelope from store (cache hit if CachedDocumentStore).
2. Publish unwrapped message to processing exchange.
3. Delete from store only after successful publish.
4. On publish failure: log error, leave message in store for recovery on next startup.

#### `schedule_pending` — startup recovery

Called after setup, before consuming starts:

1. Query store for all persisted envelopes.
2. For each: compute `remaining = fire_at - now`.
3. `remaining <= 0`: `create_task(_republish(key))` — republish immediately.
4. `remaining > 0`: `call_later(remaining, _republish)` — reschedule.
5. Log: "Scheduled N pending retries, M republished immediately".

#### `shutdown` — cancel timers

Cancels all pending `asyncio.TimerHandle` instances. Messages remain in the store for
recovery on next startup.

### 6. CachedDocumentStore

**File:** `warren/storage/cached_document_store.py`

Conforms to the `DocumentStoreInterface` protocol via structural typing (duck typing). Composes
an injected `DocumentStoreInterface` (backing store) with a `CacheInterface[Dict]` (cache layer).

```python
class CachedDocumentStore(Base):
    def __init__(
        self,
        store: DocumentStoreInterface,
        cache: CacheInterface[Dict],
        *,
        cache_ttl_seconds: Optional[int] = None,
    ) -> None: ...
```

| Method | Behavior |
|--------|----------|
| `insert` | Write to store, then cache the document (write-through) |
| `get_document` | Cache hit: return cached. Miss: fetch from store, cache it (read-through) |
| `update` | Update store, invalidate cache entry |
| `delete` | Delete from store, remove from cache |
| `exists` | Check cache first, fall back to store |
| `query` | Delegate to store (no caching for queries) |
| `get_document_type` | Delegate to store |
| `get_doc_id_field` | Delegate to store |
| `has_unique_index` | Delegate to store |

All cache operations are wrapped in try/except — cache failures are logged as warnings
and fall through to the backing store. The system degrades gracefully if Redis is unavailable.

### 7. RetryWorkerRunner

**File:** `warren/retry_management/retry_worker_runner.py`

Subclass of `WorkerRunnerBase` that wires up all retry infrastructure:

```python
class RetryWorkerRunner(WorkerRunnerBase):
    def __init__(
        self,
        worker_id: str,
        *,
        retry_store: DocumentStoreInterface,
        republish_publisher: PublisherInterface,
        consumer_manager_factory: ConsumerManagerFactory,
    ) -> None: ...
```

Setup flow:
1. Set up republish publisher.
2. Create `RetryWorker` with injected store and publisher.
3. Create consumer manager via factory, passing the RetryWorker as the consume function.
4. Set up consumer manager (declares exchange, queue, starts consuming).
5. Call `retry_worker.schedule_pending()` to recover persisted retries.

Teardown: calls `retry_worker.shutdown()` (cancel timers), then `publisher.teardown()`.

### 8. WorkerRunnerBase

**File:** `warren/workers/runners.py`

Generic lifecycle manager for workers. Refactored from an earlier version that had
connection/factory methods specific to the E2E test. Now provides:

- Abstract `setup()` method — subclasses wire their specific dependencies.
- `run()` — validates setup, installs signal handlers (SIGINT/SIGTERM), starts consuming,
  waits for shutdown event.
- `teardown()` — stops consumer, calls `_on_teardown()` hook, resets state.
- `_on_teardown()` — protected hook for subclass-specific cleanup.
- `_mark_setup_succeeded()` — must be called at end of `setup()`.

Type alias for dependency injection:
```python
ConsumerManagerFactory = Callable[[ConsumeMessageFunc], ConsumerManagerInterface]
```

## Data Flow

### Happy path (no retry)

```
Publisher ──> Processing Exchange ──> Worker ──> ACK ──> Downstream Publishers
```

### Retry path

```
1. Publisher ──> Processing Exchange ──> Worker (e.g., Chunker)
2. Worker raises SoftFailureException("Weaviate offline", retry_after=60, retry_max=5)
3. ConsumerManager._handle_soft_failure:
   a. Reads existing retry state (count=0 for first failure)
   b. Increments count to 1
   c. Calculates delay: 60 * 2.0^0 = 60s (+ optional jitter)
   d. Checks count (1) <= max (5): OK
   e. Updates body["retry"] = {count: 1, after: 60, reason: "Weaviate offline", max: 5}
   f. Wraps in envelope: {data_type: "soft-failure", data: body, job_id, origin}
   g. Publishes through downstream publishers (processing exchange)
   h. ACKs original message

4. RetryWorker receives envelope on processing exchange:
   a. Filters: data_type == "soft-failure" → process
   b. Unwraps: failed_message = envelope["data"]
   c. Builds retry_key: "job:abc:doc:123:part:0"
   d. Persists envelope to CachedDocumentStore (MongoDB + Redis)
   e. Schedules asyncio.call_later(60, _republish)
   f. Returns None (ACK, no downstream)

5. Timer fires after 60s:
   a. Fetches envelope from store (cache hit: fast)
   b. Publishes unwrapped message to processing exchange
   c. Deletes from store

6. Worker receives message again (retry.count=1):
   a. Processes successfully ──> ACK ──> downstream
   b. OR raises SoftFailureException again:
      ConsumerManager: count=2, delay=60*2.0^1=120s (+ jitter), ...
```

### Max retries exceeded

```
1. Worker raises SoftFailureException on attempt where new_count > retry_max
2. ConsumerManager._handle_soft_failure:
   a. new_count (6) > retry_max (5)
   b. Logs error with message identity [job=abc doc=123 part=0]
   c. Delegates to _handle_hard_failure (message rejected)
```

### Restart recovery

```
1. RetryWorkerRunner.setup() completes
2. Calls retry_worker.schedule_pending()
3. Queries retry store for all persisted envelopes
4. For each envelope:
   a. remaining = fire_at - now
   b. remaining <= 0: create_task(_republish(key)) — immediate
   c. remaining > 0: call_later(remaining, _republish) — rescheduled
5. Logs: "Scheduled N pending retries, M republished immediately"
6. start_consuming() begins — new retry messages processed normally
```

## E2E Test Infrastructure

The retry system is tested via the E2E test framework with failure injection:

- **`FailureSpec`** — dataclass specifying which attempts to fail at, target data type,
  and retry parameters.
- **`FailureInjector`** — wraps a worker's consume function, injects `SoftFailureException`
  at specified attempts for messages matching `target_data_type`. Required because the fanout
  topology means all message types reach all queues — the injector must only activate for
  messages the wrapped worker would actually process.
- **`E2ETestWorkerSpec`** — extends `WorkerSpec` with an optional `FailureSpec`.
- **`start_retry_worker.py`** — launches a `RetryWorkerRunner` with `CachedDocumentStore`
  (MongoDB + Redis), `RMQPublisher` targeting the processing exchange, and consumer on
  `jobs.retry_worker` queue.

Current test configuration: `fail_at_attempts=[1, 2]` with `retry_max=3` on the chunker
worker. Both fake (4 docs) and real (1 PDF) scenarios pass natively and on K8s.

See dev note "TODO — Retry E2E Test Scenarios" (12-03-2026) for untested paths.

## File Inventory

### New files (created for retry system)

| File | Description |
|------|-------------|
| `distributed/storage/cached_document_store.py` | `CachedDocumentStore` — write-through/read-through wrapper |
| `distributed/workers/retry_worker.py` | `RetryWorker` — persist, schedule, republish |
| `distributed/workers/retry_worker_runner.py` | `RetryWorkerRunner` — lifecycle wiring for RetryWorker |
| `distributed/e2e_test/start_retry_worker.py` | E2E launcher for RetryWorkerRunner |
| `distributed/e2e_test/failure_injection.py` | `FailureSpec`, `FailureInjector` for E2E testing |
| `distributed/e2e_test/purge_rmq.py` | RabbitMQ queue/exchange cleanup between test runs |

### Modified files

| File | Change |
|------|--------|
| `distributed/common.py` | `SoftFailureException` gains retry parameters and exception chaining |
| `distributed/workers/messages.py` | Added `build_message_key`, `build_message_key_prefix`, `extract_message_identity` |
| `distributed/workers/runners.py` | Refactored `WorkerRunnerBase` to generic lifecycle with `_on_teardown` hook |
| `distributed/storage/results/default.py` | `_build_cache_key` / `_build_cache_prefix` delegate to `build_message_key` |
| `distributed/pubsub/rabbitmq/consumer.py` | `RetryConfig`, soft-failure envelope wrapping, backoff resolution, identity logging |

## Known Shortcomings

### Timer-stealing interleaving on consecutive failures

**Severity:** Low (benign — no data loss, document still gets processed)

When a document fails multiple times in a row (e.g., `fail_at_attempts=[1, 2]`), a race
condition can cause an older timer's async task to "steal" a newer envelope:

1. Attempt 1 fails → envelope A stored, timer T1 scheduled (2s delay).
2. T1 fires → `_on_timer_fire` pops from `_pending_timers` and calls
   `asyncio.create_task(_republish)`, but the task doesn't execute yet.
3. Before the task runs, the republished message fails again (attempt 2) →
   `__call__` stores envelope B (overwrites A), schedules timer T2.
4. The async task from step 2 now runs, acquires the lock, reads envelope B
   (not A), republishes it, and deletes it.
5. T2 fires → `_republish` → `DocumentNotFoundError` because step 4 already
   deleted the envelope.

The `asyncio.Lock` serializes store operations but cannot prevent the gap between
`_on_timer_fire` (a synchronous `call_later` callback that creates the task) and the
task actually acquiring the lock. In that window, `__call__` can overwrite the envelope.

**Net effect:** The document is republished with a shorter delay than intended (T1's task
publishes the T2 envelope early). The stale timer T2 harmlessly skips.

**Fix (22-03-2026):** Added a monotonic per-key generation counter
(`_envelope_generation: Dict[str, int]`). Each `__call__` increments the generation and
stores it in the envelope. The generation is threaded through `call_later` →
`_on_timer_fire` → `_republish`. Before acting, `_republish` compares
`expected_generation` against the envelope's `generation` — if they differ, it skips.

An earlier attempt using `fire_at` float comparison failed because float equality is
fragile after JSON/BSON serialization round-trips. Integer generation counters are immune
to this. The `asyncio.Lock` is still required alongside the generation counter — they
solve different problems (lock: TOCTOU inside the critical section; generation: stale
timers before entering it).

A future refactor from `call_later` + sync callback to `create_task` + `asyncio.sleep`
would eliminate the sync-to-async gap entirely, making the generation counter unnecessary.

## Future Extensions

- **Dead letter tracking:** Hard failures are currently rejected and logged but not persisted.
  Add a dead letter collection so operators can query which documents permanently failed and
  why.

- **JobManager integration:** On max retries exceeded, publish a failure event to a job
  management exchange. The JobManager would track job-level completion/failure status.

- **Dead letter exchange:** Configure DLX on the retry queue for messages that the RetryWorker
  itself fails to process (deserialization errors, store failures).

- **Retry metrics:** Track retry counts, delays, success/failure rates per worker type for
  operational visibility.
