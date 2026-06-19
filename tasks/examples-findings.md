# Warren examples & OSS-readiness — investigation findings

Branch: `feat/examples`. Date: 2026-06-19.

## 1. Exchange support — can we use anything beyond fanout?

### What the transport layer supports (today, works)
- `RMQExchangeConfig.type` is `Literal["topic", "fanout"]` (`warren/pubsub/rabbitmq/config.py:35`). `direct` and `headers` are **not** allowed by the config model (pydantic rejects them at load), even though `aio_pika` itself supports all four.
- The publisher (`RMQPublisher`) already supports static `route` and dynamic `route_func` routing (`publisher.py:46-136`), and `topology.declare_queue` already binds queues with a `routing_key` (`topology.py:43-73`). So the **plumbing for topic routing exists at the transport layer**.

### What actually works end-to-end through the runtime (the gap)
**Topic does NOT work out of the box.** The runtime runners never wire routing keys:
- Every runner builds `RMQQueueConfig(name, durable)` with **no `routing_key`** (`runner.py:301-304` + the 3 internal runners) → a topic queue binds on `""` and matches nothing.
- Every runner builds `RMQPublisher(exchange_config=...)` with **neither `route` nor `route_func`** (`runner.py:287-290`). For a topic exchange this **crashes at worker setup**: `ValueError("Topic exchanges require a route or route_func.")` (`publisher.py:66-68`).
- There is **no concrete `RouteFunc` implementation anywhere** in the repo, and no field on `WorkerSpec`/YAML to supply a binding key or route.

### Multiple exchanges at once (fanout + topic)
- **Publishing to two exchanges**: nearly free. Publishers are a `list` end-to-end (`base.py:63-70`, `consumer.py:264-267`); each `RMQPublisher` owns one exchange. Only `_create_publishers()` (`runner.py:282-291`) hardcodes a single publisher. One runner change → a worker can publish to a fanout AND a topic exchange.
- **Consuming from two exchanges**: NOT supported. A `ConsumerManager` models exactly one exchange + one queue + one bind (`config.py:82-85`, `consumer.py:123-132`, `topology.py:58-65`). Needs new structure (multiple consumer managers per worker, or a list of bindings).

### `should_process()` vs routing-key filtering
Complementary, not redundant. Today fanout means the broker does no filtering and `should_process()` (Python-side, on `data_type`) does all selection. A topic exchange would push the coarse `data_type` cut to the broker while `should_process()` still handles finer self-selection (`preprocessing_*` etc.).

### Verdict / recommendation on exchanges
- **Do we need all 4?** No. `direct` is a degenerate topic (exact match) and `headers` doesn't fit the self-selecting model — both add surface area for little teaching value. The meaningful axis is **fanout (broadcast + self-select) vs topic (broker-side routing)**.
- A **fanout** example works today. **Topic** and **fanout+topic** examples require minimal framework changes first (see §4).

## 2. Existing example & authoring contract (recap)
- `examples/fake/`: 3 async workers (parse→chunk→embed) subclassing `FilteringWorkerBase`; `should_process()` + `async process()`. Spec exports `PIPELINE: PipelineSpec`; config in `config.yaml` (fanout `jobs` exchange). Publish via `examples/fake/publish_jobs.py`. Queue name = `f"{exchange.name}.{worker_type}"`.
- **Sync path exists** (`SyncProcessingWorkerBase`, `workers.py:120`) and dispatch is automatic via `inspect.iscoroutinefunction` (`consumer.py:217-240`) — but **no example uses it**. Good candidate for the "async + sync" ask.
- **Processor protocol** (`warren/processors/base.py`) is a pure-sync transform unit, **unused by any example** — good candidate to demonstrate the worker/processor split.
- **Inspecting a running job**: storage read API exists (`get_status`, `get_stage_counts`, `get_doc_status`, `get_failures` on the job-results/job stores) but **no CLI/script uses it**. Today you "watch the terminals." A small `inspect_job.py` polling `metadata.job_name` → `get_status`/`get_stage_counts` is net-new and high value.

## 3. OSS-readiness cleanup (no blockers, but visible rough edges)
- **Stale docs referencing a pre-rename layout** (`distributed/`, `e2e_test/`, nonexistent `check_completion.py`): `retry_design.md:449-454`, `document_store.md:343,365,402`, `USAGE.md:41,342`. These actively mislead.
- **Internal dev-artifacts in public docs**: dated/authored RFC header + internal note ref in `retry_design.md:3-4,441`; unanswered "Open Questions" in `rabbitmq.md`; "see TODOs" pointers in `USAGE.md:41,60`.
- **Identities**: `CODEOWNERS` → `@LexLubbers`; author handles in `retry_design.md`; `TODO(ben, 2026-04-24)` in `resolvers.py:15`.
- **Dead code**: `warren/storage/documents/sources.py` (`discover_local_pdfs`/`discover_gcs_pdfs`) imported nowhere.
- **`warren/__init__.py` empty** → `import warren` exposes nothing, no `__version__`.
- **Tests**: 9 pass on 3.12. **Zero coverage of `warren/pubsub/`** — the headline fanout feature is untested. CI is 3.12-only.
- **Deps clean**: `basics` comes from public `visionscaper-pybase` (MIT). All deps MIT/Apache-2.0. `guest/guest` are RabbitMQ dev defaults, not leaks. `e2e.env` committed but harmless.

## 4. Framework changes needed to enable topic / fanout+topic examples
1. Add an optional per-worker **binding key** to `WorkerSpec` (and YAML) → pass into `RMQQueueConfig(routing_key=...)` in the runners.
2. Add a publisher **route source** — either a static key per worker or (better) a reusable `RouteFunc` deriving the key from message content (e.g. `data_type`) → wire into `RMQPublisher`. Need to author the first concrete `RouteFunc`.
3. Internal framework workers (job_publication/status/retry) observe *all* messages → need a `#` wildcard binding under topic.
4. For fanout+topic publishing: make `_create_publishers()` return a configurable list.
5. (Optional) widen the `Literal` if we ever want `direct` — not recommended now.

## 5. Recommended example set (proposal)
- **Example A — fanout (works today)**: promote/clean `examples/fake` into a documented "fanout" get-started, add a sync worker + an `inspect_job.py`.
- **Example B — topic**: a RAG-style pipeline routing by `data_type` via routing keys (needs §4.1–4.2).
- **Example C — fanout + topic together**: workers self-select on fanout while a topic exchange feeds a side-channel (e.g. audit/notify) (needs §4.4).
- Decision pending: do we invest in the §4 framework changes now, or ship fanout-only examples for the OSS launch and defer topic?
</content>
</invoke>
