# Phase 1 implementation plan — exchange model

Goal: exchanges in `PipelineSpec`, per-worker consume + `publish` list, `binding_key` + injected
`route_func`, provided `MessageFieldRouter`, all 4 runners wired, deploy-time `validate_pipeline`.
Verify on Docker with fanout + topic examples.

## Competing consumers / KEDA (confirmed)
Binding ≠ consuming. One queue `{exchange}.{worker_type}` with one binding; N instances consume it →
competing consumers, broker round-robins by prefetch. Works the same on fanout/direct/topic. KEDA
scales on queue depth. No per-instance bindings. Already the framework's structure.

## File-by-file changes

### A. `warren/pubsub/rabbitmq/config.py`
- `RMQExchangeConfig.type`: widen `Literal["topic","fanout"]` → `Literal["fanout","direct","topic"]`.
  (`headers` deferred.)

### B. `warren/pubsub/rabbitmq/aio_pika/topology.py`
- `declare_queue`: keep "reject non-empty routing_key on fanout". Direct/topic allow keys (already does).
  No real change beyond confirming direct passes through.

### C. `warren/pubsub/rabbitmq/aio_pika/publisher.py`
- Generalize validation: `if type == "topic" and not route...` → `if type != "fanout" and not route...`
  (direct also needs a route/route_func). Keep `if type == "fanout" and has_static: error`.

### D. `warren/pubsub/routing.py` (already drafted)
- Keep `MessageFieldRouter`. **Remove `topic_route_func`** — we decided routing is injected per
  worker via `PublishSpec`, NOT auto-attached by exchange type. (Simplifies; no hidden policy.)
- `RoutingPlanRouter` lands in Phase 2.

### E. `warren/runtime/config.py` (BREAKING)
- Remove `exchange` from `RuntimeRMQConfig` → leaves `connection`, `consumer`.
- ⚠️ ISSUE 1 (see below): support workers read `config.rabbitmq.exchange` today.

### F. `warren/runtime/spec.py`
- Add `PipelineSpec.exchanges: dict[str, RMQExchangeConfig]`.
- Add `PipelineSpec.default_exchange: str` — the exchange support workers observe in Phase 1
  (until multi-bind lands in Phase 3). (See ISSUE 1.)
- New `PublishSpec(exchange: str, route: Route|None=None, route_func: RouteFunc|None=None)`.
- `WorkerSpec`: add `consume_exchange: str`, `binding_key: str|None=None`,
  `publish: list[PublishSpec] = ()`. **Remove `terminal`** (empty `publish` = terminal).
- Phase 2 adds `accepts`/`produces`.

### G. `warren/runtime/runner.py` (`DefaultWorkerRunner`)
- ⚠️ ISSUE 4 (RESOLVED): runner doesn't currently receive the pipeline, only `worker_spec`. To resolve
  exchange names it needs the registry. → add constructor param
  `exchanges: dict[str, RMQExchangeConfig]`. Launcher passes `PIPELINE.exchanges`.
- `_create_data_publishers`: build one `RMQPublisher` per `worker_spec.publish` entry, each with
  `exchange_config=exchanges[ps.exchange]`, `route=ps.route`, `route_func=ps.route_func`. Empty → [].
- `_create_control_publisher` (**D9 — control-publisher split, folded into Phase 1**): one
  `RMQPublisher` to the **consume** exchange for lifecycle envelopes (soft/hard-failure). On fanout: no
  route (broadcast, self-select — today's behavior). On topic/direct: a route_func by `data_type` so
  envelopes carry reserved keys (`soft-failure`/`hard-failure`). Pass it to the consumer manager
  separately from the data publishers so a failure is emitted **once**, never per data publisher.
- `_create_consumer_manager`: `exchange = exchanges[worker_spec.consume_exchange]`;
  `queue = f"{exchange.name}.{worker_type}"`; `RMQQueueConfig(routing_key=worker_spec.binding_key)`.
- ⚠️ ISSUE 2 (RESOLVED): if `binding_key` is None on a non-fanout consume exchange → queue binds on ""
  → matches nothing. Add a clear validation error (a non-fanout consumer must set `binding_key`).
- NOTE: Phase 1 is **single-publish** per worker in the examples; multi-publish lands in Phase 3 with
  per-exchange observers + idempotent retry. The control-publisher split is added now so the failure
  path is correct from the start.

### G2. `warren/pubsub/.../consumer.py` (`RMQConsumerManager`) — control-publisher split (D9/D10)
- Take `data_publishers: list` + `control_publisher: single` (today's single `publishers` list is
  conflated). Success → `data_publishers`; soft/hard-failure envelopes → `control_publisher` **once**.
- D10: stamp the failure envelope with the incoming `message.routing_key` + consume-exchange name so a
  retry worker can **replay** `(exchange, routing_key)` to re-deliver to the failed worker's input.
  (Full idempotent retry replay = Phase 3; the stamp is cheap to add now.)

### H. Internal runners: `warren/jobs/publishing/...`, `warren/jobs/status/...`, `warren/retry_management/...`
- Today each rebuilds an exchange from `config.rabbitmq.exchange`. Switch to `pipeline.exchanges`.
- ⚠️ ISSUE 1 (RESOLVED — D3): these launchers have no `--pipeline-spec`. → add it. They read
  `pipeline.exchanges[pipeline.default_exchange]` for their queue + (Phase 1) bind to that one exchange.
  Per-exchange observers (D12) land in Phase 3.
- ⚠️ ISSUE 5 (RESOLVED — D10): the **retry worker needs NO route_func** — it *replays* the
  `(exchange, routing_key)` stamped on the failure envelope (D10). So only the **publication worker**
  still needs routing config for the *initial* messages (`--route-func module:obj`, like its existing
  `--publisher-factory`). The **status worker**'s `job-completed` emit needs a route only under
  topic/direct — that's a Phase 3 concern (per-exchange observers); Phase 1 examples are single-exchange.

### I. `runtime_scripts/`
- `start_worker.py`: after `load_pipeline`, run `validate_pipeline(PIPELINE)` (fail-fast). Pass
  `exchanges=PIPELINE.exchanges` into the `DefaultWorkerRunner` partial.
- `start_job_status_worker.py`, `start_retry_worker.py`, `start_job_publication_worker.py`: add
  `--pipeline-spec` (+ optional `--route-func`).
- New `runtime_scripts/validate.py` → console script `warren-validate` (standalone deploy check).
- `purge_queues.py`: already derives `{exchange}.{worker_type}`; update to read `pipeline.exchanges`.

### J. `warren/runtime/validation.py` (new) — `validate_pipeline(pipeline)`
- Reference integrity: every `consume_exchange` / `PublishSpec.exchange` / `default_exchange` ∈
  `pipeline.exchanges`. (Cheap, high value.)
- Non-fanout consumer must have a `binding_key`; fanout consumer must not.
- Static-route reachability: any `PublishSpec` with a static `Route(key=...)` on a direct/topic
  exchange must match ≥1 worker's `binding_key` on that exchange → else error (route into the void).
- Orphan-binding warning: a worker binding that no static publisher targets → warning.
- ⚠️ ISSUE 6: most routing is dynamic (`route_func`), so reachability only covers static routes in
  Phase 1. Nominal type-based reachability (produces∈accepts) arrives with capabilities in Phase 2.
  Phase 1 validation is intentionally lighter; dynamic routes are reported "not statically validated".

### K. Examples (live verification) — migrate `examples/fake` to the new spec shape
- Becomes **Example A (fanout)**: `exchanges={"jobs": fanout}`, each worker `consume_exchange="jobs"`,
  `binding_key=None`, `publish=[PublishSpec("jobs")]` (embedder publishes too, not terminal).
- **Example B (topic)**: a pipeline binding each worker to a data_type pattern, publishers using
  `MessageFieldRouter`. Proves topic end-to-end.

### L. Tests
- Update existing 9 tests for new config (no `config.rabbitmq.exchange`) + new `WorkerSpec` shape.
- New unit tests: `MessageFieldRouter`; publisher validation per exchange type; `validate_pipeline`.
- New **integration test (Docker)**: declare a topic exchange, bind two queues with different keys,
  publish, assert correct delivery. Direct proof of the topology change independent of the full
  pipeline. (Plus the examples as full e2e.)

### M. Docs touched by the breaking change (must update in Phase 1, not deferred)
- `README.md` quickstart (config.yaml no longer has `rabbitmq.exchange`; spec now defines exchanges).
- `warren/runtime/USAGE.md` (WorkerSpec shape, `terminal` → `publish`, exchanges in spec).

## Decisions — all RESOLVED (see `tasks/routing-design.md` for full rationale)
1. ISSUE 1 → exchanges in `PipelineSpec`; support workers gain `--pipeline-spec` + observe
   `PipelineSpec.default_exchange` in Phase 1. (D3)
2. ISSUE 5 → retry worker needs no router (replay, D10); only the publication worker takes a route_func.
3. ISSUE 6 → Phase 1 `validate_pipeline` is reference/static-route level only; dynamic-route & nominal
   type reachability arrive in Phase 2. Documented gap. (D13)
4. `terminal` flag REMOVED — empty `publish` = no downstream data; completion is a separate concept and
   stays on `final_data_type` in Phase 1 (terminal-set completion = Phase 3, D11).
5. NEW — control-publisher split (D9) and failure-envelope routing-key stamp (D10) folded into Phase 1
   so the failure path is correct from the start; full idempotent retry + multi-publish = Phase 3.

## Verification gate
- `python -m pytest tests -q` green (migrated).
- Docker integration: topic exchange, two queues with distinct binding keys, publish → assert delivery.
- Examples A (fanout) + B (topic) run end-to-end against Docker (rabbitmq/mongo/redis), docs match.
</content>
