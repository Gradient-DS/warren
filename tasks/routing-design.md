# Flexible routing — design & decisions

**Status:** design agreed (Ben + Claude), pending colleague review before PR.
**Branch lineage:** `feat/topic-exchange-support` → phased PRs → `feat/examples`.
**Scope philosophy:** "simple in a *good* way" — full capability, clean implementation, no
overengineering and no half-measures. We are **not** changing the core execution model; everything
here is opt-in and the existing `fanout` + `should_process` pipeline keeps working unchanged.

---

## Problem

Warren has only ever run on a **fanout** exchange: every worker receives every message and
self-selects via `should_process()`. We want to (a) support **topic** and **direct** exchanges so the
*broker* can route, (b) let a **job submitter define the routing path per job** (not just deploy-time
topology), and (c) allow a deployment to run **multiple exchanges at once** (e.g. a fanout and a topic
exchange), including a worker that publishes to more than one.

Today this does **not** work: the runtime never supplies a queue `binding_key` or a publisher
`route`/`route_func`, so a topic exchange crashes at worker setup and binds to nothing. See the
investigation in `tasks/examples-findings.md`.

---

## Decisions (with rationale & alternatives)

### D1 — Supported exchange types: `fanout`, `topic`, `direct`. Defer `headers`.
- `fanout` (existing): broadcast, workers self-select.
- `topic`: pattern match — route by *kind of payload* (`data_type`), supports wildcards.
- `direct`: exact match — **addressed** routing (route to a specific worker/capability id). This is the
  natural fit for job-defined routing.
- `headers`: **deferred** — routes on binding *arguments* (`x-match`), a different code path from
  routing keys; rarely used.
- **Rationale:** `direct` is nearly free given the injected-route model (same machinery as topic,
  exact match), and removing the arbitrary restriction is cheap. `headers` is real, low-value work.

### D2 — One mechanism for all types: per-worker `binding_key` + injected `route`/`route_func`.
The exchange type is just config; there is **no per-type special-casing** in the core and **no hidden
auto-policy**. A worker declares what key its queue binds to; a publisher is given how to compute the
key. (My earlier `topic_route_func` auto-attach idea is dropped — routing is always explicitly
injected.)

### D3 — Exchanges live in `PipelineSpec`, not `config.yaml`. **(breaking, accepted)**
- **`PipelineSpec`** = *what the pipeline IS* — worker types, factories, collections, **exchanges,
  bindings, routing**. Pipeline identity/topology. **Same across every deployment.**
- **`config.yaml` (`RuntimeConfig`)** = *where/how this instance is wired to infra* — broker/Mongo/Redis
  hosts, ports, credentials, prefetch, retry toggle. **Varies per environment (dev/staging/prod).**
- **The deciding test:** "If I deploy the same pipeline to staging and prod, what changes?" → only
  hosts/credentials/scaling (config). The set of exchanges, their *types*, bindings, and routing never
  change between environments → they are pipeline identity → **spec**.
- An exchange splits cleanly: **definition** (name, type, durable) = topology → spec; **which broker it
  lives on** = environment → config (the `connection` block).
- **Cost / consequence:** support workers (status/retry/publication) must load the `PipelineSpec` to
  learn the exchanges. This is *correct coupling* — a support worker serves one specific pipeline's
  topology, so it should know that pipeline. Their launchers gain `--pipeline-spec`.
- **Alternative rejected:** keep exchange *definitions* in config (`config.rabbitmq.exchanges`) so
  support workers need no spec. Less code churn, but it splits one topology graph across two files and
  is conceptually wrong (exchanges aren't environment config).

### D4 — `WorkerSpec` shape; remove the `terminal` flag.
```python
@dataclass(frozen=True)
class PublishSpec:
    exchange: str                        # name from pipeline.exchanges
    route: Route | None = None
    route_func: RouteFunc | None = None  # fanout: both None; direct/topic: exactly one set

@dataclass(frozen=True)
class WorkerSpec:
    collections: dict[str, str]
    factory: WorkerFactory
    consume_exchange: str                # which exchange this worker binds to
    binding_key: str | None = None       # topic/direct pattern; None for fanout
    publish: list[PublishSpec] = ()      # empty = no downstream data (see D11 for completion)
    accepts: frozenset[str] = frozenset()# capability (Phase 2)
    produces: str | None = None          # capability (Phase 2)
    needs_document_fetcher: bool = False
    needs_document_store: bool = False
```
- **Remove `terminal`:** single source of truth. "Does this worker publish data downstream?" is
  answered by `publish` being empty. Keeping a separate `terminal: bool` lets the two contradict
  (`terminal=True` + non-empty `publish`). **Important:** "empty publish" ≠ "job done" — completion is a
  separate concept (see **D11**).

### D5 — Two routing schemes, both just `route_func` + `binding_key`.
- **Scheme 1 — route by `data_type` (topic).** Key = kind of payload (e.g. `markdown_document`).
  Workers bind to the data_types they accept. The *path is fixed* by what workers produce/consume.
  Helper: `MessageFieldRouter(field="data_type")`.
- **Scheme 2 — addressed routing (direct).** Key = *who handles this next* (a worker/capability id).
  Each worker binds to its **own id**. The *path is chosen* by whoever sets the keys → this is what
  makes routing job-definable. Helper: `RoutingPlanRouter` (Phase 2).
- **RabbitMQ reality:** you never route "to a worker" directly — you publish with a key, and the broker
  delivers to every queue whose binding matches. "Route to worker X" = publish key `X`, X's queue bound
  to `X`. Exact match → `direct`.

### D6 — Job-defined routing via a `RoutingPlan` in `job_parameters`.
- `job_parameters` already propagates untouched through the whole chain (`ProcessingMessage.derive()`
  copies it; `to_dict()` serializes it). So job-time routing needs **no envelope changes**.
```python
class RoutingPlan(BaseModel):
    entry: list[str]              # capability/worker-type(s) the document enters at
    edges: dict[str, list[str]]  # producer-id -> successor-id(s); [] = terminal
```
- `RoutingPlanRouter` (a `route_func`): `current = message["origin"]["type"]`; emit one `Route` per
  successor in `edges[current]`. The submitter publishes the initial message keyed to `entry`.
- **Split:** *deploy time* defines the topology = the paths that *can* exist; *submit time* picks the
  path via the plan. This is `should_process` moved to job-kickoff.
- **Tree only** — each node has one parent. **Fan-out** (multiple successors) is supported; **fan-in /
  join is NOT** (see D13). `capability id == worker-type` for v1.

### D7 — Two worker base classes.
- `FilteringWorkerBase` (existing): *imperative* self-selection — author hand-writes `should_process`.
- `CapabilityWorkerBase` (new, Phase 2): *declarative* — declares `accepts`/`produces`; `should_process`
  **defaults to `data_type ∈ accepts`** (defense-in-depth runtime type guard). Same `process()`
  contract. `accepts`/`produces` are declared on `WorkerSpec` (single source for validators) and passed
  into the worker via the factory.

### D8 — Type checking is nominal (string `data_type`). Registry deferred.
- `data_type` stays a **string** (it doubles as a routing key and a plan dict key, and must serialize).
  The user defines the taxonomy. Compatibility = string-set membership (`produces ∈ accepts`).
- **LIMITATION (must document in code + USAGE):** by-name only; "same name = same shape" is the author's
  responsibility; no structural/payload validation. The boundary is designed so a `data_type → Pydantic
  schema` registry can be layered in later **without breaking changes**.

### D9 — Control-publisher split (the fix for "retry N times").
A failure is **one event**, but a worker may have **N data publishers**. Publishing the failure envelope
through all N → N retries. Root cause: data publishing and lifecycle publishing share a path.
- **Data publishers** (`list`, from `publish`): emit successful results downstream. Plural.
- **Control publisher** (single): emits lifecycle envelopes (soft-failure, hard-failure, and — Phase 3 —
  completion signals) **exactly once**, to the worker's **consume exchange**.
- The consumer manager takes `data_publishers: list` + `control_publisher: single`. Failures go through
  the control publisher once → one envelope → one retry.
- This is **not** a separate control *exchange* (the thing we rejected): it's one publisher to the
  *same* (consume) exchange, using reserved keys (`soft-failure`/`hard-failure`) under topic/direct, and
  on **fanout** it is identical to today (broadcast + self-select) — fully backwards compatible.

### D10 — Retry by replay (no router on the retry worker).
- When the consumer manager builds a failure envelope, it stamps the **exchange + routing key the
  message originally arrived with** (`aio_pika` exposes `message.routing_key`).
- The retry worker, after its delay, **replays** that `(exchange, routing_key)` → the original lands
  back in the failed worker's input queue. The retry worker needs **no `route_func`** — it replays.
- Idempotent: dedupe on message identity + `retry.count` so a duplicated failure envelope can't
  schedule two retries.
- Everything stays on the worker's own exchange → **per-exchange, single-exchange**, no cross-exchange
  routing.

### D11 — Completion = terminal-node set + framework completion signals (not "empty publish").
- A truly terminal worker (empty `publish`) emits no message → it is **invisible** to the status worker.
  Today's code works around this: the "final" worker isn't actually terminal — it publishes a final
  message *so that* status can observe completion.
- Clean model: the framework **emits a completion signal** via the control publisher when a terminal
  worker succeeds (so terminal successes are observable without a downstream data message), and the
  **completion criteria = the set of terminal nodes** — derived statically (empty-`publish` workers) for
  a fixed pipeline, or from the routing plan (`edges[x] == []`) per job. A document is done when all its
  expected terminal nodes have signalled; the job is done when all documents are done.
- This **replaces** the "publish a fake final message" workaround and makes terminal workers
  first-class. Removing `terminal` (D4) is still right — completion does not key off it.

### D12 — Per-exchange support workers (not multi-bind).
- Run one status + one retry worker **per exchange**, each single-exchange. Avoids (a) binding one queue
  to many exchanges, and (b) cross-exchange retry routing.
- **Completion still works:** all status workers write to the *same* Mongo `job_results` (already
  idempotent: upsert on `(job_id, data_type, doc_id)`), and completion is computed from Mongo counts,
  not in-memory state. Overlap is harmless.
- Cost: more processes (2+2 for a 2-exchange deployment) and a minor "both detect completion → two
  `job-completed` emits" (idempotent; easily handled).

### D13 — Validation, two layers.
- **Deploy-time `validate_pipeline(pipeline)`** — runs at launcher startup (fail-fast) + standalone CLI
  (`warren-validate`):
  - reference integrity: every `consume_exchange` / `PublishSpec.exchange` / `default_exchange` ∈
    `pipeline.exchanges`;
  - non-fanout consumer must have a `binding_key`; fanout consumer must not;
  - every `PublishSpec` on a non-fanout exchange must have a `route`/`route_func`;
  - static-route reachability: a static `Route(key=…)` on direct/topic must match ≥1 worker's
    `binding_key`; orphan bindings → warning.
  - **NOT YET (documented gap):** reachability through **dynamic `route_func`s** cannot be statically
    enumerated → reported as "not statically validated", not a false pass. Nominal-type reachability
    (`produces ∈ accepts`) arrives with capabilities in Phase 2.
- **Submission-time `validate_routing_plan(plan, registry)`** (Phase 2) — runs in the publish/submit
  path before any message is sent: every node maps to a deployed worker; every edge `u→v` satisfies
  `produces[u] ∈ accepts[v]`; entry nodes accept the submitted document's type.

### D14 — Competing consumers / KEDA are unaffected.
Binding ≠ consuming. One queue `{exchange}.{worker_type}`, one binding; N instances consume it →
competing consumers, broker round-robins by prefetch. Identical on fanout/direct/topic. KEDA scales on
queue depth. No per-instance bindings.

---

## Deferred (future versions — call out explicitly so we don't lose them)
- **Fan-in / join** (a worker waiting on ≥2 upstream branches) — needs a stateful aggregator worker
  (collect partials by `(job_id, doc_id)` in storage, emit when complete). Application-level today.
- **`headers` exchange.**
- **Structural/schema-based type checking** (the `data_type → schema` registry).
- **Capability abstraction** distinct from worker-type (several worker-types serving one capability).
- **Dynamic-route reachability** validation.

---

## Phased build (each phase = its own branch/PR + tests; examples are live verification)

### Phase 1 — exchange model (single-exchange, single-publish) — ✅ DONE
- D1 (add `direct`/`topic` to the `Literal`), D2, D3 (exchanges → `PipelineSpec`, breaking),
  D4 (new `WorkerSpec`/`PublishSpec`, remove `terminal`), D5/D6 groundwork (`MessageFieldRouter`),
  D13 deploy-time `validate_pipeline` (lighter), D14.
- **Single-publish** per worker; completion stays on the existing `final_data_type` (Phase 1 has no
  empty-`publish` workers — the final worker still emits its final message).
- Migrated `examples/fake` → **Example A (fanout)**; added **examples/topic** (**Example B**). Tests
  green (routing + validation unit tests + a broker-backed topic integration test); README/USAGE
  updated for the new config/spec shape.

> **Deviation from the original plan (agreed: defer, don't redesign):** D9 (control-publisher split)
> and D10 (retry-by-replay) were **moved out of Phase 1 into Phase 3**. Rationale: Phase 1 is
> single-publish, so the "retry N times" bug cannot occur; folding the split in now would be
> speculative restructuring against the "keep it simple" directive. Phase 1 keeps the existing single
> `publishers`-list behavior (lifecycle envelopes ride the worker's one publisher, as today). The
> internal support-worker runners route lifecycle/observer publishing via `observer_route_func`
> (data_type) under topic; full replay-based retry lands with multi-publish in Phase 3.

### Phase 2 — capabilities & job-defined routing — ✅ DONE
- D7 (`CapabilityWorkerBase` — declares `accepts`/`produces`, `should_process` derived),
  D8 (`accepts`/`produces` on `WorkerSpec`; `worker_type`/`accepts`/`produces` added to
  `WorkerFactoryContext` so `origin.type` matches the spec key — the node id used for addressed
  routing), D6 (`RoutingPlan` + `RoutingPlanRouter`, reading `job_parameters["routing"]`),
  D13 submission-time `validate_routing_plan` + `build_capability_registry`.
- **examples/routed** (direct exchange, addressed by worker-id) verifies it e2e: a job's `RoutingPlan`
  drives the path (publisher → parser → chunker → embedder), validated before publish. Unit tests for
  the router (entry/successor/terminal/fan-out), the validator, and `CapabilityWorkerBase`.
- Note: completion tracking under addressed routing (terminal-set completion, D11) is Phase 3; the
  routed example does not run job-status.

### Phase 3 — multi-exchange & lifecycle
- Multi-publish + multiple exchanges, **D9 (control-publisher split, moved from Phase 1)**,
  D12 (per-exchange observers), D10 (idempotent retry by replay),
  D11 (terminal-set completion + completion signals), **Example C (fanout + topic/direct at once)**.

---

## Cross-references
- Investigation & current-state evidence: `tasks/examples-findings.md`
- Phase 1 file-by-file plan: `tasks/phase1-implementation-plan.md`
</content>
