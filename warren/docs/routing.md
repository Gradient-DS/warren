# Routing & Exchanges

Warren routes messages through RabbitMQ exchanges. By default a pipeline uses a
single **fanout** exchange where every worker receives every message and
self-selects via `should_process()`. Warren also supports **topic** and
**direct** exchanges, so the broker can route messages by routing key, and a
single job can define its own path through the deployed workers.

This document describes the routing model and the design decisions behind it.

## Exchange types

| Type | Routing-key behaviour | Use case |
|------|-----------------------|----------|
| `fanout` | ignored — broadcast to all bound queues | workers self-select via `should_process` |
| `topic` | pattern match (`*`, `#`) | route by *kind of payload* (e.g. `data_type`) |
| `direct` | exact match | **addressed** routing (route to a specific worker) |
| `headers` | not supported | — |

`headers` is intentionally unsupported: it routes on binding *arguments* rather
than routing keys, a different code path from everything else here.

All three supported types share one mechanism: a per-worker **binding key**
(what the worker's queue subscribes to) and an injected **route function** on
each publisher (how the routing key is computed). The exchange type is only
configuration; there is no per-type branching in the core and no hidden
routing policy — routing is always supplied explicitly per worker.

## Where topology lives: `PipelineSpec` vs `config.yaml`

Exchanges, bindings, and routing are **pipeline topology** — they are the same
across every deployment of a pipeline — so they live in the `PipelineSpec`
(Python). `config.yaml` holds only per-environment infrastructure: broker /
MongoDB / Redis hosts, credentials, prefetch.

The test: *"If I deploy the same pipeline to staging and prod, what changes?"*
Only hosts/credentials/scaling (config). The set of exchanges, their types, and
the routing never change between environments — so they are pipeline identity.

```python
PipelineSpec(
    exchanges={"jobs": RMQExchangeConfig(name="jobs", type="fanout")},
    default_exchange="jobs",
    workers={...},
    ...
)
```

- `exchanges` — named exchange definitions (name, type, durable).
- `default_exchange` — the exchange the support workers (job-status, retry,
  publication) observe.

## Wiring a worker

```python
WorkerSpec(
    collections={"read": "chunks", "write": "embeddings"},
    factory=create_embedder,
    consume_exchange="jobs",
    binding_key=None,                        # required for topic/direct, None for fanout
    publish=[PublishSpec(exchange="jobs")],  # empty list = no downstream data
)
```

- `consume_exchange` — which exchange this worker's queue binds to.
- `binding_key` — the queue's binding pattern. Must be `None` on a fanout
  exchange (which ignores keys) and is required on topic/direct exchanges.
- `publish` — a list of `PublishSpec(exchange, route=None, route_func=None)`
  targets. On fanout leave `route`/`route_func` unset; on topic/direct set one.
  An **empty `publish` list** means the worker publishes no data downstream —
  there is no separate `terminal` flag.

Because `publish` is a list, a worker can publish to a fanout **and** a
topic/direct exchange at the same time (see the multi-exchange example).

### Competing consumers / autoscaling

Binding and consuming are independent. One queue (`{exchange}.{worker_type}`,
one binding) can be consumed by N worker instances — the broker round-robins
among them by prefetch. This is identical on fanout, direct, and topic, so
horizontal scaling (e.g. KEDA on queue depth) needs no code changes.

## Route functions

A route function (`RouteFunc`) computes the routing key(s) for an outgoing
message. Warren ships a few; a pipeline can also supply its own.

- **`MessageFieldRouter(field="data_type")`** — routes by a message field
  (`data_type` by convention). The common case for topic routing: a worker
  binds the `data_type`(s) it consumes, and publishes keyed by the `data_type`
  it produces.
- **`RoutingPlanRouter`** — job-defined addressed routing (see below).
- **`ReplayRouter`** — replays the routing key a message originally arrived
  with; used by the retry worker (see *Failure lifecycle*).

## Job-defined routing

Routing *strategy* (what computes the key) and exchange *type* (how the broker
matches it) are separate choices, but they pair naturally:

| Strategy | Routes by | Natural exchange | Why |
|----------|-----------|------------------|-----|
| `MessageFieldRouter` | `data_type` (content) | `topic` | value is wildcard matching over a type taxonomy |
| `RoutingPlanRouter` | worker-type id (address) | `direct` | worker ids are exact strings — no wildcards needed |

The job-defined pattern below is the **addressed** strategy, so its natural home
is a **`direct`** exchange. (It *can* run on a topic exchange with literal
binding keys, since topic exact-matches keys without wildcards, but there's no
reason to — `direct` is the simpler, correct fit.)

Routing can be decided **per job** rather than fixed by the topology. A job
attaches a `RoutingPlan` to its `job_parameters`; because `job_parameters`
propagates through the message chain, every downstream hop can resolve the next
step from it.

```python
class RoutingPlan(BaseModel):
    entry: list[str]              # worker-type id(s) the document enters at
    edges: dict[str, list[str]]   # producer-id -> successor-id(s); [] = terminal
```

Each worker binds its queue to its own worker-type id (exact match → direct
exchange), and publishes via `RoutingPlanRouter`, which reads the plan, looks up
the producing node via the message's `origin.type`, and emits one route per
successor. The submitter publishes the initial message to the plan's `entry`
node(s).

**The plan is inert without the router.** A `RoutingPlan` is just data in
`job_parameters` — it has no effect unless the pipeline's publishers use
`RoutingPlanRouter` as their `route_func`. The two are coupled by design (the
router is the injected routing policy). Mind the failure modes: a publisher with
`RoutingPlanRouter` but a message with no plan **raises at publish time**; a plan
with no `RoutingPlanRouter` anywhere is **silently ignored**. This coupling
cannot be checked at deploy time — a `route_func` is opaque to the framework.

This is a routing **tree**: fan-out (a node with multiple successors) is
supported; fan-in / join (a worker waiting on multiple upstream branches) is
not — that requires a stateful aggregator worker and is out of scope.

## Worker base classes

- **`FilteringWorkerBase`** — imperative self-selection: you implement
  `should_process(message)`. Suited to a fanout exchange.
- **`CapabilityWorkerBase`** — declarative: the worker declares the `data_type`s
  it `accepts` and the one it `produces`; `should_process` defaults to
  "is the message's `data_type` one I accept?". This doubles as a runtime type
  guard even when the broker has already routed only matching messages, and the
  declared capabilities feed routing-plan validation.

`accepts` / `produces` are declared once on the `WorkerSpec` (the single source
of truth for validation) and passed to the worker via `WorkerFactoryContext`.

> **Open question (to discuss before 1.0): merge these two bases?** They share
> one shape — "filter, then process" — and differ only in how `should_process`
> is decided (`FilteringWorkerBase` makes it abstract; `CapabilityWorkerBase`
> derives a default from `accepts`). `CapabilityWorkerBase` is nearly a superset.
> A single base with optional `accepts`/`produces` and an overridable
> `should_process` could cover both modes, at the cost of a public-API change
> (which is cheap pre-1.0). The counter-argument is that two named classes signal
> intent. Not yet decided — see PR discussion.

## Type checking

Type compatibility is **nominal** (by name): `data_type` is a string, the user
defines the taxonomy, and compatibility is string-set membership
(`produces ∈ accepts`). There is no structural/payload validation — "same name
means same shape" is the pipeline author's responsibility. The boundary is
designed so a `data_type → schema` registry could be added later without a
breaking change.

## Failure lifecycle

A worker has two publishing paths, kept separate so a failure is never amplified
by fan-out:

- **Data publishers** (`publish`, 0..N) — successful results fan out downstream.
- **Control publisher** (one) — lifecycle envelopes (soft/hard-failure) go to
  the worker's *consume* exchange exactly once, so a worker with several data
  publishers still emits one failure envelope and triggers one retry.

On a soft failure the consumer manager stamps the routing key the message
arrived with onto the envelope. The retry worker persists it, waits the backoff
delay, and republishes via `ReplayRouter` — replaying that key so the message
lands back in the queue of the worker that failed, under any routing scheme. The
retry worker deduplicates on the message's retry key, so a duplicated envelope
cannot schedule two retries.

## Validation

- **Deploy-time** (`validate_pipeline`, run at launcher startup and as a
  standalone check): exchange references resolve; a non-fanout consumer has a
  `binding_key` and a fanout consumer does not; every non-fanout publish target
  has a route. Reachability of **dynamic** route functions cannot be enumerated
  statically and is not yet checked.
- **Submission-time** (`validate_routing_plan`): every node in a job's
  `RoutingPlan` maps to a deployed worker, every edge is nominally
  type-compatible (`produces ∈ accepts`), and entry nodes accept the submitted
  document's type. Run before any message is published.

## Examples

| Example | Exchange | Demonstrates |
|---------|----------|--------------|
| `examples/fake` | fanout | broadcast + `should_process` self-selection |
| `examples/topic` | topic | broker routing by `data_type` |
| `examples/routed` | direct | capability workers + job-defined `RoutingPlan` |
| `examples/multi_exchange` | fanout + topic | one worker publishing to two exchanges; an audit side-channel |

## Not supported (yet)

These are deliberate boundaries, designed to be added later without reworking
the model:

- **Fan-in / join** — a worker waiting on multiple upstream branches.
- **`headers` exchanges.**
- **Structural type checking** — a `data_type → schema` registry over the
  current nominal (by-name) checks.
- **Static reachability of dynamic routes** — keys produced by a `route_func`
  cannot be enumerated at deploy time.
- **Framework completion signals for terminal workers** — completion is
  detected from `final_data_type` today, which assumes the final worker emits a
  message. A genuinely terminal (`publish=[]`) worker is not yet counted toward
  completion on its own.
- **Multi-exchange lifecycle observation** — the job-status / retry workers
  observe a single (`default_exchange`) exchange. Running observers per exchange
  is possible but not yet wired.
</content>
