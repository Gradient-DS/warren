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

## One exchange per pipeline

A pipeline uses a **single exchange**. All workers consume from it and publish
to it; the exchange type decides how routing works. Deployments with multiple
exchanges (or a worker publishing to several at once) are **deferred** — see
*Not supported (yet)*.

## Where topology lives: `PipelineSpec` vs `config.yaml`

The exchange, bindings, and routing are **pipeline topology** — the same across
every deployment of a pipeline — so they live in the `PipelineSpec` (Python).
`config.yaml` holds only per-environment infrastructure: broker / MongoDB /
Redis hosts, credentials, prefetch.

The test: *"If I deploy the same pipeline to staging and prod, what changes?"*
Only hosts/credentials/scaling (config). The exchange, its type, and the routing
never change between environments — so they are pipeline identity.

```python
PipelineSpec(
    exchange=RMQExchangeConfig(name="jobs", type="fanout"),
    workers={...},
    ...
)
```

## Wiring a worker

```python
WorkerSpec(
    collections={"read": "chunks", "write": "embeddings"},
    factory=create_embedder,
    binding_key=None,         # required for topic/direct, None for fanout
    publish=PublishSpec(),    # None = no downstream data (terminal)
)
```

- `binding_key` — the queue's binding pattern. Must be `None` on a fanout
  exchange (which ignores keys) and is required on topic/direct exchanges.
- `publish` — a `PublishSpec(route=None, route_func=None)` describing how the
  worker publishes its result to the pipeline exchange, or `None` if the worker
  publishes nothing downstream (terminal — there is no separate `terminal`
  flag). On fanout leave `route`/`route_func` unset; on topic/direct set one.

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

## Observation (support workers)

The support workers — job-status (completion tracking) and retry — **observe**
the pipeline by consuming everything, on an **observer exchange** that the
framework derives from the pipeline exchange (`observer_exchange`):

- **fanout / topic** → observed *in place*: fanout broadcasts to every queue,
  and topic supports a `#` catch-all binding, so the data exchange is its own
  observer exchange. No second exchange, no extra messages.
- **direct** → can't be observed wholesale (it routes by exact key), so the
  framework creates a dedicated **fanout** observer exchange (`<name>.observer`),
  *invisible to the user*. On a direct pipeline, each worker also publishes its
  result to the observer exchange (an extra publish per message, direct only),
  and lifecycle envelopes go there too — so the support workers see the whole
  pipeline. The retry worker observes the observer exchange and republishes
  retried messages back to the direct data exchange.

The observer workers themselves are identical across all three cases — they bind
a plain fanout/topic exchange and record/retry. Only the framework's wiring
differs by exchange type. (Defining *additional* user-facing exchanges, or a
worker publishing to several at once, remains deferred — see below.)

## Failure lifecycle

A worker publishes through separate paths so the failure path is independent of
the success path, and so observation works regardless of the data exchange type:

- **Data publisher** (`publish`) — the successful result routes downstream on
  the data exchange (the worker's route).
- **Control publisher** — lifecycle envelopes (soft/hard-failure) go to the
  *observer* exchange so the retry/status workers see them.
- **Observer publisher** (direct only) — echoes the successful result to the
  observer exchange so the status worker can track stages on a direct pipeline.
  Unset for fanout/topic (the data publish is already observable).

On a soft failure the consumer manager stamps the routing key the message
arrived with onto the envelope. The retry worker persists it, waits the backoff
delay, and republishes via `ReplayRouter` — replaying that key so the message
lands back in the queue of the worker that failed, under any routing scheme. The
retry worker deduplicates on the message's retry key, so a duplicated envelope
cannot schedule two retries.

## Validation

- **Deploy-time** (`validate_pipeline`, run at launcher startup and as a
  standalone check): a non-fanout consumer has a `binding_key` and a fanout
  consumer does not; a non-fanout publish has a route. Reachability of **dynamic**
  route functions cannot be enumerated statically and is not checked.
- **Submission-time** (`validate_routing_plan`): every node in a job's
  `RoutingPlan` maps to a deployed worker, every edge is nominally
  type-compatible (`produces ∈ accepts`), and entry nodes accept the submitted
  document's type. Run before any message is published.

## Examples

| Example | Exchange | Demonstrates |
|---------|----------|--------------|
| `examples/rag` | fanout | **real** PDF → chunk → embed (pypdf + OpenAI); document fetcher; soft/hard failure on a real API |
| `examples/exchanges/fanout` | fanout | broadcast + `should_process` self-selection (synthetic, zero-dependency) |
| `examples/exchanges/topic` | topic | broker routing by `data_type` |
| `examples/exchanges/direct` | direct | capability workers + job-defined `RoutingPlan` |

## Not supported (yet)

These are deliberate boundaries, designed to be added later without reworking
the model:

- **Multiple user-facing exchanges** — a pipeline exposing more than one
  exchange, or a worker publishing to several at once. A pipeline has exactly one
  user-defined exchange today. (The framework's derived observer exchange for
  direct pipelines is internal and doesn't count — the user never defines it.)
- **Fan-in / join** — a worker waiting on multiple upstream branches.
- **`headers` exchanges.**
- **Structural type checking** — a `data_type → schema` registry over the
  current nominal (by-name) checks.
- **Static reachability of dynamic routes** — keys produced by a `route_func`
  cannot be enumerated at deploy time.
- **Framework completion signals for terminal workers** — completion is
  detected from `final_data_type` today, which assumes the final worker emits a
  message. A genuinely terminal (`publish=None`) worker is not yet counted toward
  completion on its own.
</content>
