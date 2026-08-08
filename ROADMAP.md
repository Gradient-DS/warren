# Roadmap

Where Warren is headed. This is a statement of direction, not a schedule —
items ship when they're ready, and feedback (issues, discussions) can and
should reorder it.

## Transport generalization

Warren's model is transport-agnostic; the implementation is not yet fully
there. The plan, in order:

1. **Neutral channel spec** — replace the RabbitMQ-flavoured exchange config
   in `PipelineSpec` with a backend-neutral `ChannelSpec`, so a pipeline spec
   doesn't name broker primitives.
2. **Route key split** — separate *selection* (which worker processes a
   message) from *affinity* (which messages must stay ordered relative to each
   other). Wiring the affinity key onto the Kafka message key unlocks per-key
   ordering.
3. **`PubSubBackend` protocol + capability matrix** — a declarative statement
   of what each backend supports, so unsupported combinations are rejected by
   data rather than hand-written guards.
4. **Full Kafka routing support** — Kafka is currently fanout-only by design
   (topic/direct selection fails fast at startup). Filtered/selective
   consumption on Kafka lands once the capability matrix exists and a use
   case demands it.
5. **Redis Streams backend** — a strong third-backend candidate: Redis is
   already in the stack, which would make the smallest possible Warren
   deployment a single Redis + MongoDB.
6. **`warren-run` launcher consolidation** — one entry point replacing the
   per-worker-kind start scripts.

## Towards processing DAGs

Fan-out works today (one message, many self-selecting consumers). The planned
next step is **fan-in**: a join worker base class that waits for a set of
upstream results per item before proceeding, plus completion tracking over a
terminal *set* of data types rather than a single final one. See
`warren/docs/routing.md` for the current thinking.

## Worker self-registration

Workers registering their capabilities (`accepts`/`produces`) in the store at
startup, so routing plans can be validated against what is actually deployed —
and so pipelines can be composed dynamically against a live worker fleet.

## Resilience

- **Startup connection retry** — workers currently fail fast if the broker or
  stores aren't reachable at startup; add retry with backoff so orchestrators
  don't have to crash-loop them.
- **Transient store-error mapping** — map transient storage errors to soft
  failures (bus-level retry) for primary consumers, extending the
  local-retry policy that observers already use.
- **Retry-path test coverage** — end-to-end scenarios for retry-worker restart
  recovery, max-retry exhaustion, and mixed outcomes.

## Operations

- **First-class autoscaling story** — scaling on queue depth already works
  today with no code changes (workers are competing consumers per queue; see
  `warren/docs/rabbitmq.md` for the KEDA pointer). Planned: shipped example
  scaler manifests per backend (RabbitMQ queue-depth, Kafka consumer-lag) and
  documentation of the scale-to-zero implications for job completion tracking.

## Developer experience and API polish

- In-memory broker backend for tests — run a pipeline in a single process with
  no infrastructure.
- Connection-string configuration (`mongodb://`, `redis://`) alongside
  host/port fields.
- Batch storage on the results-store interface (`batch_store`).
- Job lookup by metadata as a first-class `JobStore` query instead of direct
  collection access.
- Slim down `PipelineSpec`: move fields only used by test harnesses out of the
  core spec.
- Rework the worker-factory context so factories own their dependencies
  (fewer `needs_*` flags, better typing).
- Couple consumer prefetch to worker concurrency instead of configuring them
  independently.
