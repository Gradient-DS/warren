# RabbitMQ: Exchange, Binding, Queue & Workers

## Core Components

| Component | Role |
|-----------|------|
| **Exchange** | Receives messages from producers, routes them based on rules |
| **Binding** | Connects exchange → queue with a routing key pattern |
| **Queue** | Stores messages until consumed (lives on the broker, not the client) |
| **Connection** | TCP link between a client (pod) and the RabbitMQ broker |
| **Channel** | Lightweight virtual connection multiplexed over a single connection |

## Message Flow

```
Producer → Exchange → Binding (routing key match?) → Queue → Consumer
```

## Exchange Types

| Type | Routing Key Behavior |
|------|---------------------|
| `direct` | Exact match only |
| `topic` | Wildcards: `*` (one word), `#` (zero or more) |
| `fanout` | Ignored — all bound queues receive all messages |

## Worker Patterns

**Competing consumers (shared queue):** Multiple instances of the same worker type share one queue. Each message goes to ONE worker. Use for load balancing / task distribution.

```
Exchange ──► [shared-queue] ──┬──► worker #1
                              ├──► worker #2
                              └──► worker #3
```

**Pub/sub (separate queues):** Different worker types have their own queues with their own bindings. Each type receives messages matching their routing key.

```
Exchange ──┬── binding: doc.*   ──► [doc-queue]    ──► doc-processor
           └── binding: notify.* ──► [notify-queue] ──► notifier
```

## Rule of Thumb

- Same worker type → same queue (instances compete)
- Different worker type → different queue (own binding/routing key)

## Connections, Channels & Prefetch

### Where things live

| Concept | Scope | Lives on |
|---------|-------|----------|
| Connection | Per pod | Client ↔ Broker |
| Channel | Per connection | Client ↔ Broker |
| `prefetch_count` | Per channel/consumer | Enforced by broker |
| Queue | Global (by name) | Broker |

### How competing consumers work across pods (K8s)

Each pod creates its own independent connection — there is no shared connection between pods.

```
Pod A  ──conn A──┐
Pod B  ──conn B──┼──▶  RabbitMQ broker  ──▶  "my-task-queue"
Pod C  ──conn C──┘
```

The queue is a **server-side object** on the broker. When each worker calls `declare_queue("my-task-queue", durable=True)`, they tell the broker "ensure this queue exists" and then subscribe as a consumer. The broker maintains one queue with a list of registered consumers and dispatches messages based on:

1. Which consumers have capacity (fewer unacked messages than their `prefetch_count`)
2. Round-robin among those with available capacity

### Prefetch count

`prefetch_count` controls how many unacked messages RabbitMQ pushes to a consumer at once. It is a **per-channel** setting, configured independently per pod.

| Scenario | Recommended `prefetch_count` |
|----------|------------------------------|
| Sequential processing (iterator pattern) | `1` |
| CPU-bound multiprocessing (N subprocesses) | `N` to `N+2` |
| I/O-bound async tasks | Higher (tasks spend time waiting) |

**Too high** → one consumer hoards messages from the shared broker-side queue, starving other pods.
**Too low** → workers sit idle waiting for the next message.

### Concurrent consumption patterns (aio_pika)

- **`queue.iterator()`** — processes messages **sequentially** (awaits one at a time). Higher prefetch only pre-buffers in the client.
- **`queue.consume(callback)`** — invokes callback per delivered message **concurrently**, up to `prefetch_count`. Use this for actual parallel processing.
- **Semaphore + tasks** — for fine-grained concurrency control over async processing.
- **`ProcessPoolExecutor`** — for offloading CPU-heavy work to subprocesses, with manual ack/nack.

### K8s specifics

- All pods point to the same RabbitMQ service (e.g. `amqp://rabbitmq-service.default.svc.cluster.local:5672/`)
- Use `connect_robust` — auto-reconnects on network blips during pod rescheduling
- Scale workers by increasing Deployment `replicas` — no code changes needed
- Use [KEDA RabbitMQ scaler](https://keda.sh/docs/scalers/rabbitmq-queue/) for autoscaling based on queue depth

## Open Questions
- Do workers declare exchanges / queues / bindings themselves, or are these managed separately, and worker connects with just url, queue_name, prefetch?
- Who handles publishing? Worker needs to know where to publish next, or is everything contained in the publish function? (worker gets next step from the metadata?)

## Health, blocked connections and redelivery

**What warren observes.** `RMQConsumerManager.health()` reports `connected`
(transport present, `connected` event set), `blocked` (the broker sent
`Connection.Blocked`; detected because `transport.ready()` does not complete
within the probe timeout), `channel_open` and `consumer_registered` (our
consumer tag on the underlying aiormq channel). The runner polls it every
`health.check_interval_s` and serves the last sample on `/ready`.

**Blocked.** A blocked connection backs up aiormq's 10-frame write queue;
every publish and every ack then waits, and the heartbeat sender tears the
connection down after `(heartbeat + 1) × 3` seconds. aiormq logs
`Connection … was blocked by: …` at WARNING through `aiormq.connection`;
the runtime scripts' logging passes it. Warren logs the transition too.
Nothing restarts: a restart cannot help a blocked broker.

**Consumer lost with a live connection.** aio-pika restores channels and
consumers after a *connection* drop and after a broker-initiated
`Channel.Close`; it does not when the restore itself fails, and a
`Basic.Cancel` from the broker (queue deleted) leaves the channel open with
no consumer. Warren does not try to repair these in place: after
`health.consumer_lost_grace_s` the runner ends with `WarrenError`, the
process exits non-zero and the orchestrator restarts it.

**In-flight deliveries after a channel loss.** Every delivery pins the
channel it arrived on; once that channel is closed, ack/nack/reject raise.
Warren treats such a delivery as no longer its own — nothing is published
for it, nothing is settled, one line is logged — because the broker requeues
every unacked delivery of a closed channel and will redeliver it.

**Redelivery counting.** With `rabbitmq.consumer.max_deliveries` set, a
redelivered message (`redelivered=true`) is not processed but replayed
through the retry worker with `delivery_count` in its body, and dead-lettered
(hard-failure envelope, reject) once the count exceeds the limit. Every
requeue — a channel loss mid-flight, a shutdown nack — costs one delivery.
Quorum queues offer the same broker-side (`x-delivery-limit`, default 20 in
RabbitMQ 4.x, with a DLX); pass their arguments via
`rabbitmq.consumer.queue_arguments`.
