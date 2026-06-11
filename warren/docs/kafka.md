# Kafka: Topic, Consumer Groups & Workers

Warren's Kafka backend is a drop-in alternative to RabbitMQ. Select it with
`backend: kafka` in your `RuntimeConfig` YAML — nothing else in your pipeline
spec, workers, or launcher scripts changes. The runtime builds the connection
manager, publishers, and consumer managers through
`warren.runtime.backends`, which switches on `config.backend`.

For the RabbitMQ topology this mirrors, see [`rabbitmq.md`](rabbitmq.md).

## Semantic mapping (RabbitMQ → Kafka)

The Kafka backend reproduces the RabbitMQ fanout-and-self-select model. The
table below maps every RabbitMQ behavior to its Kafka implementation:

| RMQ behavior | Kafka implementation |
|---|---|
| Fanout exchange `jobs` | One topic; every consumer group sees every message |
| Queue per worker type `jobs.<worker_type>` | Consumer group per worker type, default group id `<topic>.<worker_type>`, overridable |
| Competing consumers in a worker type | Partitions within the group |
| `message.ack()` after success | Commit offset after processing (manual commit, at-least-once) |
| `nack(requeue=True)` | Don't commit + `seek()` back to the message offset → re-polled |
| `reject(requeue=False)` (hard failure / malformed JSON) | Commit anyway (drop is intentional and terminal) |
| `prefetch_count: 1` | Sequential per-consumer processing loop |
| Declare exchange/queue on setup | Verify topic exists (raise `PubSubSetupError`); optional create-if-missing for local |
| Mongo-based retry + retry-worker republish | Unchanged — broker-independent |
| Worker self-selection by `data_type` | Unchanged — application code |

## How the parity works

- **Fanout = one topic + one group per worker type.** A topic broadcasts every
  message to every consumer group, so giving each worker type its own group
  (`<topic>.<worker_type>`) reproduces the fanout exchange: every worker type
  sees every message and self-selects what to process. The group id is
  pre-resolved in `warren.runtime.backends.create_consumer_manager` (the single
  place that owns the naming convention), so the convention lives in one place;
  override it with `kafka.consumer.group_id` on platforms with pre-assigned
  group names.

- **Competing consumers = partitions.** Multiple instances of the same worker
  type join the same group and the broker splits the topic's partitions across
  them — each message in a group goes to exactly one instance. Scale a worker
  type by adding replicas, exactly as on RabbitMQ. The partition count
  (`kafka.topic.num_partitions`, default 6) bounds in-group parallelism.

- **Ack/nack/reject = commit/seek.** The consumer manager runs a sequential
  poll loop (one message in flight — the faithful mapping of `prefetch_count:
  1`) and commits the offset only after processing resolves. Success or a
  terminal/hard failure commits past the message (ack / `reject(requeue=False)`
  — the drop is intentional); a soft failure with no publisher seeks back
  without committing so the message is re-polled (`nack(requeue=True)`).

- **Ordering is per-partition only.** Kafka guarantees order within a partition,
  not across the topic. This is fine for Warren: documents flow independently,
  so cross-document ordering is never required.

- **Producer publishes key=None, round-robin.** Messages are sent without a key,
  so the cluster distributes them round-robin across the topic's partitions
  (fanout parity). Routes are rejected — a topic plays the role of a fanout
  exchange and never uses a routing key.

- **Long processing is bounded by `max_poll_interval_ms`** (default 600000 ms /
  10 min). A consumer that does not poll within that window is evicted from the
  group and its in-flight (uncommitted) message is redelivered after the
  rebalance. Raise it for workers whose per-message processing can exceed the
  default.

- **Retry is broker-independent.** The Mongo-based retry store and the
  retry-worker republish path are unchanged across backends — soft failures are
  published as `data_type: "soft-failure"` envelopes onto the same topic, and a
  retry worker on its own group picks them up.

## Topic provisioning

On setup the consumer manager and publisher both call `ensure_topic`:

- If the topic exists, nothing happens.
- If it is missing and `kafka.topic.create_if_missing` is **true** (enable
  locally), it is created idempotently with `num_partitions` /
  `replication_factor`.
- If it is missing and creation is **disabled** (the default — platforms that
  provision topics out-of-band), setup fails loud with `PubSubSetupError`.

## Resetting local state

`runtime_scripts.purge_queues` is **RabbitMQ-only** and refuses to run with
`backend: kafka`. To reset Kafka state, delete the topic and its consumer
groups (`kafka-topics --delete` / `kafka-consumer-groups --delete`) or recreate
the local broker container.
