# Warren

[![tests](https://github.com/Gradient-DS/warren/actions/workflows/tests.yml/badge.svg)](https://github.com/Gradient-DS/warren/actions/workflows/tests.yml) [![PyPI version](https://img.shields.io/pypi/v/warren)](https://pypi.org/project/warren/) [![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)

Warren is an application-agnostic, message-driven **distributed processing
framework**. Independent items flow through workers over RabbitMQ or Kafka.
Workers select the messages they handle, store their results, and publish
references for downstream workers. Messages stay small; payloads live in storage.
Scale a worker type by adding replicas that share its queue or consumer group.

The framework provides worker base classes and storage and pubsub interfaces.
The [runtime](warren/runtime/USAGE.md) wires them to RabbitMQ or Kafka with
MongoDB and Redis, or to in-process memory implementations for local use.

## Features

- **Transport choice:** RabbitMQ and memory support fanout, topic and direct
  routing; Kafka supports fanout. Workers can bind to several routing keys.
- **Durable retries:** configurable backoff and retry limits, with one active
  retry scheduler elected through a MongoDB lease and standby takeover.
- **Handler limits:** configurable concurrency and timeouts, readiness and
  liveness endpoints, and in-flight handler counts.
- **Job tracking:** per-item, per-stage outcomes, indexed failure counts, and
  conditional completion updates that prevent competing completion signals.
- **Storage:** batched result writes, ordered reads for results split into
  parts, explicit cache expiry, and optional MongoDB job-record retention.
- **Deployment configuration:** connection strings, credentials, TLS, pool
  limits, startup retries, environment expansion, and rejection of unknown keys.
- **Optional scopes:** isolated content databases and cache namespaces,
  propagated handler context, and scope erasure with deletion counts.

## Installation

Requires Python 3.12+. Install the transport extra you use:

```bash
pip install "warren[rmq]"
```

Use `warren[kafka]` for Kafka. The memory backend needs no transport extra.
Payload resolvers are optional too: `warren[gcs]`, `warren[s3]`, and
`warren[http]`. Selecting a transport or resolver without its extra raises
`OptionalDependencyError`.

The examples below run from a clone:

```bash
git clone https://github.com/Gradient-DS/warren.git
cd warren
pip install -e ".[dev]"
```

## Quickstart

### Synthetic pipeline with Docker

`examples/exchanges/fanout/` runs three stages over pre-baked data. Four input
items produce eighteen final results without external data or API credentials.
The stage and collection names below are those of the bundled example.

**1. Start RabbitMQ, MongoDB, and Redis:**

```bash
docker run -d --name warren-rabbitmq -p 5672:5672 rabbitmq:4
docker run -d --name warren-mongodb -p 27017:27017 mongo:8
docker run -d --name warren-redis -p 6379:6379 redis:7
```

**2. Start the processing workers, one per terminal:**

```bash
python -m runtime_scripts.start_worker --pipeline-spec ./examples/exchanges/fanout --worker-type document_parser --config-file examples/exchanges/fanout/config.yaml
python -m runtime_scripts.start_worker --pipeline-spec ./examples/exchanges/fanout --worker-type text_chunker --config-file examples/exchanges/fanout/config.yaml
python -m runtime_scripts.start_worker --pipeline-spec ./examples/exchanges/fanout --worker-type embedding_generator --config-file examples/exchanges/fanout/config.yaml
```

Start the support workers in two more terminals for completion tracking and
retry management:

```bash
python -m runtime_scripts.start_job_status_worker --pipeline-spec ./examples/exchanges/fanout --config-file examples/exchanges/fanout/config.yaml
python -m runtime_scripts.start_retry_worker --pipeline-spec ./examples/exchanges/fanout --config-file examples/exchanges/fanout/config.yaml
```

For local processes sharing a host, add `health: {enabled: false}` to the
example config to avoid sharing the default health port, 8080. In deployments,
use separate network namespaces or configure a distinct port per process.

**3. Publish a job:**

```bash
python -m examples.exchanges.publish --job-name demo-001 --config-file examples/exchanges/fanout/config.yaml
```

Results land in the `parsed_documents`, `chunks`, and `embeddings` collections
of the `warren_fanout` database. The example config sets the database name.

**4. Watch completion:**

```bash
python -m examples.inspect_job --job-name demo-001 --config-file examples/exchanges/fanout/config.yaml
```

With the job-status worker running, this prints per-stage counts until the job
completes. Start more instances of a processing worker to share its load.
Multiple retry workers may run too; only the lease holder schedules retries.

### Local pipelines without infrastructure

Set `backend: memory` and use `create_in_process_runners` and `run_in_process`
from `warren.runtime.in_process`. The runners share one `RuntimeInfra` and
`MemoryStoreRegistry`. Pass `instances={"transform": 2}` to create two instances
of a worker type named `transform`; unspecified types get one.

Set `health.enabled: false` when runners share a process. Memory supports one
process and event loop, keeps all state in memory, and loses it on exit. It is
intended for local runs and tests. See the [memory usage guide](warren/docs/memory.md).

### Running on Kafka instead

Install `warren[kafka]`, start a broker at `localhost:9092`, and point the fanout
commands at `examples/exchanges/fanout/config.kafka.yaml`. That config selects
`backend: kafka` and creates the `jobs` topic if missing. MongoDB and Redis are
still required. Kafka rejects topic and direct exchange types at startup.
See the [Kafka guide](warren/docs/kafka.md).

## Runtime configuration

`RuntimeConfig.from_yaml` expands `${VAR}` in YAML string values and reports an
unset variable by name. Unknown fields in modeled runtime sections are rejected.
Omitted fields retain their defaults.

### Connections and startup

Distributed runners accept connection URLs and individual connection fields.
For example, with `MONGODB_URI` and `REDIS_URL` set in the environment:

```yaml
mongodb:
  uri: ${MONGODB_URI}
  database: my_pipeline
  max_pool_size: 20
  server_selection_timeout_ms: 5000
redis:
  url: ${REDIS_URL}
  max_connections: 20
  socket_timeout: 5.0
startup:
  attempts: 5
  initial_delay_seconds: 1.0
  max_delay_seconds: 30.0
```

MongoDB also accepts `host`, `port`, `username`, `password`, `auth_source`, and
`tls`; a URI replaces host and port, and explicit options override URI options.
Redis accepts `host`, `port`, `username`, `password`, `db`, and `ssl`; URL options
win over separate fields. Use `rediss://` for TLS with a Redis URL. Connection
strings and passwords are masked in configuration representations.

Startup connects the transport and pings MongoDB and Redis. Failed attempts
close their connections before retrying with exponential backoff. The defaults
are one attempt, a one-second initial delay and a 30-second delay cap. Driver
timeouts still bound individual operations; startup settings do not impose a
total deadline. Memory skips external connections and startup retries.
See [connection details](warren/runtime/USAGE.md#runtimeconfig) for precedence
and available fields. The bundled publishing and inspection examples use local
host/port connections; they are not deployment configuration templates.

### Concurrency, timeouts, and health

```yaml
rabbitmq:
  consumer:
    concurrency: 4
    prefetch_count: 4
    handler_timeout_seconds: 60
```

| Backend | Handler settings | Default when concurrency is omitted |
| --- | --- | --- |
| RabbitMQ | `rabbitmq.consumer` | `prefetch_count`, normally 1; zero is unlimited |
| Kafka | `kafka.consumer` | One handler; concurrency above 1 is rejected |
| Memory | `memory` | One handler per instance |

An explicit concurrency is a positive integer. RabbitMQ prefetch is at least
that limit. Handlers must support concurrent calls when the limit exceeds one.
All three sections accept `handler_timeout_seconds`, a positive number or
`null` to disable the deadline, which is the default.

Timeouts use the existing soft-failure retry policy. A synchronous handler runs
in an executor thread; timing out cannot stop that thread, and it retains its
concurrency slot until it finishes. Health responses include
`in_flight_handlers`. `/live` returns 503 when the latest health sample reports
a handler running longer than twice its timeout, including one that ignores
cancellation. `/ready` reports whether the consumer is connected and available.

### Retry ownership

```yaml
retry:
  enabled: true
  collection_name: retries
  lease_ttl_seconds: 30
  policy:
    max_delay_cap: 300
```

Start a retry worker alongside distributed processing workers. The memory
runner helper includes one when `retry.enabled` is true. Distributed retry
runners use one lease per retry database and collection, stored in
`<collection_name>_lease` in `mongodb.database`. Each process has a unique
holder ID. The lease TTL must be a positive integer; it defaults to 30 seconds.

The holder renews and standbys poll every TTL/3. Only the holder schedules and
republishes. Standbys still persist received failures; the holder scans for
pending work at each renewal, so those retries can incur an extra poll interval.
Lease loss, renewal failure, or expiry cancels local timers and publish tasks.
After expiry, a standby takes ownership and recovers persisted work. Shutdown
leaves the lease to expire. Keep worker clocks synchronized; expiry uses wall
clock time, and lease updates use majority write concern.

Retries remain at-least-once. A crash after publishing but before deleting the
stored retry can cause replay. Replacement retries are protected by conditional
cleanup. Custom injected retry stores must support
`delete(key, expected={"generation": ...})`; the built-in MongoDB, memory, and
cached stores do. Direct `RetryWorker` users must inject a lease and call
`start()` for distributed scheduling. Memory runners use no MongoDB lease.

### Cache expiry and record retention

```yaml
documents:
  cache_ttl_seconds: 86400
results:
  cache_ttl_seconds: 3600
retention:
  job_records_ttl_seconds: 604800
  job_records_max_age_seconds: 2592000
```

The cache values above are defaults and must be positive. Fetched payloads use
the same TTL for shared and job-scoped keys. Binary result caches default to
3600 seconds; retry caches use the policy's maximum delay cap plus 60 seconds.
Redis cache entries always have a positive expiry.

Both retention settings default to `null`. The first expires MongoDB job
completion, result-status, and publishing-status records; the second bounds job
age from creation, including unfinished jobs. When both are set, maximum age
must be at least the record TTL. Values must be positive. Setup creates or
replaces TTL indexes only for configured values; `null` leaves indexes untouched.
Removing a TTL index is an operator action.
See [job stores](warren/docs/job_stores.md) for timestamp fields and factory wiring.

### Optional scopes

```yaml
scoping:
  enabled: true
  required: true
  database_prefix: wr_
```

Scoping defaults to disabled. When enabled, a message's top-level `scope`
selects its content database and Redis namespace. Scopes are opaque names
matching `^[a-z0-9][a-z0-9-]{0,39}$`. Missing required or malformed scopes fail at scoped
storage access. Worker outputs, failures, retries, and job signals preserve the
scope; handler context reaches executor threads too.

Content lives in `wr_<scope>` and Redis keys prefixed with `s:<scope>:`. Control
records stay in `mongodb.database`, labelled with valid scopes. Factories
receive `ctx.scoped_database` and `ctx.current_scope`; injected stores remain
the application's responsibility. Scopes separate storage namespaces and are
not an authorization boundary. See [scoping and erasure](warren/docs/scoping.md)
for optional scopes and `erase_scope`, including quiescing traffic before erasure.

## Defining your own pipeline

A pipeline directory contains `pipeline_spec.py`, exporting a `PIPELINE:
PipelineSpec`, and a runtime `config.yaml`. Each worker owns an async
`create(ctx: WorkerFactoryContext)` factory. `WorkerSpec` supplies its store
roles, factory, routing bindings, and optional downstream publisher.

### Choosing an exchange

| Exchange | Routing | Supported backends |
| --- | --- | --- |
| `fanout` | Each worker type receives every message and self-selects | RabbitMQ, Kafka, memory |
| `topic` | Binding patterns select routing keys, conventionally `data_type` | RabbitMQ, memory |
| `direct` | Exact keys address workers; jobs can supply a `RoutingPlan` | RabbitMQ, memory |

Fanout needs no binding keys. On topic or direct exchanges, set
`WorkerSpec(binding_keys=("input.*", "retry.#"), ...)` for topic patterns or
exact keys for direct routing. `binding_key` remains a single-key constructor
alias. Overlapping bindings deliver a message only once per matching queue.
The `publish` field supplies a `PublishSpec`, or `None` for a worker with no
downstream data publisher.

The three `examples/exchanges/{fanout,topic,direct}` directories wire the same
synthetic stages to each exchange type. See [routing](warren/docs/routing.md)
and the [runtime usage guide](warren/runtime/USAGE.md) for factories, capability
workers, job-defined plans, custom runners, and the launcher flags.

## How Warren compares

Warren processes each item independently through a graph of persistent workers.
Fan-out is supported; fan-in and joins remain on the [roadmap](ROADMAP.md).

| Tool category | Typical work | Warren's focus |
| --- | --- | --- |
| Batch orchestrators such as Airflow or Dagster | Scheduled task graphs over datasets | Continuous per-item messages and job tracking |
| Durable workflows such as Temporal | Long-running, stateful workflows | Independent items flowing through processing stages |
| Stream analytics such as Flink | Windows, joins, and aggregations | Worker-defined per-item processing |
| Task queues such as Celery or RQ | Function invocation | Pipeline topology, routing, and per-stage outcomes |

An external scheduler can submit Warren jobs and poll completion while Warren
handles the per-item processing.

## Launchers

`runtime_scripts/` also installs console scripts:

| Console script | Module | Purpose |
| --- | --- | --- |
| `warren-worker` | `runtime_scripts.start_worker` | Any worker type from a `PipelineSpec` |
| `warren-job-publication-worker` | `runtime_scripts.start_job_publication_worker` | Job submission and per-item publication |
| `warren-job-status-worker` | `runtime_scripts.start_job_status_worker` | Completion detection and progress tracking |
| `warren-retry-worker` | `runtime_scripts.start_retry_worker` | Soft-failure replay with backoff |
| `warren-purge-queues` | `runtime_scripts.purge_queues` | Queue and exchange cleanup between runs |

## Development

```bash
pip install -e ".[dev]"
ruff check && ruff format --check && pytest -q
```

See [CONTRIBUTING.md](CONTRIBUTING.md). Report security issues through
[SECURITY.md](SECURITY.md).

## License

Apache-2.0. See [LICENSE](LICENSE).
