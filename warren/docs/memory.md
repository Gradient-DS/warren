# In-process backend (`memory`)

`backend: memory` runs a whole pipeline inside one Python process: no RabbitMQ,
no Kafka, no MongoDB, no Redis. It exists for two things: trying warren on a
laptop, and running whole pipelines inside a test.

It is not a production mode. Nothing is persisted, nothing survives a restart,
and nothing crosses a process boundary.

## What it is

- **Transport:** `warren.pubsub.memory`. One broker per process, built on
  `asyncio.Queue`, with RabbitMQ exchange semantics: `fanout`, `direct` and
  `topic` all work, a queue is named `<exchange>.<worker_type>`, instances of one
  worker type compete for its messages, and a message published to an exchange
  nobody is bound to is dropped.
- **Stores:** pure-Python implementations of the store Protocols
  (`MemoryDocumentStore`, `MemoryJobStore`, `MemoryJobResultsStore`,
  `MemoryCache`), shared between workers by a `MemoryStoreRegistry`.
- **Same semantics where it matters:** configurable handler concurrency
  (one by default), the same soft-failure and hard-failure envelopes, the
  same retry math, the same health states.

## Running a pipeline

    config = RuntimeConfig(backend="memory", health=HealthConfig(enabled=False))
    infra = await create_runtime_infrastructure(config)
    stores = MemoryStoreRegistry()
    try:
        runners = await create_in_process_runners(config, PIPELINE, infra=infra, stores=stores)
        await run_in_process(runners, until=publish_and_wait)
    finally:
        await close_runtime_infrastructure(infra)

`until` is called once every runner is consuming. Publish your messages there and
return when the work is done; the run then stops and everything is torn down.
`examples/rag/run_local.py` is a complete example, and
`tests/runtime/test_in_process_pipeline.py` shows the same thing in a test.

Turn the health endpoint off (`health.enabled: false`): every runner shares the
process, so they would all try to bind the same port.

## Handler limits and instances

Configure limits per consumer manager in the runtime configuration:

```yaml
backend: memory
memory:
  concurrency: 3
  handler_timeout_seconds: 30
  on_shutdown_timeout: 30
```

For direct construction, pass these settings to `MemoryConsumerManager`.
Concurrency defaults to `None`, preserving one handler at a time; an explicit
value must be at least one.
Timeouts default to `None` and use the existing soft-failure retry policy.

Pass `instances={"transform": 2}` to `create_in_process_runners` to create
`transform-0` and `transform-1`. Unspecified worker types get one instance;
counts must be positive integers. Instances share stores and the worker
type's queue. Status and retry runners remain single instances.
Handler concurrency applies to each instance.

## Limits

- One process, one event loop. `RetryWorker` schedules retries with in-process
  timers, which is fine here and is the only run mode this backend supports.
- No persistence, no redelivery after a crash, no backpressure: queues are
  unbounded and every payload lives on the heap.
- `WorkerFactoryContext.mongo_client` and `redis_client` are `None`. A worker
  factory that builds its own stores from raw clients does not run on this
  backend; use the stores in `ctx.stores` and `ctx.document_store`.
- `MemoryDocumentStore.query` supports flat equality only.
- The job publication worker is not supported yet; publish directly, as the
  example does.
- It validates your pipeline wiring and worker logic. It does not exercise the
  MongoDB or Redis store code, so it says nothing about indexes or aggregations.
