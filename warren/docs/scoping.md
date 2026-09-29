# Scopes

A scope is an opaque namespace matching `^[a-z0-9][a-z0-9-]{0,39}$`.
Scoped storage prevents accidental reads across namespaces and provides a unit
of erasure. It is not an authorization boundary: a compromised worker holds
credentials for every scope, and the broker is shared.

Enable it in runtime YAML:

```yaml
scoping:
  enabled: true
  required: true
  database_prefix: wr_
```

Publish messages with a top-level `scope`, for example `"scope": "alpha"`.
Derived messages, failures, retries, observer echoes and completion signals
carry it forward. Handlers receive its raw value through
`warren.storage.scoping.get_current_scope()`, including synchronous handlers
run in executor threads. Application code that submits its own executor work
must use `contextvars.copy_context().run` too.

Validation happens only when scoped storage is accessed. A missing required
scope raises `HardFailureException("missing scope")`; an invalid value raises
`HardFailureException("malformed scope")`. Observers and control workers can
handle either without generating another scope failure. With `required: false`,
an absent scope uses the configured default database and unprefixed cache keys;
malformed values still fail. Disabled scoping preserves existing storage behavior.

Content goes to `wr_<scope>` by default: per-role results, binary results and the
`documents` registry. Indexes are created lazily on first access. Redis content
keys, including fetcher keys, start with `s:<scope>:`. Jobs, job results,
publishing results and retries stay in `mongodb.database`, with an indexed
`scope` field only when the raw value is valid. Job IDs remain globally unique.
Scoped retries read directly from MongoDB so cached retries cannot survive erasure.

Factories receive `ctx.scoped_database`, a callable returning the current MongoDB
database, and `ctx.current_scope`. Pass the same resolver as `scoped_database=`
when building additional result stores. For manually built caches and control
stores, pass `scoping_enabled=config.scoping.enabled`; caches also accept
`scope_required=config.scoping.required`. Injected stores, including in-memory
stores, are the application's responsibility.

An API can pass `scope=` to `JobStore.create_job` and
`JobDocumentsPublisher.publish_job`. Outside a handler, wrap direct content-store
access in a context token:

```python
from warren.storage.scoping import current_scope

token = current_scope.set("alpha")
try:
    await results.store({"value": 1}, "item-1")
finally:
    current_scope.reset(token)
```

Stop traffic and workers for the scope before erasing it, and remove any queued
messages that could recreate its data. Then:

```python
from warren.storage.scoping import erase_scope

counts = await erase_scope(
    infra,  # Or (mongo_client, redis_client).
    "alpha",
    control_database=config.mongodb.database,
    prefix=config.scoping.database_prefix,
)
```

The function drops the scoped database, falling back to individual collection
drops if MongoDB denies `dropDatabase`. It uses Redis SCAN with count 1000 and
UNLINK batches of at most 1000 keys, then deletes matching `scope` rows from all
collections in the control database, including custom retry collections.
The returned counts are `collections`, `redis_keys` and `control_rows`.
Errors propagate; erasure is not transactional and can be retried. Unlabelled
control rows cannot be attributed to a scope. Restart workers before resuming
traffic so every process recreates indexes for any new content.
