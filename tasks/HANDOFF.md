# Handoff — flexible routing PR, Kafka merge, OSS prep

**Date:** 2026-07-06 · **Branch:** `feat/topic-exchange-support` (PR #3 → `main`)
**Note:** `tasks/` is gitignored; this file is force-added deliberately as the
team handoff. Drop or move it before open-sourcing.

## Where things stand

**PR #3 is ready for review** (Ben reviews next; nobody else has). CI green
(`test` + `bare-install`), `MERGEABLE`, no conflicts. Version deliberately
left at **0.2.2** — bump decided at merge time (breaking change: exchange
moved from `config.yaml` into `PipelineSpec`; `terminal` flag removed;
`publish: PublishSpec | None`).

The PR now contains three bodies of work:

1. **Flexible routing** (original scope): topic/direct exchanges, per-worker
   `binding_key` + `publish`, `CapabilityWorkerBase` (`accepts`/`produces`),
   job-defined `RoutingPlan` + `RoutingPlanRouter`, two-layer validation,
   framework-managed observer exchange for direct pipelines,
   control-publisher split + retry-by-replay. One exchange per pipeline
   (multi-exchange was built in Phase 3 and deliberately reverted).
   Full decision record: PR description + `warren/docs/routing.md`.
2. **Merge of main** (commit `7c6f40f`): reconciled main's Kafka backend +
   S3/HTTP resolvers with the routing model. Resolution: `warren/runtime/
   backends.py` keeps main's factory architecture but carries the routing
   signatures (exchange-from-spec, binding_key, data/control/observer
   publisher split); **Kafka is fanout-only by design** — `_require_kafka_fanout`
   fails fast on topic/direct; `KafkaConsumerManager` adopted the publisher
   split with byte-identical envelopes (parity tests enforce it). Main's
   `_resolve_cloud` (gcs|s3) + HTTP resolver were ported into the runner.
3. **Docs refresh for OSS** (commit `8df4122`): repositioned as a generic
   *distributed processing* framework (RAG = flagship example, not scope);
   README gained "How Warren compares" (Airflow/Temporal/Flink/Celery +
   composition) and "Choosing a backend" (operational choice; Redpanda/Event
   Hubs wire-compat); USAGE.md / runtime README de-staled (Kafka exists,
   exchange in spec, built-in S3/HTTP resolvers, current
   `WorkerFactoryContext`); claim-check framing on the fetcher layer;
   fan-in documented as the planned DAG direction in `routing.md`;
   `document_store.md` marked historical.

**Local env note:** `aiokafka` and `boto3` were pip-installed into `.venv` to
run the merged suite (CI installs all extras). 109 tests green, ruff clean.

## The strategy record

`tasks/open-source-readiness-and-transport-generalization.md` is the
decision record driving what comes next (positioning, "reason from pipeline
needs, not backends", the minimal transport SPI, promoted/demoted items,
action checklists). Highlights:

- **Positioning:** per-item message-flow graphs, no scheduler; NOT stage-based
  (stages are emergent); fan-out works today, fan-in/join = planned DAG
  direction (`JoinWorkerBase` sketch is in the doc and `routing.md`).
- **Two hats:** Warren OSS (must be general) vs Gradient platform (tenancy,
  fairness/admission control, replay-from-results-store, GDPR lifecycle —
  built on top).
- **`doc_id`/"document" is Mongo vocabulary** (generic item) — do NOT rename;
  only positioning surfaces needed rewording (done in `8df4122`).
- **Transport plan (in order):** neutral `ChannelSpec` replacing
  `RMQExchangeConfig` in the spec → split `Route` into `selection_key` vs
  `affinity_key` (wire the Kafka message key — unlocks per-key ordering) →
  `PubSubBackend` protocol + capability matrix (guard becomes declarative) →
  `warren-run` launcher consolidation → Kafka filtered-selective only if a
  use case demands. Redis Streams = best third backend (already a dependency).

## Field findings (genai-utils production pipeline, warren 0.2.2)

Reviewed `genai-utils/document_processing/distributed/pipeline` against the
maatwerk/bulk-dataset pain points. Section 5b of the takeaways doc has the
full record; the short version:

- The pipeline **hand-rolled job-defined routing** on fanout
  (`preprocessing_required` list + `should_process` gating + re-emit) —
  empirical validation of the routing PR. Migration: upgrade to 0.3 →
  `accepts`/`produces` on fanout (neutral) → `direct` + `RoutingPlan` via the
  pipeline API → delete the re-emit machinery. Rule: `data_type` = payload
  shape; position = plan.
- "Redeploying all workers" is **packaging, not the model**: pin per-worker
  image tags (Helm chart already supports it) and mount `pipeline_spec.py`
  via ConfigMap (lazy factory wrappers exist for exactly this).
- New roadmap item: **worker self-registration** (capabilities in Mongo at
  startup; `validate_routing_plan` reads live registrations) — enables the
  click-to-compose pipeline UI they want.
- Caveat: direct pipelines pay one extra publish per message (observer echo)
  — measurable at bulk volume.

## Next actions

1. **Ben:** review PR #3; decide version bump at merge (suggest 0.3.0).
2. **Before OSS** (small): drop/move this file and `tasks/`; the README
   positioning work is done in `8df4122` — remaining checklist in the
   takeaways doc §Action checklist.
3. **Follow-up PRs (in order):** ChannelSpec + Route split + PubSubBackend
   protocol (promote the takeaways doc to `warren/docs/` as part of it) →
   `warren-run` consolidation → genai-utils 0.3 migration (separate repo).
4. **Open questions parked for review:** one-exchange collapse (was it right,
   given multi-exchange was built?), nominal typing / schema registry,
   `FilteringWorkerBase`/`CapabilityWorkerBase` merge (flagged in
   `routing.md`), deferred D11 terminal-set completion (now DAG-prerequisite).
