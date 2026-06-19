# Plan: flexible routing + get-started examples

## Documents (read in this order)
1. `tasks/examples-findings.md` — investigation: current state, exchange support gaps, OSS-readiness.
2. `tasks/routing-design.md` — **authoritative design & decisions (D1–D14)**, for colleague review.
3. `tasks/phase1-implementation-plan.md` — Phase 1 file-by-file plan + verification gate.

## Status
- [x] Investigate repo, exchanges, OSS readiness.
- [x] Design dialogue → decisions D1–D14 agreed (Ben + Claude).
- [x] Documented for colleague review before PR.
- [ ] **Colleague review of `routing-design.md`** ← we are here.
- [ ] Phase 1 implementation.
- [ ] Phase 2, Phase 3.
- [ ] Examples branch (`feat/examples`).

## Branch / phase plan
- `feat/topic-exchange-support` (this branch) holds the design docs.
- **Phase 1** — exchange model (single-exchange, single-publish) + control-publisher split + deploy-time
  validation. Examples A (fanout) + B (topic). Own PR.
- **Phase 2** — capabilities + job-defined routing (`RoutingPlanRouter`, submission-time validation).
- **Phase 3** — multi-publish + multiple exchanges + per-exchange observers + idempotent retry +
  terminal-set completion. Example C (fanout + topic/direct at once).
- `feat/examples` rebases on the merged phases; final get-started examples land there.

## Separate, deferred (tracked, not blocking)
- `chore/oss-cleanup` — stale doc paths (`distributed/`, `e2e_test/`, `check_completion.py`), dead
  `storage/documents/sources.py`, empty `warren/__init__.py` + `__version__`, pubsub tests, internal
  RFC headers/author handles. See `tasks/examples-findings.md` §3.

## Examples (land on `feat/examples`, verify the phases)
- **A — fanout RAG**: parse→chunk→embed; add a sync worker (`SyncProcessingWorkerBase`) + `inspect_job.py`.
- **B — topic**: route by `data_type` (or job-defined `RoutingPlan`).
- **C — fanout + topic at once**: one worker publishes to both (Phase 3).
</content>
