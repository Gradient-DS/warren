# Examples

Two kinds of runnable examples. Full run instructions (infrastructure, workers,
publishing, inspecting) live in the repo [README](../README.md) — this is just
the map.

## `rag/` — real workload

Real PDFs → text → chunks → OpenAI embeddings: the first three stages of a RAG
pipeline. The parser **downloads** each PDF from a URL. Bring your own
`OPENAI_API_KEY`; defaults to two arXiv papers and takes your own via `--url`.
Start here if you want to see Warren do actual work. See
[Get started for real](../README.md#get-started-for-real--pdfs-to-embeddings).

## `exchanges/` — one synthetic pipeline, three exchanges

The **same** parse → chunk → embed pipeline over synthetic data, wired onto each
of Warren's exchange types so the routing is what stands out. See
[Choosing an exchange](../README.md#choosing-an-exchange) for when to use which.

| Dir | Exchange | What it shows |
|-----|----------|---------------|
| `exchanges/fanout/` | `fanout` | Every worker self-selects (the quickstart). Also runs on Kafka via `config.kafka.yaml`. |
| `exchanges/topic/` | `topic` | Broker routes by `data_type`. |
| `exchanges/direct/` | `direct` | Per-job `RoutingPlan` over capability workers. |

Shared across the three: `data.py` (the stand-in documents), `workers/` (the
synthetic filtering workers), `capability_workers.py` (the declarative variants
the direct example uses), `documents_publisher.py`, and `publish.py` (the shared
fanout/topic publisher — direct has its own under `direct/publish.py`).

## `inspect_job.py`

A small read-only CLI that polls a job by name and prints a live per-stage view
until it completes. Works against any of the pipelines — point it at the same
`--config-file` you started the workers with.
