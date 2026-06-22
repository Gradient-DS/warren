"""
Watch a job progress while it runs.

Looks up a job by its ``--job-name`` (stored in metadata at publish time) and
polls the job's status and per-stage counts, printing a live view until the job
completes (or ``--once`` for a single snapshot). Works with any of the example
pipelines — point it at the same ``--config-file`` you started the workers with.

Usage:
    python -m examples.inspect_job \
        --job-name demo-001 --config-file examples/fake/config.yaml
"""

import argparse
import asyncio
import logging
from pathlib import Path

from pymongo import AsyncMongoClient

from warren.runtime.config import RuntimeConfig
from warren.storage.job_results.mongodb import MongoDBJobResultsStore
from warren.storage.jobs.mongodb import MongoDBJobStore


async def _find_job_id(client: AsyncMongoClient, database: str, job_name: str) -> str:
    doc = await client[database]["jobs"].find_one({"metadata.job_name": job_name})
    if doc is None:
        msg = f"No job found with metadata.job_name={job_name!r} in db {database!r}"
        raise SystemExit(msg)
    return doc["job_id"]


def _render(status: dict, stages: list[dict]) -> str:
    lines = [
        f"  {'stage':<22} {'total':>6} {'ok':>6} {'soft':>6} {'hard':>6}",
        f"  {'-' * 22} {'-' * 6} {'-' * 6} {'-' * 6} {'-' * 6}",
    ]
    for s in sorted(stages, key=lambda x: x["data_type"]):
        lines.append(
            f"  {s['data_type']:<22} {s['total']:>6} {s['succeeded']:>6} "
            f"{s['soft_failed']:>6} {s['hard_failed']:>6}"
        )
    done = status.get("completed", False)
    state = "COMPLETED" if done else "running"
    if done and status.get("with_failures"):
        state = "COMPLETED (with failures)"
    lines.append(f"  state: {state}")
    return "\n".join(lines)


async def _watch(config: RuntimeConfig, job_name: str, *, once: bool) -> None:
    client = AsyncMongoClient(host=config.mongodb.host, port=config.mongodb.port)
    job_store = MongoDBJobStore(client=client, database_name=config.mongodb.database)
    results_store = MongoDBJobResultsStore(
        client=client, database_name=config.mongodb.database
    )
    try:
        job_id = await _find_job_id(client, config.mongodb.database, job_name)
        print(f"job {job_name} -> {job_id}")
        while True:
            status = await job_store.get_status(job_id)
            stages = await results_store.get_stage_counts(job_id)
            print(f"\n[{job_name}]")
            print(_render(status, stages))
            if once or status.get("completed"):
                break
            await asyncio.sleep(2)
    finally:
        await client.close()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Watch a job's progress")
    parser.add_argument("--job-name", required=True, help="Job name (in metadata).")
    parser.add_argument(
        "--config-file", type=Path, required=True, help="RuntimeConfig YAML."
    )
    parser.add_argument(
        "--once", action="store_true", default=False, help="Print one snapshot, exit."
    )
    return parser.parse_args()


def main() -> None:
    # This is a read-only viewer; keep the driver/loop chatter out of the table.
    for noisy in ("pymongo", "asyncio"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    args = _parse_args()
    config = RuntimeConfig.from_yaml(args.config_file)
    asyncio.run(_watch(config, args.job_name, once=args.once))


if __name__ == "__main__":
    main()
