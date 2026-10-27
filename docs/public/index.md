---
title: Overview
description: Process independent items through a pipeline of workers over RabbitMQ, Kafka or memory, with the payloads in MongoDB and Redis.
---

Warren is a message-driven framework for distributed processing. Independent items flow through a pipeline of
workers. Each worker selects the messages that it handles, stores its results, and publishes references for the
next workers. The messages stay small, because the payloads are in storage. To scale a worker type, add replicas
that share its queue or consumer group.

Warren gives the worker base classes and the interfaces for pubsub and storage. The runtime connects them to
RabbitMQ or Kafka, with MongoDB and Redis, or to in-memory implementations for local runs and tests.

## Install

Warren needs Python 3.12 or later. Install the extra of the transport that you use:

```bash
pip install "warren[rmq]"
```

| Extra | Adds |
|---|---|
| `rmq` | aio-pika, for the RabbitMQ transport |
| `kafka` | aiokafka, for the Kafka transport |
| `gcs` | google-cloud-storage, for documents in Google Cloud Storage |
| `s3` | boto3, for documents in S3 |
| `http` | httpx, for documents at a URL |

The memory transport needs no extra. If you select a transport or a document resolver without its extra, Warren
raises [`OptionalDependencyError`](py:warren.OptionalDependencyError).

## Where to start

- The [Quickstart](https://github.com/Gradient-DS/warren#quickstart) in the repository runs a three-stage example
  pipeline on RabbitMQ, MongoDB and Redis.
- [`warren.runtime`](py:warren.runtime): compose a pipeline with [`PipelineSpec`](py:warren.runtime.PipelineSpec)
  and [`WorkerSpec`](py:warren.runtime.WorkerSpec), and load the settings with
  [`RuntimeConfig`](py:warren.runtime.RuntimeConfig). For a local run without infrastructure, set
  `backend: memory` in the `RuntimeConfig`, and run the runners in one process with
  [`run_in_process`](py:warren.runtime.run_in_process).
- [`warren.workers`](py:warren.workers): the worker base classes, such as
  [`AsyncProcessingWorkerBase`](py:warren.workers.AsyncProcessingWorkerBase), and the message that a worker
  receives, [`ProcessingMessage`](py:warren.workers.ProcessingMessage).
- [`warren.pubsub`](py:warren.pubsub): the publisher and consumer contracts and the routing. The transports are
  [`warren.pubsub.rabbitmq`](py:warren.pubsub.rabbitmq), [`warren.pubsub.kafka`](py:warren.pubsub.kafka) and
  [`warren.pubsub.memory`](py:warren.pubsub.memory).
- [`warren.storage`](py:warren.storage): the stores for documents, results, jobs and the cache, each in its own
  subpackage.
