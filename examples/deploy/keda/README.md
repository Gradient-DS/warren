# Autoscale a Warren worker with KEDA

Apply one example, matching your pipeline's broker. It points to an **existing**
Kubernetes Deployment named `warren-document-parser` in the same namespace as the
`ScaledObject`. That Deployment runs the single worker type called `document_parser` in
`PipelineSpec.workers`; do not target a Deployment that launches all workers.
Install KEDA and its CRDs, then set these values before applying either file:

- [RabbitMQ queue depth](rabbitmq.yaml): `queueName` is
  `<exchange.name>.<worker_type>` (here `jobs.document_parser`). Create the referenced
  `warren-rabbitmq-connection` Secret with a `host` key holding the broker AMQP
  connection URI, including the correct vhost. Don't commit the URI. The
  scaler shown counts ready messages. If autoscaling should also count
  unacknowledged deliveries, use the RabbitMQ scaler's HTTP management API
  mode and a matching management connection.
- [Kafka consumer lag](kafka.yaml): set `bootstrapServers`, `topic` and
  `consumerGroup` to the worker's broker, topic and group. The default group is
  `<topic>.<worker_type>`; use the configured override if
  `kafka.consumer.group_id` is set. Match `offsetResetPolicy` to the consumer
  and size `maxReplicaCount` for the topic's partitions. This example assumes
  an unauthenticated test broker. Configure the scaler's TLS/SASL settings
  and a Secret-backed authentication reference for protected brokers.

The sample target of 20 is backlog per replica, not a deadline or a job count.
After substituting your deployment and broker details, run
`kubectl apply --dry-run=server -f rabbitmq.yaml` (or `kafka.yaml`) against your
KEDA-enabled cluster, then verify that the backlog for that worker moves
the replica count as expected.

## Scale-to-zero is optional

Both samples keep one replica. Before changing a data worker's minimum to
zero, make sure its RabbitMQ queue is durable, declared and bound before
traffic, or its Kafka topic and consumer offsets are provisioned as intended.
Allow enough termination grace for in-flight work. A backlog of zero does
not mean every job is complete: a message may be in flight, another stage may
still be running, or a retry may be due later.

Keep the **job-status worker** and **retry worker** running independently
with at least one replica each. They observe completion and republish due
retries even while a data worker is at zero.

See the Warren [RabbitMQ guide](../../../warren/docs/rabbitmq.md) and
[Kafka guide](../../../warren/docs/kafka.md), and the KEDA
[RabbitMQ scaler](https://keda.sh/docs/2.18/scalers/rabbitmq-queue/) and
[Kafka scaler](https://keda.sh/docs/2.18/scalers/apache-kafka/).
