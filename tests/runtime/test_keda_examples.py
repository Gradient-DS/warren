from pathlib import Path

import yaml

from examples.exchanges.fanout.pipeline_spec import PIPELINE


EXAMPLES = Path(__file__).resolve().parents[2] / "examples"
KEDA = EXAMPLES / "deploy" / "keda"
WORKER = "document_parser"


def test_rabbitmq_trigger_matches_the_fanout_worker() -> None:
    auth, scaled = yaml.safe_load_all((KEDA / "rabbitmq.yaml").read_text())
    assert WORKER in PIPELINE.workers
    assert auth["kind"] == "TriggerAuthentication"
    assert scaled["kind"] == "ScaledObject"
    trigger = scaled["spec"]["triggers"][0]
    assert trigger["type"] == "rabbitmq"
    assert trigger["authenticationRef"]["name"] == auth["metadata"]["name"]
    assert any(ref["parameter"] == "host" for ref in auth["spec"]["secretTargetRef"])
    assert trigger["metadata"]["queueName"] == f"{PIPELINE.exchange.name}.{WORKER}"
    assert trigger["metadata"]["mode"] == "QueueLength"


def test_kafka_trigger_matches_fanout_topic_and_group() -> None:
    config = yaml.safe_load((EXAMPLES / "exchanges" / "fanout" / "config.kafka.yaml").read_text())
    scaled = yaml.safe_load((KEDA / "kafka.yaml").read_text())
    assert WORKER in PIPELINE.workers
    assert scaled["kind"] == "ScaledObject"
    trigger = scaled["spec"]["triggers"][0]
    assert trigger["type"] == "kafka"
    topic = config["kafka"]["topic"]["name"]
    assert trigger["metadata"]["topic"] == topic
    assert trigger["metadata"]["consumerGroup"] == f"{topic}.{WORKER}"
    assert trigger["metadata"]["offsetResetPolicy"] == config["kafka"]["consumer"]["auto_offset_reset"]
