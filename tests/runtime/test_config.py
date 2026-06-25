"""Unit tests for ``RuntimeConfig`` backend selection and YAML round-trip.

No broker, no transport library — these tests only exercise the pure-data
config models, asserting that:

- a legacy RMQ YAML (no ``backend:`` key) still parses and defaults to
  ``backend == "rabbitmq"`` — full backward compatibility,
- a ``backend: kafka`` YAML parses, selects the Kafka backend, and
  populates the kafka section,
- an unknown backend is rejected by the ``Literal`` constraint.
"""

from pathlib import Path

import pytest
from pydantic import ValidationError

from warren.runtime.config import RuntimeConfig


def _write_yaml(tmp_path: Path, body: str) -> Path:
    path = tmp_path / "config.yaml"
    path.write_text(body)
    return path


def test_legacy_rmq_yaml_defaults_to_rabbitmq(tmp_path: Path) -> None:
    # A pre-existing YAML with no ``backend:`` key — must stay valid.
    path = _write_yaml(
        tmp_path,
        """
rabbitmq:
  connection:
    host: rabbit.example
    port: 5672
  exchange:
    name: jobs
    type: fanout
mongodb:
  database: e2e_test
""",
    )

    config = RuntimeConfig.from_yaml(path)

    assert config.backend == "rabbitmq"
    assert config.rabbitmq.connection.host == "rabbit.example"
    assert config.rabbitmq.exchange.name == "jobs"
    # Kafka section still gets its defaults even when unused.
    assert config.kafka.topic.name == "jobs"


def test_kafka_yaml_selects_kafka_backend(tmp_path: Path) -> None:
    path = _write_yaml(
        tmp_path,
        """
backend: kafka
kafka:
  connection:
    bootstrap_servers:
      - broker.example:9092
  topic:
    name: jobs
    create_if_missing: true
  consumer:
    auto_offset_reset: earliest
mongodb:
  database: e2e_test
""",
    )

    config = RuntimeConfig.from_yaml(path)

    assert config.backend == "kafka"
    assert config.kafka.connection.bootstrap_servers == ["broker.example:9092"]
    assert config.kafka.topic.name == "jobs"
    assert config.kafka.topic.create_if_missing is True
    assert config.kafka.consumer.auto_offset_reset == "earliest"


def test_explicit_rabbitmq_backend_is_accepted(tmp_path: Path) -> None:
    path = _write_yaml(
        tmp_path,
        """
backend: rabbitmq
mongodb:
  database: e2e_test
""",
    )

    config = RuntimeConfig.from_yaml(path)

    assert config.backend == "rabbitmq"


def test_unknown_backend_is_rejected() -> None:
    with pytest.raises(ValidationError):
        RuntimeConfig(backend="redis-streams")  # type: ignore[arg-type]


def test_empty_config_defaults_to_rabbitmq() -> None:
    # model_validate(None) path: an empty YAML doc -> all defaults.
    config = RuntimeConfig()

    assert config.backend == "rabbitmq"
    assert config.rabbitmq.exchange.name == "jobs"
    assert config.kafka.topic.name == "jobs"
