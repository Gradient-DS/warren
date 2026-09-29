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
import yaml
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
mongodb:
  database: e2e_test
""",
    )

    config = RuntimeConfig.from_yaml(path)

    assert config.backend == "rabbitmq"
    assert config.rabbitmq.connection.host == "rabbit.example"
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
    # The exchange is pipeline topology and lives in the PipelineSpec,
    # not here — config carries only per-environment infrastructure.
    assert config.rabbitmq.connection.host == "localhost"
    assert config.kafka.topic.name == "jobs"


def test_retry_policy_from_yaml(tmp_path: Path) -> None:
    path = _write_yaml(
        tmp_path,
        """
retry:
  enabled: true
  policy:
    max_delay_cap: 900
""",
    )

    config = RuntimeConfig.from_yaml(path)

    assert config.retry.policy.max_delay_cap == 900
    assert config.retry.policy.max_retries == 5  # untouched defaults survive


def test_health_from_yaml(tmp_path: Path) -> None:
    path = _write_yaml(
        tmp_path,
        """
health:
  port: 9090
  consumer_lost_grace_s: 30
""",
    )

    config = RuntimeConfig.from_yaml(path)

    assert config.health.enabled is True
    assert config.health.port == 9090
    assert config.health.consumer_lost_grace_s == 30.0


def test_memory_backend_is_a_valid_choice() -> None:
    assert RuntimeConfig.model_validate({"backend": "memory"}).backend == "memory"


@pytest.mark.parametrize(
    "section",
    [
        (),
        ("rabbitmq",),
        ("rabbitmq", "connection"),
        ("rabbitmq", "consumer"),
        ("kafka",),
        ("kafka", "connection"),
        ("kafka", "topic"),
        ("kafka", "consumer"),
        ("mongodb",),
        ("redis",),
        ("documents",),
        ("results",),
        ("retention",),
        ("retry",),
        ("retry", "policy"),
        ("health",),
        ("startup",),
    ],
)
@pytest.mark.parametrize("from_yaml", [False, True])
def test_unknown_config_keys_are_rejected(
    tmp_path: Path, section: tuple[str, ...], *, from_yaml: bool
) -> None:
    data = RuntimeConfig().model_dump(mode="json")
    target = data
    for key in section:
        target = target[key]
    target["hosst"] = "localhost"

    if from_yaml:
        load = RuntimeConfig.from_yaml
        value = _write_yaml(tmp_path, yaml.safe_dump(data))
    else:
        load = RuntimeConfig.model_validate
        value = data
    with pytest.raises(ValidationError) as exc:
        load(value)

    assert exc.value.errors()[0]["type"] == "extra_forbidden"
    assert exc.value.errors()[0]["loc"] == (*section, "hosst")


@pytest.mark.parametrize(
    "path",
    sorted(
        path
        for directory in ("examples", "runtime_scripts", "tests")
        for path in (Path(__file__).resolve().parents[2] / directory).rglob("*")
        if path.suffix in {".yaml", ".yml"}
    ),
)
def test_repository_yaml_configs(path: Path) -> None:
    RuntimeConfig.from_yaml(path)


def test_yaml_expands_environment_in_string_values(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("WARREN_TEST_HOST", "broker.example")
    monkeypatch.setenv("WARREN_TEST_PASSWORD", "value: # ${NOT_EXPANDED}")
    path = _write_yaml(
        tmp_path,
        """
kafka:
  connection:
    bootstrap_servers: ["${WARREN_TEST_HOST}:9092"]
mongodb:
  password: ${WARREN_TEST_PASSWORD}
redis:
  url: redis://${WARREN_TEST_HOST}:6379/2
  db: 2
""",
    )
    config = RuntimeConfig.from_yaml(path)
    assert config.kafka.connection.bootstrap_servers == ["broker.example:9092"]
    assert config.mongodb.password.get_secret_value() == "value: # ${NOT_EXPANDED}"
    assert config.redis.url.get_secret_value() == "redis://broker.example:6379/2"
    assert config.redis.db == 2


def test_yaml_rejects_unset_environment_variable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("WARREN_TEST_MISSING", raising=False)
    path = _write_yaml(tmp_path, "mongodb:\n  password: ${WARREN_TEST_MISSING}\n")
    with pytest.raises(ValueError, match=r"WARREN_TEST_MISSING.*not set"):
        RuntimeConfig.from_yaml(path)


@pytest.mark.parametrize(
    "settings",
    [
        {"attempts": 0},
        {"attempts": -1},
        {"initial_delay_seconds": -1},
        {"max_delay_seconds": -1},
        {"initial_delay_seconds": float("inf")},
        {"max_delay_seconds": float("nan")},
    ],
)
def test_invalid_startup_settings(settings: dict[str, int | float]) -> None:
    with pytest.raises(ValidationError):
        RuntimeConfig.model_validate({"startup": settings})


def test_retention_defaults_to_disabled() -> None:
    config = RuntimeConfig()
    assert config.retention.job_records_ttl_seconds is None
    assert config.retention.job_records_max_age_seconds is None


@pytest.mark.parametrize(
    ("ttl", "max_age"),
    [(None, None), (None, 60), (0, 0), (60, None), (60, 60), (60, 90)],
)
def test_retention_from_yaml(
    tmp_path: Path, ttl: int | None, max_age: int | None
) -> None:
    settings = {"job_records_ttl_seconds": ttl, "job_records_max_age_seconds": max_age}
    path = _write_yaml(tmp_path, yaml.safe_dump({"retention": settings}))
    assert RuntimeConfig.from_yaml(path).retention.model_dump() == settings


@pytest.mark.parametrize(
    "settings",
    [
        {"job_records_ttl_seconds": -1},
        {"job_records_max_age_seconds": -1},
        {"job_records_ttl_seconds": 60, "job_records_max_age_seconds": 59},
    ],
)
def test_invalid_retention_settings(settings: dict[str, int]) -> None:
    with pytest.raises(ValidationError):
        RuntimeConfig.model_validate({"retention": settings})
