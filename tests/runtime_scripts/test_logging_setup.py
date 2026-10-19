"""Runtime script logging: text, JSON lines, or a host's own dictConfig."""

import json
import logging

import pytest

from runtime_scripts.lib.logging_setup import JsonFormatter, configure_logging


@pytest.fixture(autouse=True)
def restored_root():
    root = logging.getLogger()
    handlers, level = root.handlers[:], root.level
    yield
    root.handlers[:] = handlers
    root.setLevel(level)


def test_json_lines_carry_extra_fields_and_the_error(monkeypatch, capsys):
    monkeypatch.setenv("WARREN_LOG_FORMAT", "json")
    configure_logging(debug=False)
    logger = logging.getLogger("warren.test")
    logger.setLevel(logging.DEBUG)

    logger.debug("not shown")
    logger.info("doc %s done", "d-1", extra={"job_id": "j-1"})
    error = "boom"
    try:
        raise RuntimeError(error)
    except RuntimeError:
        logger.exception("failed")

    lines = [json.loads(line) for line in capsys.readouterr().err.splitlines()]
    assert [line["message"] for line in lines] == ["doc d-1 done", "failed"]
    assert lines[0]["job_id"] == "j-1"
    assert lines[0]["level"] == "INFO"
    assert lines[1]["error"]["type"] == "RuntimeError"


def test_a_host_dictconfig_replaces_the_setup_and_keeps_existing_loggers(
    monkeypatch, tmp_path, capsys
):
    existing = logging.getLogger("warren.existing")
    config = tmp_path / "logging.yaml"
    config.write_text(
        "version: 1\n"
        "formatters: {plain: {format: 'HOST %(name)s %(message)s'}}\n"
        "handlers: {err: {class: logging.StreamHandler, stream: ext://sys.stderr,"
        " formatter: plain}}\n"
        "root: {level: INFO, handlers: [err]}\n"
    )
    monkeypatch.setenv("WARREN_LOG_CONFIG", str(config))

    configure_logging(debug=True)
    existing.info("still here")

    assert capsys.readouterr().err.strip() == "HOST warren.existing still here"


def test_an_unknown_format_is_refused(monkeypatch):
    monkeypatch.setenv("WARREN_LOG_FORMAT", "xml")

    with pytest.raises(ValueError, match="WARREN_LOG_FORMAT"):
        configure_logging(debug=False)


def test_the_formatter_leaves_out_standard_record_attributes():
    record = logging.LogRecord("warren", logging.INFO, __file__, 1, "hi", (), None)

    fields = set(json.loads(JsonFormatter().format(record)))
    assert fields == {"ts", "level", "logger", "message"}
