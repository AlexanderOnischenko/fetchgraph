"""Offline integration of Fetchgraph JSON parsers with llm-output-repair."""
import json
from pathlib import Path
import pytest
from llm_output_repair.regression import load_case, replay_case
from fetchgraph.core.models import RawLLMOutput
from fetchgraph.parsing import JsonParser, PlanParser, OutputParserException
from fetchgraph.parsing.repair_runtime import ENV_CAPTURE, ENV_DUMP_DIR, recorder_for
from fetchgraph.retrieval.engine import RetrievalEngine

def raw(text):
    return RawLLMOutput(text=text)

def test_no_default_capture(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv(ENV_DUMP_DIR, raising=False)
    monkeypatch.delenv(ENV_CAPTURE, raising=False)
    with pytest.raises(OutputParserException):
        PlanParser().parse(raw('{"context_plan":'))
    assert list(tmp_path.iterdir()) == []

def test_plan_parser_repairs_missing_comma():
    plan = PlanParser().parse(raw('{"required_context":["shop"] "context_plan":[{"provider":"shop"}]}'))
    assert plan.required_context == ["shop"]
    assert plan.context_plan[0].provider == "shop"

def test_domain_validation_still_applies():
    with pytest.raises(OutputParserException, match="должен быть списком"):
        PlanParser().parse(raw('{"required_context": 5}'))

def test_generic_parser_preserves_object_and_array_compatibility():
    assert JsonParser().parse(raw('{"x":1}')) == {"x": 1}
    assert JsonParser().parse(raw('[1,2,3]')) == [1,2,3]
    assert JsonParser(key="x").parse(raw('{"x":1,"y":2}')) == {"x": 1}

def test_safe_policy_rejects_ambiguity():
    with pytest.raises(OutputParserException):
        JsonParser(policy="safe").parse(raw('{"x":1} {"x":2}'))

def test_opt_in_dump_replays(tmp_path, monkeypatch):
    folder = tmp_path / "dumps"
    monkeypatch.setenv(ENV_CAPTURE, "failed")
    monkeypatch.setenv(ENV_DUMP_DIR, str(folder))
    original = '  \n{"context_plan": '
    with pytest.raises(OutputParserException):
        PlanParser().parse(raw(original))
    files = list(folder.glob("*.json"))
    assert len(files) == 1
    case = load_case(files[0])
    assert case["input"]["raw"] == original
    assert case["operation"] == "parse_json"
    assert case["input"]["options"]["policy"] == "safe"
    assert case["source"]["project"] == "fetchgraph"
    assert replay_case(case).status == "rejected"
    with pytest.raises(OutputParserException):
        PlanParser().parse(raw(original))
    assert len(list(folder.glob("*.json"))) == 1

def test_relative_dump_directory_is_rejected(monkeypatch):
    monkeypatch.setenv(ENV_CAPTURE, "failed")
    monkeypatch.setenv(ENV_DUMP_DIR, "relative/dumps")
    with pytest.raises(ValueError, match="absolute"):
        recorder_for("plan")

def test_no_recorder_without_opt_in(monkeypatch):
    monkeypatch.delenv(ENV_DUMP_DIR, raising=False)
    monkeypatch.delenv(ENV_CAPTURE, raising=False)
    assert recorder_for("plan") is None


def test_stale_directory_does_not_enable_capture(monkeypatch, tmp_path):
    folder = tmp_path / "stale"
    monkeypatch.setenv(ENV_DUMP_DIR, str(folder))
    monkeypatch.delenv(ENV_CAPTURE, raising=False)
    with pytest.raises(OutputParserException):
        PlanParser().parse(raw('{"context_plan":'))
    assert not folder.exists()
    monkeypatch.setenv(ENV_CAPTURE, "off")
    with pytest.raises(OutputParserException):
        PlanParser().parse(raw('{"context_plan":'))
    assert not folder.exists()


@pytest.mark.parametrize("mode", ["", "on", "all", "FAILED", "true"])
def test_invalid_capture_mode_fails_early(monkeypatch, tmp_path, mode):
    folder = tmp_path / "dumps"
    monkeypatch.setenv(ENV_CAPTURE, mode)
    monkeypatch.setenv(ENV_DUMP_DIR, str(folder))
    with pytest.raises(ValueError, match=ENV_CAPTURE):
        PlanParser()
    with pytest.raises(ValueError, match=ENV_CAPTURE):
        RetrievalEngine({})
    assert not folder.exists()


def test_failed_without_directory_fails_at_initialization(monkeypatch):
    monkeypatch.setenv(ENV_CAPTURE, "failed")
    monkeypatch.delenv(ENV_DUMP_DIR, raising=False)
    with pytest.raises(ValueError, match=ENV_DUMP_DIR):
        PlanParser()
    with pytest.raises(ValueError, match=ENV_DUMP_DIR):
        RetrievalEngine({})
    monkeypatch.setenv(ENV_DUMP_DIR, "")
    with pytest.raises(ValueError, match=ENV_DUMP_DIR):
        PlanParser()


def test_failed_relative_directory_fails_at_initialization(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv(ENV_CAPTURE, "failed")
    monkeypatch.setenv(ENV_DUMP_DIR, "relative/path")
    with pytest.raises(ValueError, match="absolute"):
        PlanParser()
    assert not (tmp_path / "relative").exists()


def test_active_parser_does_not_re_read_environment(monkeypatch, tmp_path):
    monkeypatch.delenv(ENV_CAPTURE, raising=False)
    monkeypatch.delenv(ENV_DUMP_DIR, raising=False)
    disabled = PlanParser()
    folder = tmp_path / "dumps"
    monkeypatch.setenv(ENV_CAPTURE, "failed")
    monkeypatch.setenv(ENV_DUMP_DIR, str(folder))
    # Changing environment cannot enable an existing parser.
    with pytest.raises(OutputParserException):
        disabled.parse(raw('{"context_plan":'))
    assert not folder.exists()
    enabled = PlanParser()
    monkeypatch.setenv(ENV_CAPTURE, "off")
    # Changing environment cannot disable an already-created recorder.
    with pytest.raises(OutputParserException):
        enabled.parse(raw('  {"context_plan": '))
    assert len(list(folder.glob("*.json"))) == 1


def test_retrieval_engine_snapshots_config_before_inference(monkeypatch, tmp_path):
    monkeypatch.setenv(ENV_CAPTURE, "off")
    folder = tmp_path / "dumps"
    monkeypatch.setenv(ENV_DUMP_DIR, str(folder))
    disabled = RetrievalEngine({})
    assert disabled._plan_recorder is None
    assert disabled._answer_recorder is None
    assert not folder.exists()
    monkeypatch.setenv(ENV_CAPTURE, "failed")
    enabled = RetrievalEngine({})
    assert enabled._plan_recorder is not None
    assert enabled._answer_recorder is not None
    assert folder.is_dir()
    monkeypatch.setenv(ENV_CAPTURE, "off")
    assert enabled._plan_recorder is not None
    assert enabled._answer_recorder is not None


def test_configuration_emits_operational_status(monkeypatch, tmp_path, caplog):
    import logging

    caplog.set_level(logging.INFO, logger="fetchgraph.parsing.repair_runtime")
    monkeypatch.setenv(ENV_CAPTURE, "off")
    recorder_for("plan")
    assert "LLM repair crash capture: OFF" in caplog.text
    caplog.clear()

    monkeypatch.setenv(ENV_CAPTURE, "failed")
    folder = tmp_path / "dumps"
    monkeypatch.setenv(ENV_DUMP_DIR, str(folder))
    recorder_for("plan")
    assert "LLM repair crash capture: FAILED storage=" in caplog.text
    assert str(folder) in caplog.text


def test_explicit_recorder_injection_remains_an_opt_in(monkeypatch, tmp_path):
    from llm_output_repair import CrashRecorder

    monkeypatch.setenv(ENV_CAPTURE, "off")
    folder = tmp_path / "explicit"
    parser = PlanParser(recorder=CrashRecorder(folder))
    with pytest.raises(OutputParserException):
        parser.parse(raw('{"context_plan":'))
    assert len(list(folder.glob("*.json"))) == 1
