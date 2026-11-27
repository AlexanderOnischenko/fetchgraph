"""Offline integration of Fetchgraph JSON parsers with llm-output-repair."""
import json
from pathlib import Path
import pytest
from llm_output_repair.regression import load_case, replay_case
from fetchgraph.core.models import RawLLMOutput
from fetchgraph.parsing import JsonParser, PlanParser, OutputParserException
from fetchgraph.parsing.repair_runtime import ENV_DUMP_DIR, recorder_for

def raw(text):
    return RawLLMOutput(text=text)

def test_no_default_capture(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv(ENV_DUMP_DIR, raising=False)
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
    monkeypatch.setenv(ENV_DUMP_DIR, "relative/dumps")
    with pytest.raises(ValueError, match="absolute"):
        recorder_for("plan")

def test_no_recorder_without_opt_in(monkeypatch):
    monkeypatch.delenv(ENV_DUMP_DIR, raising=False)
    assert recorder_for("plan") is None
