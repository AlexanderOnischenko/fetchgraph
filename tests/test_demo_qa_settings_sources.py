from __future__ import annotations

from pathlib import Path

import pytest

from examples.demo_qa.settings import load_settings, resolve_config_path


def write_toml(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def test_source_priorities(tmp_path, monkeypatch):
    config_path = tmp_path / "demo_qa.toml"
    write_toml(
        config_path,
        """
[llm]
api_key = "sk-toml"
plan_model = "toml-plan"
synth_model = "toml-synth"
""",
    )
    monkeypatch.setenv("DEMO_QA_LLM__API_KEY", "sk-env")
    monkeypatch.setenv("DEMO_QA_LLM__PLAN_MODEL", "env-plan")

    settings, resolved = load_settings(config_path=config_path, overrides={"llm": {"plan_model": "override-plan"}})

    assert resolved == config_path
    assert settings.llm.api_key == "sk-env"
    assert settings.llm.plan_model == "override-plan"
    assert settings.llm.synth_model == "toml-synth"


def test_validation_requires_models(tmp_path, monkeypatch):
    config_path = tmp_path / "demo_qa.toml"
    write_toml(
        config_path,
        """
[llm]
api_key = "sk"
plan_model = ""
synth_model = "gpt-4o-mini"
""",
    )

    with pytest.raises(Exception):
        load_settings(config_path=config_path)


def test_base_url_validation(tmp_path):
    config_path = tmp_path / "demo_qa.toml"
    write_toml(
        config_path,
        """
[llm]
api_key = "sk"
base_url = "not-a-url"
""",
    )

    with pytest.raises(Exception):
        load_settings(config_path=config_path)


def test_resolve_config_path_prefers_cli(tmp_path):
    custom = tmp_path / "custom.toml"
    custom.write_text("[llm]\napi_key='sk'\n", encoding="utf-8")
    assert resolve_config_path(custom, None) == custom
