from __future__ import annotations

import json
import sys
from types import SimpleNamespace
from pathlib import Path

import pytest

from examples.demo_qa.llm.factory import build_llm
from examples.demo_qa.llm.openai_adapter import OpenAILLM
from examples.demo_qa.settings import DemoQASettings, load_settings


def write_toml(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def test_env_overrides_toml(tmp_path, monkeypatch):
    config_path = tmp_path / "demo_qa.toml"
    write_toml(
        config_path,
        """
[llm]
provider = "mock"
[llm.openai]
api_key = "sk-from-toml"
""",
    )
    monkeypatch.setenv("DEMO_QA_LLM__PROVIDER", "openai")
    monkeypatch.setenv("DEMO_QA_LLM__OPENAI__API_KEY", "sk-from-env")

    settings = load_settings(config_path=config_path)
    assert settings.llm.provider == "openai"
    assert settings.llm.openai.api_key == "sk-from-env"


def test_openai_requires_api_key(tmp_path):
    config_path = tmp_path / "demo_qa.toml"
    write_toml(
        config_path,
        """
[llm]
provider = "mock"
""",
    )

    with pytest.raises(ValueError):
        load_settings(config_path=config_path, overrides={"llm": {"provider": "openai"}})


def test_base_url_passed_to_openai_client(tmp_path, monkeypatch):
    config_path = tmp_path / "demo_qa.toml"
    plan_fixture = tmp_path / "plan_fixture.json"
    plan_fixture.write_text(json.dumps({"default": "{}"}), encoding="utf-8")
    write_toml(
        config_path,
        f"""
[llm]
provider = "openai"
[llm.openai]
api_key = "env:TEST_KEY"
base_url = "http://localhost:1234/v1"
""",
    )
    monkeypatch.setenv("TEST_KEY", "sk-test")

    created = {}

    class FakeOpenAI:
        def __init__(self, api_key=None, base_url=None, **kwargs):
            created["api_key"] = api_key
            created["base_url"] = base_url

        class chat:
            class completions:
                @staticmethod
                def create(**kwargs):
                    created["create_kwargs"] = kwargs
                    return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content="ok"))])

    monkeypatch.setitem(sys.modules, "openai", SimpleNamespace(OpenAI=FakeOpenAI))

    settings = load_settings(config_path=config_path)
    llm = build_llm(settings)

    result = llm("hello", sender="generic_plan")
    assert result == "ok"
    assert created["base_url"] == "http://localhost:1234/v1"
    assert created["api_key"] == "sk-test"
    assert created["create_kwargs"] == {"model": "gpt-4o-mini", "messages": [{"role": "user", "content": "hello"}], "temperature": 0.0}


def test_timeout_and_retries_use_with_options(monkeypatch):
    created: dict = {}

    class FakeCompletion:
        @staticmethod
        def create(**kwargs):
            created["create_kwargs"] = kwargs
            return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content="ok"))])

    class FakeChat:
        completions = FakeCompletion()

    class FakeOpenAIClient:
        def __init__(self, api_key=None, base_url=None, **kwargs):
            created["init"] = {"api_key": api_key, "base_url": base_url, **kwargs}
            self.chat = FakeChat()

        def with_options(self, **kwargs):
            created["with_options"] = kwargs
            return self

    monkeypatch.setitem(sys.modules, "openai", SimpleNamespace(OpenAI=FakeOpenAIClient))

    llm = OpenAILLM(api_key="sk", base_url=None, timeout_s=12.5, retries=3)
    llm("question", sender="generic_plan")

    assert created["with_options"] == {"timeout": 12.5, "max_retries": 3}
    assert created["create_kwargs"] == {
        "model": "gpt-4o-mini",
        "messages": [{"role": "user", "content": "question"}],
        "temperature": 0.0,
    }


def test_no_options_uses_base_client(monkeypatch):
    created: dict = {}

    class FakeCompletion:
        @staticmethod
        def create(**kwargs):
            created["create_kwargs"] = kwargs
            return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content="ok"))])

    class FakeChat:
        completions = FakeCompletion()

    class FakeOpenAIClient:
        def __init__(self, api_key=None, base_url=None, **kwargs):
            created["init"] = {"api_key": api_key, "base_url": base_url, **kwargs}
            self.chat = FakeChat()

        def with_options(self, **kwargs):
            created["with_options_called"] = True
            return self

    monkeypatch.setitem(sys.modules, "openai", SimpleNamespace(OpenAI=FakeOpenAIClient))

    llm = OpenAILLM(api_key="sk", base_url=None, timeout_s=None, retries=None)
    llm("question", sender="generic_synth")

    assert "with_options_called" not in created
    assert created["create_kwargs"] == {
        "model": "gpt-4o-mini",
        "messages": [{"role": "user", "content": "question"}],
        "temperature": 0.2,
    }
