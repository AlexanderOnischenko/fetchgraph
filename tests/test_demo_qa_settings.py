from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

from examples.demo_qa.llm.factory import build_llm
from examples.demo_qa.llm.openai_adapter import OpenAILLM
from examples.demo_qa.settings import load_settings


def write_toml(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def _install_fake_openai(monkeypatch, created: dict):
    def _store_and_return(kwargs):
        created["chat_kwargs"] = kwargs
        return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content="ok"))])

    class FakeOpenAI:
        def __init__(self, api_key=None, base_url=None, **kwargs):
            created["api_key"] = api_key
            created["base_url"] = base_url
            self.chat = SimpleNamespace(completions=SimpleNamespace(create=lambda **kwargs: _store_and_return(kwargs)))

    monkeypatch.setitem(sys.modules, "openai", SimpleNamespace(OpenAI=FakeOpenAI))


def test_env_overrides_toml(tmp_path, monkeypatch):
    config_path = tmp_path / "demo_qa.toml"
    write_toml(
        config_path,
        """
[llm]
api_key = "sk-from-toml"
base_url = "http://localhost:1234/v1"
plan_model = "toml-plan"
""",
    )
    monkeypatch.setenv("DEMO_QA_LLM__API_KEY", "sk-from-env")
    monkeypatch.setenv("DEMO_QA_LLM__PLAN_MODEL", "env-plan")

    settings, resolved = load_settings(config_path=config_path)
    assert resolved == config_path
    assert settings.llm.api_key == "sk-from-env"
    assert settings.llm.base_url == "http://localhost:1234/v1"
    assert settings.llm.plan_model == "env-plan"




def test_allow_missing_api_key_when_disabled(tmp_path):
    config_path = tmp_path / "demo_qa.toml"
    write_toml(
        config_path,
        """
[llm]
plan_model = "gpt-4o-mini"
synth_model = "gpt-4o-mini"
""",
    )

    settings, resolved = load_settings(config_path=config_path)
    assert resolved == config_path
    assert settings.llm.api_key is None


def test_openai_key_from_global_env(tmp_path, monkeypatch):
    config_path = tmp_path / "demo_qa.toml"
    write_toml(
        config_path,
        """
[llm]
base_url = "http://localhost:1234/v1"
""",
    )
    monkeypatch.setenv("OPENAI_API_KEY", "sk-global")
    created = {}
    _install_fake_openai(monkeypatch, created)

    settings, resolved = load_settings(config_path=config_path)
    assert resolved == config_path
    llm = build_llm(settings)

    llm("hello", sender="generic_plan")
    assert created["api_key"] == "sk-global"


def test_base_url_passed_to_openai_client(tmp_path, monkeypatch):
    config_path = tmp_path / "demo_qa.toml"
    write_toml(
        config_path,
        """
[llm]
api_key = "env:TEST_KEY"
base_url = "http://localhost:1234/v1"
plan_model = "demo-plan"
""",
    )
    monkeypatch.setenv("TEST_KEY", "sk-test")

    created = {}

    _install_fake_openai(monkeypatch, created)

    settings, resolved = load_settings(config_path=config_path)
    assert resolved == config_path
    llm = build_llm(settings)

    result = llm("hello", sender="generic_plan")
    assert result == "ok"
    assert created["base_url"] == "http://localhost:1234/v1"
    assert created["api_key"] == "sk-test"
    assert created["chat_kwargs"] == {
        "model": "demo-plan",
        "messages": [{"role": "user", "content": "hello"}],
        "temperature": 0.0,
    }


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

    llm = OpenAILLM(
        api_key="sk",
        base_url=None,
        plan_model="demo-plan",
        synth_model="demo-synth",
        timeout_s=12.5,
        retries=3,
    )
    llm("question", sender="generic_plan")

    assert created["with_options"] == {"timeout": 12.5, "max_retries": 3}
    assert created["create_kwargs"] == {
        "model": "demo-plan",
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

    llm = OpenAILLM(
        api_key="sk",
        base_url=None,
        plan_model="demo-plan",
        synth_model="demo-synth",
        timeout_s=None,
        retries=None,
    )
    llm("question", sender="generic_synth")

    assert "with_options_called" not in created
    assert created["create_kwargs"] == {
        "model": "demo-synth",
        "messages": [{"role": "user", "content": "question"}],
        "temperature": 0.2,
    }


def test_missing_api_key_uses_unused(monkeypatch):
    created: dict = {}
    _install_fake_openai(monkeypatch, created)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)

    llm = OpenAILLM(
        api_key=None,
        base_url=None,
        plan_model="demo-plan",
        synth_model="demo-synth",
    )
    llm("hello", sender="generic_plan")

    assert created["api_key"] == "unused"


def test_env_reference_uses_openai_api_key(monkeypatch):
    created: dict = {}
    _install_fake_openai(monkeypatch, created)
    monkeypatch.setenv("OPENAI_API_KEY", "sk-env")

    llm = OpenAILLM(
        api_key="env:OPENAI_API_KEY",
        base_url=None,
        plan_model="demo-plan",
        synth_model="demo-synth",
    )
    llm("hello", sender="generic_plan")

    assert created["api_key"] == "sk-env"


def test_env_reference_defaults_to_unused_when_missing(monkeypatch):
    created: dict = {}
    _install_fake_openai(monkeypatch, created)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)

    llm = OpenAILLM(
        api_key="env:OPENAI_API_KEY",
        base_url=None,
        plan_model="demo-plan",
        synth_model="demo-synth",
    )
    llm("hello", sender="generic_plan")

    assert created["api_key"] == "unused"
