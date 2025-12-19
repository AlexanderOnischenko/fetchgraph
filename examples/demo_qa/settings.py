from __future__ import annotations

import os
from pathlib import Path
from typing import Any, ClassVar, Dict, Literal
from urllib.parse import urlparse

from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict, TomlConfigSettingsSource


class OpenAISettings(BaseModel):
    api_key: str | None = Field(default=None, validation_alias="OPENAI_API_KEY")
    base_url: str | None = Field(default=None, validation_alias="OPENAI_BASE_URL")
    plan_model: str = "gpt-4o-mini"
    synth_model: str = "gpt-4o-mini"
    plan_temperature: float = 0.0
    synth_temperature: float = 0.2
    timeout_s: float | None = None
    retries: int | None = None

    @field_validator("base_url")
    @classmethod
    def validate_base_url(cls, value: str | None) -> str | None:
        if value in (None, ""):
            return None
        parsed = urlparse(value)
        if not (parsed.scheme and parsed.netloc):
            raise ValueError("base_url must be a valid URL, e.g. http://localhost:8000/v1")
        return value


class MockSettings(BaseModel):
    plan_fixture: Path | None = None
    synth_template: str = "Mock synthesis for: {question}"


class LLMSettings(BaseModel):
    provider: Literal["mock", "openai"] = "mock"
    openai: OpenAISettings = OpenAISettings()
    mock: MockSettings = MockSettings()


class DemoQASettings(BaseSettings):
    llm: LLMSettings = LLMSettings()

    _toml_path: ClassVar[Path | None] = None

    model_config = SettingsConfigDict(
        env_prefix="DEMO_QA_",
        env_nested_delimiter="__",
        env_file=".env.demo_qa",
        extra="ignore",
    )

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls,
        init_settings,
        env_settings,
        dotenv_settings,
        file_secret_settings,
    ):
        sources = [init_settings, env_settings, dotenv_settings]
        if cls._toml_path:
            sources.append(TomlConfigSettingsSource(settings_cls, cls._toml_path))
        sources.append(file_secret_settings)
        return tuple(sources)

    @model_validator(mode="after")
    def require_openai_key(self) -> "DemoQASettings":
        if self.llm.provider == "openai" and not self.llm.openai.api_key:
            env_key = os.getenv("OPENAI_API_KEY")
            if env_key:
                self.llm.openai.api_key = env_key
            else:
                raise ValueError("OpenAI provider selected but no api_key provided.")
        return self


def resolve_config_path(config: Path | None, data_dir: Path | None) -> Path | None:
    if config is not None:
        return config
    search: list[Path] = []
    if data_dir is not None:
        search.append(data_dir / "demo_qa.toml")
    search.append(Path(__file__).resolve().parent / "demo_qa.toml")
    for candidate in search:
        if candidate.exists():
            return candidate
    return None


def load_settings(
    *,
    config_path: Path | None = None,
    data_dir: Path | None = None,
    overrides: Dict[str, Any] | None = None,
) -> DemoQASettings:
    resolved = resolve_config_path(config_path, data_dir)
    if config_path is not None and resolved is None:
        raise FileNotFoundError(f"Config file not found at {config_path}")

    DemoQASettings._toml_path = resolved
    try:
        settings = DemoQASettings(**(overrides or {}))
    except ValidationError as exc:
        DemoQASettings._toml_path = None
        raise
    DemoQASettings._toml_path = None

    if settings.llm.mock.plan_fixture and not settings.llm.mock.plan_fixture.is_absolute() and resolved:
        settings.llm.mock.plan_fixture = (resolved.parent / settings.llm.mock.plan_fixture).resolve()

    return settings


__all__ = [
    "DemoQASettings",
    "LLMSettings",
    "OpenAISettings",
    "MockSettings",
    "resolve_config_path",
    "load_settings",
]
