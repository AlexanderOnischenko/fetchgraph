from __future__ import annotations

import os
from pathlib import Path
from typing import Any, ClassVar, Dict
from urllib.parse import urlparse

from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator

try:
    from pydantic_settings.sources import TomlConfigSettingsSource

    from pydantic_settings import BaseSettings, SettingsConfigDict
except ImportError as exc:  # pragma: no cover - make missing dependency explicit
    raise ImportError(
        "pydantic-settings is required for demo_qa configuration. "
        "Install demo extras via `pip install -e .[demo]` or `pip install -r examples/demo_qa/requirements.txt`."
    ) from exc

class LLMSettings(BaseModel):
    base_url: str | None = Field(default=None)
    api_key: str | None = Field(default=None)
    require_api_key: bool = True
    model: str | None = None
    plan_model: str = "default"
    synth_model: str = "default"
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
            raise ValueError("llm.base_url must be a valid URL, e.g. http://localhost:8000/v1")
        return value.rstrip("/")

    @field_validator("model", "plan_model", "synth_model")
    @classmethod
    def validate_model(cls, value: str | None, info: Any) -> str | None:
        if value is None:
            return value
        if str(value).strip() == "":
            raise ValueError(f"{info.field_name} must not be empty")
        return value

    @model_validator(mode="after")
    def propagate_single_model(self) -> "LLMSettings":
        if self.model:
            fields_set = getattr(self, "model_fields_set", set())
            if "plan_model" not in fields_set:
                self.plan_model = self.model
            if "synth_model" not in fields_set:
                self.synth_model = self.model
        if not self.plan_model or not self.synth_model:
            raise ValueError("plan_model and synth_model are required and must not be empty.")
        return self


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
            sources.append(TomlConfigSettingsSource(settings_cls, toml_file=cls._toml_path))
        sources.append(file_secret_settings)
        return tuple(sources)

    @model_validator(mode="after")
    def require_api_key(self) -> "DemoQASettings":
        if not self.llm.api_key:
            env_key = os.getenv("OPENAI_API_KEY")
            if env_key:
                self.llm.api_key = env_key
        if self.llm.require_api_key and not self.llm.api_key:
            raise ValueError("llm.api_key is required. Provide it in config or set OPENAI_API_KEY.")
        return self


def resolve_config_path(config: Path | None, data_dir: Path | None) -> Path | None:
    if config is not None:
        if not config.exists():
            raise FileNotFoundError(f"Config file not found at {config}")
        return config
    if data_dir is not None:
        candidate = data_dir / "demo_qa.toml"
        if candidate.exists():
            return candidate
    root = Path(__file__).resolve().parent
    default = root / "demo_qa.toml"
    if default.exists():
        return default
    example = root / "demo_qa.toml.example"
    if example.exists():
        return example
    return None


def load_settings(
    *,
    config_path: Path | None = None,
    data_dir: Path | None = None,
    overrides: Dict[str, Any] | None = None,
) -> tuple[DemoQASettings, Path | None]:
    resolved = resolve_config_path(config_path, data_dir)
    DemoQASettings._toml_path = resolved
    try:
        settings = DemoQASettings(**(overrides or {}))
    except ValidationError:
        DemoQASettings._toml_path = None
        raise
    DemoQASettings._toml_path = None
    return settings, resolved


__all__ = ["DemoQASettings", "LLMSettings", "resolve_config_path", "load_settings"]
