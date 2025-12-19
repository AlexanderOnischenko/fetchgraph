from __future__ import annotations

import os
import tomllib
from pathlib import Path
from typing import Any, Dict
from urllib.parse import urlparse

from pydantic import BaseModel, ConfigDict, field_validator, model_validator


class OpenAISettings(BaseModel):
    api_key: str | None = None
    base_url: str | None = None
    plan_model: str | None = None
    synth_model: str | None = None
    plan_temperature: float = 0.0
    synth_temperature: float = 0.2
    timeout_s: float | None = None
    retries: int | None = None

    model_config = ConfigDict(extra="ignore")

    @field_validator("base_url")
    @classmethod
    def validate_base_url(cls, value: str | None) -> str | None:
        if value in (None, ""):
            return None
        parsed = urlparse(value)
        if not (parsed.scheme and parsed.netloc):
            raise ValueError("base_url must be a valid URL, e.g. http://localhost:8000/v1")
        return value


class LLMSettings(BaseModel):
    openai: OpenAISettings = OpenAISettings()

    model_config = ConfigDict(extra="ignore")


class DemoQASettings(BaseModel):
    llm: LLMSettings = LLMSettings()

    model_config = ConfigDict(extra="ignore")

    @model_validator(mode="after")
    def require_openai_key(self) -> "DemoQASettings":
        if not self.llm.openai.api_key:
            env_key = os.getenv("OPENAI_API_KEY")
            if env_key:
                self.llm.openai.api_key = env_key
        if not self.llm.openai.base_url:
            env_base = os.getenv("OPENAI_BASE_URL")
            if env_base:
                self.llm.openai.base_url = OpenAISettings.validate_base_url(env_base)
        if not self.llm.openai.api_key:
            raise ValueError("llm.openai.api_key is required. Provide it in config or set OPENAI_API_KEY.")
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


def _deep_update(target: Dict[str, Any], updates: Dict[str, Any]) -> Dict[str, Any]:
    for key, value in updates.items():
        if isinstance(value, dict) and isinstance(target.get(key), dict):
            _deep_update(target[key], value)  # type: ignore[index]
        else:
            target[key] = value
    return target


def _load_toml(path: Path | None) -> Dict[str, Any]:
    if path is None or not path.exists():
        return {}
    with path.open("rb") as f:
        return tomllib.load(f)


def _parse_env_file(path: Path) -> Dict[str, str]:
    if not path.exists():
        return {}
    result: Dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        result[key.strip()] = value.strip()
    return result


def _extract_prefixed(source: Dict[str, str], *, prefix: str = "DEMO_QA_", delimiter: str = "__") -> Dict[str, Any]:
    data: Dict[str, Any] = {}
    for key, value in source.items():
        if not key.startswith(prefix):
            continue
        path = key.removeprefix(prefix).split(delimiter)
        target = data
        for part in path[:-1]:
            target = target.setdefault(part.lower(), {})
        target[path[-1].lower()] = value
    return data


def load_settings(
    *,
    config_path: Path | None = None,
    data_dir: Path | None = None,
    overrides: Dict[str, Any] | None = None,
) -> DemoQASettings:
    resolved = resolve_config_path(config_path, data_dir)
    if config_path is not None and resolved is None:
        raise FileNotFoundError(f"Config file not found at {config_path}")

    merged: Dict[str, Any] = {}
    _deep_update(merged, _load_toml(resolved))
    _deep_update(merged, _extract_prefixed(_parse_env_file(Path(".env.demo_qa"))))
    _deep_update(merged, _extract_prefixed(dict(os.environ)))
    if overrides:
        _deep_update(merged, overrides)

    return DemoQASettings(**merged)


__all__ = [
    "DemoQASettings",
    "LLMSettings",
    "OpenAISettings",
    "resolve_config_path",
    "load_settings",
]
