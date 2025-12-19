from __future__ import annotations

import os
import tomllib
from pathlib import Path
from typing import Any, Dict, Iterable, Tuple

from pydantic import BaseModel


class SettingsConfigDict(dict):
    """Fallback stand-in for pydantic-settings config."""


def _extract_prefixed(source: Dict[str, str], *, prefix: str = "", delimiter: str = "__") -> Dict[str, Any]:
    data: Dict[str, Any] = {}
    for key, value in source.items():
        if prefix and not key.startswith(prefix):
            continue
        stripped = key[len(prefix) :] if prefix else key
        path = stripped.split(delimiter)
        target = data
        for part in path[:-1]:
            target = target.setdefault(part.lower(), {})
        target[path[-1].lower()] = value
    return data


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


class InitSettingsSource:
    def __init__(self, values: Dict[str, Any]):
        self.values = values

    def __call__(self) -> Dict[str, Any]:
        return dict(self.values)


class EnvSettingsSource:
    def __init__(self, config: SettingsConfigDict):
        self.env_prefix = config.get("env_prefix", "") or ""
        self.delimiter = config.get("env_nested_delimiter", "__") or "__"

    def __call__(self) -> Dict[str, Any]:
        return _extract_prefixed(dict(os.environ), prefix=self.env_prefix, delimiter=self.delimiter)


class DotEnvSettingsSource:
    def __init__(self, config: SettingsConfigDict):
        env_file = config.get("env_file")
        self.path = Path(env_file) if env_file else None
        self.env_prefix = config.get("env_prefix", "") or ""
        self.delimiter = config.get("env_nested_delimiter", "__") or "__"

    def __call__(self) -> Dict[str, Any]:
        if not self.path:
            return {}
        return _extract_prefixed(_parse_env_file(self.path), prefix=self.env_prefix, delimiter=self.delimiter)


class SecretsSettingsSource:
    def __call__(self) -> Dict[str, Any]:
        return {}


class TomlConfigSettingsSource:
    def __init__(self, settings_cls: type[BaseModel], path: Path):
        self.path = Path(path)

    def __call__(self) -> Dict[str, Any]:
        if not self.path.exists():
            return {}
        with self.path.open("rb") as f:
            return tomllib.load(f)


def _deep_update(target: Dict[str, Any], updates: Dict[str, Any]) -> Dict[str, Any]:
    for key, value in updates.items():
        if isinstance(value, dict) and isinstance(target.get(key), dict):
            _deep_update(target[key], value)  # type: ignore[index]
        else:
            target[key] = value
    return target


class BaseSettings(BaseModel):
    model_config: SettingsConfigDict = SettingsConfigDict()

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls,
        init_settings: InitSettingsSource,
        env_settings: EnvSettingsSource,
        dotenv_settings: DotEnvSettingsSource,
        file_secret_settings: SecretsSettingsSource,
    ) -> Tuple[Any, ...]:
        return init_settings, env_settings, dotenv_settings, file_secret_settings

    def __init__(self, **values: Any):
        config: SettingsConfigDict = getattr(self.__class__, "model_config", SettingsConfigDict())
        init_source = InitSettingsSource(values)
        env_source = EnvSettingsSource(config)
        dotenv_source = DotEnvSettingsSource(config)
        secrets_source = SecretsSettingsSource()

        sources: Iterable[Any] = self.settings_customise_sources(
            self.__class__, init_source, env_source, dotenv_source, secrets_source
        )

        merged: Dict[str, Any] = {}
        for source in reversed(tuple(sources)):
            _deep_update(merged, source())

        super().__init__(**merged)


__all__ = [
    "BaseSettings",
    "SettingsConfigDict",
    "TomlConfigSettingsSource",
]
