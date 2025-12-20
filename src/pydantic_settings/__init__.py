from __future__ import annotations

import os
import tomllib
from typing import Any, Callable, ClassVar, Dict, Iterable, Mapping

from pydantic import BaseModel


def SettingsConfigDict(**kwargs: Any) -> Dict[str, Any]:
    return dict(**kwargs)


def _deep_update(base: Dict[str, Any], updates: Mapping[str, Any]) -> Dict[str, Any]:
    for key, value in updates.items():
        if isinstance(value, Mapping) and isinstance(base.get(key), dict):
            base[key] = _deep_update(base[key], value)
        else:
            base[key] = value
    return base


class TomlConfigSettingsSource:
    def __init__(self, settings_cls: type[BaseModel], path: os.PathLike | str | None):
        self._path = path

    def __call__(self) -> Dict[str, Any]:
        if not self._path:
            return {}
        try:
            with open(self._path, "rb") as toml_file:
                return tomllib.load(toml_file)
        except FileNotFoundError:
            return {}


class BaseSettings(BaseModel):
    model_config: ClassVar[SettingsConfigDict] = {}

    def __init__(self, **values: Any) -> None:
        sources = self.settings_customise_sources(
            self.__class__,
            self._build_init_settings(values),
            self._build_env_settings(),
            self._build_dotenv_settings(),
            self._build_file_secret_settings(),
        )
        merged: Dict[str, Any] = {}
        for source in reversed(tuple(sources)):
            merged = _deep_update(merged, source() or {})
        super().__init__(**merged)

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseModel],
        init_settings: Callable[[], Mapping[str, Any]],
        env_settings: Callable[[], Mapping[str, Any]],
        dotenv_settings: Callable[[], Mapping[str, Any]],
        file_secret_settings: Callable[[], Mapping[str, Any]],
    ) -> Iterable[Callable[[], Mapping[str, Any]]]:
        return (init_settings, env_settings, dotenv_settings, file_secret_settings)

    @staticmethod
    def _build_init_settings(values: Mapping[str, Any]) -> Callable[[], Mapping[str, Any]]:
        return lambda: dict(values)

    @classmethod
    def _build_env_settings(cls) -> Callable[[], Mapping[str, Any]]:
        prefix = cls.model_config.get("env_prefix", "") or ""
        delimiter = cls.model_config.get("env_nested_delimiter", "__") or "__"

        def source() -> Dict[str, Any]:
            settings: Dict[str, Any] = {}
            for key, value in os.environ.items():
                if not key.startswith(prefix):
                    continue
                raw_key = key[len(prefix) :]
                parts = raw_key.split(delimiter) if delimiter else [raw_key]
                cls._insert_nested(settings, [part.lower() for part in parts], value)
            return settings

        return source

    @classmethod
    def _build_dotenv_settings(cls) -> Callable[[], Mapping[str, Any]]:
        return lambda: {}

    @classmethod
    def _build_file_secret_settings(cls) -> Callable[[], Mapping[str, Any]]:
        return lambda: {}

    @staticmethod
    def _insert_nested(target: Dict[str, Any], parts: list[str], value: Any) -> None:
        current = target
        for part in parts[:-1]:
            current = current.setdefault(part, {})
        current[parts[-1]] = value
