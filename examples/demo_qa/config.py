from __future__ import annotations

import json
import os
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, MutableMapping, Tuple

DEFAULT_CONFIG: Dict[str, Any] = {
    "llm": {
        "provider": "mock",
        "openai": {
            "api_key": None,
            "base_url": None,
            "plan_model": "gpt-4o-mini",
            "synth_model": "gpt-4o-mini",
            "plan_temperature": 0.0,
            "synth_temperature": 0.2,
            "timeout_s": None,
            "retries": None,
        },
        "mock": {
            "plan_fixture": None,
            "synth_template": "Mock synthesis for: {question}",
        },
    }
}


@dataclass
class OpenAISettings:
    api_key: str | None
    base_url: str | None
    plan_model: str
    synth_model: str
    plan_temperature: float
    synth_temperature: float
    timeout_s: float | None
    retries: int | None


@dataclass
class MockSettings:
    plan_fixture: Path | None
    synth_template: str | None


@dataclass
class DemoQAConfig:
    provider: str
    openai: OpenAISettings
    mock: MockSettings
    source: Path | None


class ConfigError(RuntimeError):
    pass


ENV_OVERRIDE_MAP: Dict[Tuple[str, ...], Tuple[str, ...]] = {
    ("llm", "provider"): ("DEMO_QA_LLM_PROVIDER",),
    ("llm", "openai", "api_key"): ("DEMO_QA_OPENAI_API_KEY", "OPENAI_API_KEY"),
    ("llm", "openai", "base_url"): ("DEMO_QA_OPENAI_BASE_URL", "OPENAI_BASE_URL"),
    ("llm", "openai", "plan_model"): ("DEMO_QA_OPENAI_PLAN_MODEL",),
    ("llm", "openai", "synth_model"): ("DEMO_QA_OPENAI_SYNTH_MODEL",),
    ("llm", "openai", "plan_temperature"): ("DEMO_QA_OPENAI_PLAN_TEMPERATURE",),
    ("llm", "openai", "synth_temperature"): ("DEMO_QA_OPENAI_SYNTH_TEMPERATURE",),
    ("llm", "openai", "timeout_s"): ("DEMO_QA_OPENAI_TIMEOUT",),
    ("llm", "openai", "retries"): ("DEMO_QA_OPENAI_RETRIES",),
    ("llm", "mock", "plan_fixture"): ("DEMO_QA_MOCK_PLAN_FIXTURE",),
    ("llm", "mock", "synth_template"): ("DEMO_QA_MOCK_SYNTH_TEMPLATE",),
}


def deep_merge(base: Mapping[str, Any], update: Mapping[str, Any]) -> Dict[str, Any]:
    merged: Dict[str, Any] = dict(base)
    for key, value in update.items():
        if isinstance(value, Mapping) and isinstance(merged.get(key), Mapping):
            merged[key] = deep_merge(merged[key], value)  # type: ignore[arg-type]
        else:
            merged[key] = value
    return merged


def _load_toml(path: Path) -> Dict[str, Any]:
    try:
        with path.open("rb") as f:
            return tomllib.load(f)
    except FileNotFoundError:
        raise
    except Exception as exc:  # pragma: no cover - tomllib should provide details
        raise ConfigError(f"Failed to read config from {path}: {exc}") from exc


def _set_nested(mapping: MutableMapping[str, Any], keys: Iterable[str], value: Any) -> None:
    keys_list = list(keys)
    cursor: MutableMapping[str, Any] = mapping
    for key in keys_list[:-1]:
        if key not in cursor or not isinstance(cursor[key], MutableMapping):
            cursor[key] = {}
        cursor = cursor[key]
    cursor[keys_list[-1]] = value


def _apply_env_overrides(config: Dict[str, Any]) -> Dict[str, Any]:
    result = dict(config)
    for key_path, env_vars in ENV_OVERRIDE_MAP.items():
        for env_var in env_vars:
            raw = os.getenv(env_var)
            if raw is None or raw == "":
                continue
            coerced: Any = raw
            key = key_path[-1]
            if key in {"plan_temperature", "synth_temperature", "timeout_s"}:
                try:
                    coerced = float(raw)
                except ValueError as exc:
                    raise ConfigError(f"Environment variable {env_var} must be a number, got {raw!r}.") from exc
            elif key == "retries":
                try:
                    coerced = int(raw)
                except ValueError as exc:
                    raise ConfigError(f"Environment variable {env_var} must be an integer, got {raw!r}.") from exc
            _set_nested(result, key_path, coerced)
            break
    return result


def _load_plan_fixture(path: Path | None) -> Dict[str, str]:
    if path is None:
        return {}
    if not path.exists():
        raise ConfigError(f"Plan fixture not found at {path}")
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as exc:
        raise ConfigError(f"Failed to read plan fixture from {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise ConfigError(f"Plan fixture in {path} must be a JSON object mapping patterns to responses")
    return {str(k): str(v) for k, v in data.items()}


def _normalize_config_paths(config: Dict[str, Any], source: Path | None) -> Dict[str, Any]:
    mock_cfg = config.get("llm", {}).get("mock", {})
    plan_fixture = mock_cfg.get("plan_fixture")
    if isinstance(plan_fixture, str) and plan_fixture:
        fixture_path = Path(plan_fixture)
        if not fixture_path.is_absolute() and source:
            fixture_path = (source.parent / fixture_path).resolve()
        mock_cfg["plan_fixture"] = fixture_path
    return config


def load_config(
    config_path: Path | None = None,
    *,
    data_dir: Path | None = None,
    provider_override: str | None = None,
) -> DemoQAConfig:
    search_paths = []
    if config_path is not None:
        search_paths.append(config_path)
    else:
        if data_dir is not None:
            search_paths.append(data_dir / "demo_qa.toml")
        search_paths.append(Path(__file__).resolve().parent / "demo_qa.toml")

    raw_config: Dict[str, Any] = {}
    used_path: Path | None = None
    for candidate in search_paths:
        if candidate.exists():
            raw_config = _load_toml(candidate)
            used_path = candidate
            break

    if config_path is not None and used_path is None:
        raise ConfigError(f"Config file not found at {config_path}.")

    merged = deep_merge(DEFAULT_CONFIG, raw_config)
    merged = _apply_env_overrides(merged)
    merged = _normalize_config_paths(merged, used_path)

    provider = provider_override or merged.get("llm", {}).get("provider") or "mock"
    if provider not in {"mock", "openai"}:
        raise ConfigError(f"Unsupported provider {provider!r}. Choose from ['mock', 'openai'].")

    openai_cfg = merged.get("llm", {}).get("openai", {})
    mock_cfg = merged.get("llm", {}).get("mock", {})

    mock_settings = MockSettings(
        plan_fixture=mock_cfg.get("plan_fixture"),
        synth_template=mock_cfg.get("synth_template"),
    )

    openai_settings = OpenAISettings(
        api_key=openai_cfg.get("api_key"),
        base_url=openai_cfg.get("base_url"),
        plan_model=openai_cfg.get("plan_model", "gpt-4o-mini"),
        synth_model=openai_cfg.get("synth_model", "gpt-4o-mini"),
        plan_temperature=float(openai_cfg.get("plan_temperature", 0.0)),
        synth_temperature=float(openai_cfg.get("synth_temperature", 0.2)),
        timeout_s=openai_cfg.get("timeout_s"),
        retries=openai_cfg.get("retries"),
    )

    if provider == "openai":
        if openai_settings.api_key in (None, ""):
            raise ConfigError("OpenAI provider selected but llm.openai.api_key is missing.")

    config = DemoQAConfig(
        provider=provider,
        openai=openai_settings,
        mock=mock_settings,
        source=used_path,
    )
    return config


def make_llm(config: DemoQAConfig):
    if config.provider == "mock":
        from .llm.mock_adapter import MockLLM

        plan_responses = _load_plan_fixture(config.mock.plan_fixture) if isinstance(config.mock.plan_fixture, Path) else {}
        return MockLLM(plan_responses=plan_responses or None, synth_template=config.mock.synth_template)

    from .llm.openai_adapter import OpenAILLM

    return OpenAILLM(
        api_key=config.openai.api_key,
        base_url=config.openai.base_url,
        plan_model=config.openai.plan_model,
        synth_model=config.openai.synth_model,
        plan_temperature=config.openai.plan_temperature,
        synth_temperature=config.openai.synth_temperature,
        timeout_s=config.openai.timeout_s,
        retries=config.openai.retries,
    )


__all__ = [
    "DemoQAConfig",
    "OpenAISettings",
    "MockSettings",
    "ConfigError",
    "load_config",
    "make_llm",
]
