"""Validated, process-local MCP configuration."""

import importlib
import json
import math
import os
import re
import tomllib
from pathlib import Path
from urllib.parse import urlsplit

from fetchgraph.relational.schema import (
    ColumnConfig,
    EntityConfig,
    RelationConfig,
    SchemaConfig,
    validate_schema,
)


def load_schema(path):
    data = json.loads(path.read_text())
    entities = [
        EntityConfig(**{**e, "columns": [ColumnConfig(**c) for c in e["columns"]]})
        for e in data.pop("entities")
    ]
    relations = [RelationConfig(**r) for r in data.pop("relations", [])]
    schema = SchemaConfig(**data, entities=entities, relations=relations)
    validate_schema(schema)
    return schema


def factory(name):
    module, attribute = name.split(":")
    return getattr(importlib.import_module(module), attribute)


def load_config(path):
    path = Path(path).resolve()
    data = tomllib.loads(path.read_text())
    if (
        set(data) != {"version", "profiles", "sources"}
        or type(data["version"]) is not int
        or data["version"] != 1
    ):
        raise ValueError("Invalid configuration version or fields")
    profiles, sources = data["profiles"], data["sources"]
    if not 1 <= len(profiles) <= 8 or not 1 <= len(sources) <= 32:
        raise ValueError("Invalid catalog size")
    for alias in [*profiles, *sources]:
        if not re.fullmatch("[a-z][a-z0-9_]{0,47}", alias):
            raise ValueError("Invalid alias")
    for profile in profiles.values():
        if not {"base_url", "model"} <= profile.keys() or profile.keys() - {
            "base_url",
            "model",
            "api_key_env",
            "plan_model",
            "synth_model",
            "timeout_s",
        }:
            raise ValueError("Invalid profile fields")
        timeout = profile.get("timeout_s", 60)
        if (
            type(timeout) not in (int, float)
            or not math.isfinite(timeout)
            or timeout <= 0
        ):
            raise ValueError("Invalid inference timeout")
        if any(
            not isinstance(v, str) or not v.strip()
            for k, v in profile.items()
            if k != "timeout_s"
        ):
            raise ValueError("Empty profile value")
        url = urlsplit(profile["base_url"])
        if (
            url.scheme not in {"https", "http"}
            or not url.netloc
            or url.username is not None
            or url.query
            or url.fragment
        ):
            raise ValueError("Invalid inference endpoint")
        if "api_key_env" in profile and not os.environ.get(profile["api_key_env"]):
            raise ValueError("Missing inference credential")
    required = {
        "csv": {"data_dir", "schema"},
        "sqlite": {"database", "schema"},
        "artifact": {"run_dir", "run_id"},
        "plugin": {"factory", "options"},
    }
    for source in sources.values():
        kind = source["kind"]
        fields = {"kind", "description", "inference_profiles", "default_profile"}
        if kind not in required or set(source) != fields | required[kind]:
            raise ValueError("Invalid source fields")
        allowed = source["inference_profiles"]
        if (
            not isinstance(allowed, list)
            or not allowed
            or len(set(allowed)) != len(allowed)
            or not set(allowed) <= profiles.keys()
        ):
            raise ValueError("Invalid source profiles")
        if (
            source["default_profile"] not in allowed
            or not isinstance(source["description"], str)
            or not 1 <= len(source["description"]) <= 240
        ):
            raise ValueError("Invalid default profile or description")
        for key in required[kind] & {"data_dir", "schema", "database", "run_dir"}:
            resolved = (path.parent / source[key]).resolve()
            if (
                not resolved.exists()
                or (key in {"data_dir", "run_dir"}) != resolved.is_dir()
            ):
                raise ValueError("Source path missing")
            source[key] = resolved
        if kind in {"csv", "sqlite"}:
            source["_schema"] = load_schema(source["schema"])
        if kind == "csv":
            for entity in source["_schema"].entities:
                file = (source["data_dir"] / (entity.source or "")).resolve()
                if not file.is_relative_to(source["data_dir"]) or not file.is_file():
                    raise ValueError("Invalid CSV path")
        if kind == "artifact" and (
            not isinstance(source["run_id"], str) or not source["run_id"].strip()
        ):
            raise ValueError("Invalid run ID")
        if kind == "plugin":
            if not isinstance(source["options"], dict):
                raise ValueError("Invalid factory options")
            if not callable(factory(source["factory"])):
                raise ValueError("Invalid provider factory")
            json.dumps(source["options"], allow_nan=False)
    return data
