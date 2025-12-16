from __future__ import annotations

import difflib
import importlib
import importlib.util
import os
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

_yaml_spec = importlib.util.find_spec("yaml")
yaml = importlib.import_module("yaml") if _yaml_spec is not None else None

from .ast import Clause, ClauseOrGroup, NormalizedQuerySketch, QuerySketch, WhereExpr
from .diagnostics import Diagnostics, Severity




_STATIC_SPEC: Dict[str, Any] = {
    "defaults": {"get": ["*"], "with": [], "take": 200},
        "keys": {
            "from": {
                "aliases": ["root", "entity", "root_entity"],
                "required": True,
            },
            "where": {"aliases": ["find", "filter", "filters"], "required": True, "default": []},
            "get": {"aliases": ["select", "fields"]},
            "with": {"aliases": ["include", "joins", "join", "relations"]},
            "take": {"aliases": ["limit", "top"]},
        },
    "operators": {
        "canonical": [
            "=",
            "!=",
            "<",
            ">",
            "<=",
            ">=",
            "contains",
            "starts",
            "ends",
            "in",
            "between",
            "before",
            "after",
            "is",
            "similar",
            "related",
        ],
        "aliases": {
            "eq": "=",
            "neq": "!=",
            "ne": "!=",
            "gt": ">",
            "gte": ">=",
            "lt": "<",
            "lte": "<=",
            "like": "is",
            "ilike": "is",
        },
        "autocorrect": {"cutoff": 0.8},
    },
}


@dataclass
class DslSpec:
    defaults: Dict[str, Any]
    keys: Dict[str, Dict[str, Any]]
    operators: Dict[str, Any]

    @classmethod
    def load(cls, path: Optional[str] = None) -> "DslSpec":
        spec_path = path or os.path.join(os.path.dirname(__file__), "spec.yaml")
        if yaml is not None:
            with open(spec_path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f)
        else:
            data = _STATIC_SPEC
        return cls(
            defaults=data.get("defaults", {}),
            keys=data.get("keys", {}),
            operators=data.get("operators", {}),
        )


_DEFAULT_SPEC: Optional[DslSpec] = None


def get_spec(path: Optional[str] = None) -> DslSpec:
    global _DEFAULT_SPEC
    if _DEFAULT_SPEC is None or path:
        _DEFAULT_SPEC = DslSpec.load(path)
    return _DEFAULT_SPEC


def normalize_query_sketch(q: QuerySketch | Dict[str, Any], *, spec: DslSpec | None = None) -> Tuple[NormalizedQuerySketch, Diagnostics]:
    spec = spec or get_spec()
    diagnostics = Diagnostics()
    data = q.data if isinstance(q, QuerySketch) else dict(q)

    key_aliases = _build_key_aliases(spec)
    canonical: Dict[str, Any] = {}

    for key, value in data.items():
        canonical_key = key_aliases.get(key, key if key in spec.keys else None)
        if canonical_key is None:
            diagnostics.add(
                code="DSL_UNKNOWN_KEY",
                message=f"Unknown key '{key}'",
                path=f"$.{key}",
                severity=Severity.WARNING,
            )
            continue
        canonical[canonical_key] = value

    normalized_where, where_diags = _normalize_where(canonical.get("where"), spec, path="$.where")
    diagnostics.extend(where_diags)

    if "from" not in canonical:
        diagnostics.add(
            code="DSL_MISSING_REQUIRED_KEY",
            message="Missing required key 'from'",
            path="$.from",
            severity=Severity.ERROR,
        )
        canonical_from = ""
    else:
        canonical_from = canonical.get("from", "")

    if "where" not in canonical:
        diagnostics.add(
            code="DSL_MISSING_REQUIRED_KEY",
            message="Missing required key 'where'; inserted empty filter",
            path="$.where",
            severity=Severity.WARNING,
        )

    get_fields = _normalize_get_fields(
        canonical.get("get", spec.defaults.get("get", ["*"])), canonical_from
    )
    with_fields = _ensure_list(canonical.get("with", spec.defaults.get("with", [])))
    take_value = canonical.get("take", spec.defaults.get("take"))

    take_normalized = _normalize_take(take_value, spec, diagnostics)

    normalized = NormalizedQuerySketch(
        from_=canonical_from,
        where=normalized_where,
        get=[str(v) for v in get_fields],
        with_=[str(v) for v in with_fields],
        take=take_normalized,
    )

    return normalized, diagnostics


def _build_key_aliases(spec: DslSpec) -> Dict[str, str]:
    aliases: Dict[str, str] = {}
    for canonical, meta in spec.keys.items():
        aliases[canonical] = canonical
        for alias in meta.get("aliases", []):
            aliases[alias] = canonical
    return aliases


def _ensure_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _normalize_get_fields(value: Any, root_entity: str) -> List[str]:
    fields = _ensure_list(value)
    normalized: List[str] = []
    for item in fields:
        if isinstance(item, dict) and "expr" in item:
            expr = item.get("expr")
            if isinstance(expr, str):
                path = expr
                if root_entity and "." not in path:
                    path = f"{root_entity}.{path}"
                normalized.append(path)
                continue
        normalized.append(str(item))
    return normalized


def _is_comparison_dict(value: Any) -> bool:
    return isinstance(value, dict) and (
        value.get("type") == "comparison" or {"field", "value"}.issubset(set(value.keys()))
    )


def _normalize_comparison_dict(
    obj: Dict[str, Any], spec: DslSpec, path: str, diagnostics: Diagnostics
) -> Optional[Clause]:
    raw_field = obj.get("field")
    if not isinstance(raw_field, str):
        diagnostics.add(
            code="DSL_BAD_CLAUSE_PATH",
            message="Comparison field must be a string",
            path=f"{path}.field",
            severity=Severity.ERROR,
        )
        return None

    entity = obj.get("entity")
    if isinstance(entity, str) and entity:
        raw_field = f"{entity}.{raw_field}" if not raw_field.startswith(f"{entity}.") else raw_field

    raw_op = obj.get("op")
    value = obj.get("value")

    if raw_op is None:
        op, value = _auto_operator(value)
    else:
        op = _normalize_operator(raw_op, spec, diagnostics, path=f"{path}.op")
        if op is None:
            return None

    return Clause(path=raw_field, op=op, value=value)


def _normalize_where(where_value: Any, spec: DslSpec, path: str) -> Tuple[WhereExpr, Diagnostics]:
    diagnostics = Diagnostics()
    if where_value is None:
        return WhereExpr(), diagnostics

    if isinstance(where_value, list):
        all_clauses: List[ClauseOrGroup] = []
        for idx, clause in enumerate(where_value):
            normalized, diags = _normalize_clause_or_group(clause, spec, path=f"{path}.all[{idx}]")
            diagnostics.extend(diags)
            if normalized is not None:
                all_clauses.append(normalized)
        return WhereExpr(all=all_clauses), diagnostics

    if isinstance(where_value, dict):
        if _is_comparison_dict(where_value):
            clause = _normalize_comparison_dict(where_value, spec, path, diagnostics)
            clauses = [clause] if clause is not None else []
            return WhereExpr(all=clauses), diagnostics

        allowed_keys = {"all", "any", "not"}
        present_keys = set(where_value.keys())
        for key in present_keys - allowed_keys:
            diagnostics.add(
                code="DSL_UNKNOWN_KEY",
                message=f"Unknown where key '{key}'",
                path=f"{path}.{key}",
                severity=Severity.WARNING,
            )

        all_list = where_value.get("all", [])
        any_list = where_value.get("any", [])
        not_value = where_value.get("not")

        normalized_all, diags_all = _normalize_clause_list(all_list, spec, f"{path}.all")
        normalized_any, diags_any = _normalize_clause_list(any_list, spec, f"{path}.any")
        diagnostics.extend(diags_all)
        diagnostics.extend(diags_any)

        normalized_not: Optional[ClauseOrGroup] = None
        if not_value is not None:
            normalized_not, diags_not = _normalize_clause_or_group(not_value, spec, f"{path}.not")
            diagnostics.extend(diags_not)

        if not present_keys.intersection(allowed_keys):
            diagnostics.add(
                code="DSL_EMPTY_WHERE_OBJECT",
                message="Where object must include 'all', 'any', or 'not' groups",
                path=path,
                severity=Severity.WARNING,
            )

        return WhereExpr(all=normalized_all, any=normalized_any, not_=normalized_not), diagnostics

    diagnostics.add(
        code="DSL_UNKNOWN_KEY",
        message="Invalid where format; expected list or object",
        path=path,
        severity=Severity.ERROR,
    )
    return WhereExpr(), diagnostics


def _normalize_take(value: Any, spec: DslSpec, diagnostics: Diagnostics) -> int:
    default_take = spec.defaults.get("take", 0)
    if value is None:
        return int(default_take)

    try:
        return int(value)
    except (TypeError, ValueError):
        diagnostics.add(
            code="DSL_INVALID_TAKE",
            message="Invalid take value; expected an integer",
            path="$.take",
            severity=Severity.ERROR,
        )
        return int(default_take)


def _normalize_clause_list(value: Any, spec: DslSpec, path: str) -> Tuple[List[ClauseOrGroup], Diagnostics]:
    diagnostics = Diagnostics()
    clauses: List[ClauseOrGroup] = []
    if not isinstance(value, list):
        diagnostics.add(
            code="DSL_BAD_WHERE_GROUP_TYPE",
            message="Where group must be a list of clauses",
            path=path,
            severity=Severity.ERROR,
        )
        return clauses, diagnostics
    for idx, clause in enumerate(value):
        normalized, diags = _normalize_clause_or_group(clause, spec, path=f"{path}[{idx}]")
        diagnostics.extend(diags)
        if normalized is not None:
            clauses.append(normalized)
    return clauses, diagnostics


def _normalize_clause_or_group(obj: Any, spec: DslSpec, path: str) -> Tuple[Optional[ClauseOrGroup], Diagnostics]:
    diagnostics = Diagnostics()
    if isinstance(obj, dict):
        if _is_comparison_dict(obj):
            clause = _normalize_comparison_dict(obj, spec, path, diagnostics)
            return clause, diagnostics
        nested, nested_diags = _normalize_where(obj, spec, path=path)
        diagnostics.extend(nested_diags)
        return nested, diagnostics
    if isinstance(obj, list):
        clause = _normalize_clause(obj, spec, path=path, diagnostics=diagnostics)
        return clause, diagnostics
    diagnostics.add(
        code="DSL_BAD_CLAUSE_ARITY",
        message="Clause must be a list",
        path=path,
        severity=Severity.ERROR,
    )
    return None, diagnostics


def _normalize_clause(obj: List[Any], spec: DslSpec, path: str, diagnostics: Diagnostics) -> Optional[Clause]:
    if len(obj) not in (2, 3):
        diagnostics.add(
            code="DSL_BAD_CLAUSE_ARITY",
            message="Clause must contain 2 or 3 items",
            path=path,
            severity=Severity.ERROR,
        )
        return None

    raw_path = obj[0]
    if not isinstance(raw_path, str):
        diagnostics.add(
            code="DSL_BAD_CLAUSE_PATH",
            message="Clause path must be a string",
            path=f"{path}[0]",
            severity=Severity.ERROR,
        )
        return None

    if len(obj) == 2:
        raw_value = obj[1]
        op, value = _auto_operator(raw_value)
    else:
        raw_op = obj[1]
        value = obj[2]
        op = _normalize_operator(raw_op, spec, diagnostics, path=f"{path}[1]")
        if op is None:
            return None

    return Clause(path=raw_path, op=op, value=value)


def _auto_operator(value: Any) -> Tuple[str, Any]:
    if isinstance(value, str):
        return "is", value
    if isinstance(value, (int, float, bool)) or value is None:
        return "=", value
    if isinstance(value, list):
        if len(value) == 2 and all(isinstance(v, str) and _looks_like_date(v) for v in value):
            return "between", value
        return "in", value
    return "is", value


def _looks_like_date(value: str) -> bool:
    iso_date = r"^\d{4}-\d{2}-\d{2}(?:[T\s]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?)?$"
    return re.match(iso_date, value) is not None


def _normalize_operator(raw_op: Any, spec: DslSpec, diagnostics: Diagnostics, path: str) -> Optional[str]:
    if not isinstance(raw_op, str):
        diagnostics.add(
            code="DSL_UNKNOWN_OP",
            message="Operator must be a string",
            path=path,
            severity=Severity.ERROR,
        )
        return None

    op_lower = raw_op.lower()
    canonical_ops = sorted(spec.operators.get("canonical", []))
    alias_map = {k.lower(): v for k, v in spec.operators.get("aliases", {}).items()}

    if op_lower in canonical_ops:
        return op_lower if raw_op.islower() else op_lower

    if op_lower in alias_map:
        return alias_map[op_lower]

    cutoff = float(spec.operators.get("autocorrect", {}).get("cutoff", 0))
    close = difflib.get_close_matches(op_lower, canonical_ops, n=1, cutoff=cutoff)
    if close:
        corrected = close[0]
        diagnostics.add(
            code="DSL_OP_AUTOCORRECT",
            message=f"Operator '{raw_op}' corrected to '{corrected}'",
            path=path,
            severity=Severity.WARNING,
        )
        return corrected

    diagnostics.add(
        code="DSL_UNKNOWN_OP",
        message=f"Unknown operator '{raw_op}'",
        path=path,
        severity=Severity.ERROR,
    )
    return None


def parse_and_normalize(src: str | Dict[str, Any]) -> Tuple[NormalizedQuerySketch, Diagnostics]:
    from .parser import parse_query_sketch

    parsed, parse_diags = parse_query_sketch(src)
    normalized, norm_diags = normalize_query_sketch(parsed, spec=get_spec())

    parse_diags.extend(norm_diags)
    return normalized, parse_diags
