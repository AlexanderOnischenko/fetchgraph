from __future__ import annotations

from typing import Dict, Optional

from .types import ProviderSchema


class SchemaRegistry:
    """In-memory schema registry for providers."""

    def __init__(self):
        self._cache: Dict[str, ProviderSchema] = {}

    def register(self, provider_name: str, schema: ProviderSchema) -> None:
        self._cache[provider_name] = schema

    def get(self, provider_name: str) -> Optional[ProviderSchema]:
        return self._cache.get(provider_name)

    def get_or_describe(self, provider: object) -> ProviderSchema:
        name = getattr(provider, "name", provider.__class__.__name__)
        cached = self.get(name)
        if cached:
            return cached

        schema: Optional[ProviderSchema] = None
        describe_schema = getattr(provider, "describe_schema", None)
        if callable(describe_schema):
            schema = describe_schema()
        elif hasattr(provider, "entities") and hasattr(provider, "relations"):
            from .types import ProviderSchema as PS  # local import to avoid cycles

            schema = PS.from_relational(getattr(provider, "entities"), getattr(provider, "relations"))
        else:
            fetch = getattr(provider, "fetch", None)
            if callable(fetch):
                res = fetch("", selectors={"op": "schema"})
                entities = getattr(res, "entities", None)
                relations = getattr(res, "relations", None)
                if entities is not None and relations is not None:
                    from .types import ProviderSchema as PS  # local import to avoid cycles

                    schema = PS.from_relational(entities, relations)

        if schema is None:
            raise ValueError(f"Provider {name!r} cannot describe schema")

        self.register(name, schema)
        return schema


registry = SchemaRegistry()

__all__ = ["SchemaRegistry", "registry"]
