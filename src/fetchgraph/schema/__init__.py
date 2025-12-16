"""Schema registry and binding helpers for relational providers."""

from .types import ProviderSchema, EntityDescriptor, FieldDescriptor, RelationDescriptor
from .policy import ResolutionPolicy
from .bind import bind_selectors, UnknownField, UnknownRelation, AmbiguousField
from .registry import SchemaRegistry, registry

__all__ = [
    "ProviderSchema",
    "EntityDescriptor",
    "FieldDescriptor",
    "RelationDescriptor",
    "ResolutionPolicy",
    "bind_selectors",
    "UnknownField",
    "UnknownRelation",
    "AmbiguousField",
    "SchemaRegistry",
    "registry",
]
