"""Schema registry and binding helpers for relational providers."""

from .types import ProviderSchema, EntityDescriptor, FieldDescriptor, RelationDescriptor
from .policy import ResolutionPolicy
from .bind import (
    AmbiguousField,
    RelationNotFromRoot,
    UnknownField,
    UnknownRelation,
    bind_selectors,
)
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
    "RelationNotFromRoot",
    "SchemaRegistry",
    "registry",
]
