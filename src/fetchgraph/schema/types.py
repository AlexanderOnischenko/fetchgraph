from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, List, Optional


@dataclass
class FieldDescriptor:
    name: str
    type: Optional[str] = None
    role: Optional[str] = None
    semantic: bool = False


@dataclass
class EntityDescriptor:
    name: str
    label: Optional[str] = None
    fields: List[FieldDescriptor] = field(default_factory=list)

    def field_names(self) -> List[str]:
        return [f.name for f in self.fields]


@dataclass
class RelationDescriptor:
    name: str
    from_entity: str
    to_entity: str
    from_field: Optional[str] = None
    to_field: Optional[str] = None
    cardinality: Optional[str] = None


@dataclass
class ProviderSchema:
    entities: List[EntityDescriptor] = field(default_factory=list)
    relations: List[RelationDescriptor] = field(default_factory=list)

    def entity_index(self) -> dict[str, EntityDescriptor]:
        return {e.name: e for e in self.entities}

    def relation_index(self) -> dict[str, RelationDescriptor]:
        return {r.name: r for r in self.relations}

    @classmethod
    def from_relational(
        cls,
        entities: Iterable[object],
        relations: Iterable[object],
    ) -> "ProviderSchema":
        """Build :class:`ProviderSchema` from relational descriptors.

        The relational descriptors are expected to resemble
        ``fetchgraph.relational.models.EntityDescriptor`` and
        ``RelationDescriptor`` objects (with ``columns`` and ``join`` fields).
        Only the attributes used for schema binding are accessed.
        """

        ent_list: List[EntityDescriptor] = []
        for ent in entities:
            cols = getattr(ent, "columns", []) or []
            fields: List[FieldDescriptor] = []
            for col in cols:
                fields.append(
                    FieldDescriptor(
                        name=getattr(col, "name", ""),
                        type=getattr(col, "type", None),
                        role=getattr(col, "role", None),
                        semantic=bool(getattr(col, "semantic", False)),
                    )
                )
            ent_list.append(
                EntityDescriptor(
                    name=getattr(ent, "name", ""),
                    label=getattr(ent, "label", None),
                    fields=fields,
                )
            )

        rel_list: List[RelationDescriptor] = []
        for rel in relations:
            join = getattr(rel, "join", None)
            rel_list.append(
                RelationDescriptor(
                    name=getattr(rel, "name", ""),
                    from_entity=getattr(rel, "from_entity", ""),
                    to_entity=getattr(rel, "to_entity", ""),
                    from_field=getattr(join, "from_column", None) if join else None,
                    to_field=getattr(join, "to_column", None) if join else None,
                    cardinality=getattr(rel, "cardinality", None),
                )
            )

        return cls(entities=ent_list, relations=rel_list)
