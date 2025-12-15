from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Any, Deque, Dict, List, Tuple

from ..relational.models import ColumnDescriptor, EntityDescriptor, RelationDescriptor


def _normalize_name(value: Any) -> str:
    s = str(value)
    s = s.strip()
    s = s.lower()
    s = " ".join(s.split())
    return s


@dataclass(frozen=True)
class FieldCandidate:
    entity: str
    field: str
    field_type: str
    join_path: Tuple[str, ...]


class SchemaRegistry:
    entity_by_name: Dict[str, EntityDescriptor]
    fields_by_entity: Dict[str, Dict[str, ColumnDescriptor]]
    relations_by_name: Dict[str, RelationDescriptor]
    adj: Dict[str, List[RelationDescriptor]]

    def __init__(self, entities: List[EntityDescriptor], relations: List[RelationDescriptor]):
        self.entity_by_name = {}
        self.fields_by_entity = {}
        self.relations_by_name = {}
        self.adj = {}

        for entity in entities:
            norm_name = _normalize_name(entity.name)
            if norm_name in self.entity_by_name:
                raise ValueError(f"Duplicate entity name: {entity.name}")
            self.entity_by_name[norm_name] = entity
            field_map: Dict[str, ColumnDescriptor] = {}
            for column in entity.columns:
                norm_field = _normalize_name(column.name)
                if norm_field not in field_map:
                    field_map[norm_field] = column
            self.fields_by_entity[norm_name] = field_map

        sorted_relations = sorted(relations, key=lambda r: r.name)
        for relation in sorted_relations:
            norm_name = _normalize_name(relation.name)
            if norm_name in self.relations_by_name:
                raise ValueError(f"Duplicate relation name: {relation.name}")

            from_norm = _normalize_name(relation.from_entity)
            to_norm = _normalize_name(relation.to_entity)
            if from_norm not in self.entity_by_name or to_norm not in self.entity_by_name:
                raise ValueError(
                    f"Relation {relation.name} references unknown entities: "
                    f"{relation.from_entity} -> {relation.to_entity}"
                )
            self.relations_by_name[norm_name] = relation
            self.adj.setdefault(from_norm, []).append(relation)
            # Allow traversal in both directions while keeping deterministic order.
            self.adj.setdefault(to_norm, []).append(relation)

    def has_entity(self, name: str) -> bool:
        norm_name = _normalize_name(name)
        return norm_name in self.entity_by_name

    def entity(self, name: str) -> EntityDescriptor:
        norm_name = _normalize_name(name)
        if norm_name not in self.entity_by_name:
            raise KeyError(name)
        return self.entity_by_name[norm_name]

    def has_field(self, entity: str, field: str) -> bool:
        norm_entity = _normalize_name(entity)
        norm_field = _normalize_name(field)
        if norm_entity not in self.fields_by_entity:
            return False
        return norm_field in self.fields_by_entity[norm_entity]

    def field(self, entity: str, field: str) -> ColumnDescriptor:
        norm_entity = _normalize_name(entity)
        norm_field = _normalize_name(field)
        if norm_entity not in self.fields_by_entity:
            raise KeyError(entity)
        if norm_field not in self.fields_by_entity[norm_entity]:
            raise KeyError(field)
        return self.fields_by_entity[norm_entity][norm_field]

    def find_entities_with_field(self, field: str) -> List[str]:
        norm_field = _normalize_name(field)
        matches = []
        for norm_entity, fields in self.fields_by_entity.items():
            if norm_field in fields:
                matches.append(self.entity_by_name[norm_entity].name)
        return sorted(matches)

    def find_paths(self, root: str, target: str, *, max_depth: int) -> List[Tuple[str, ...]]:
        norm_root = _normalize_name(root)
        norm_target = _normalize_name(target)
        if norm_root not in self.entity_by_name:
            raise KeyError(root)
        if norm_target not in self.entity_by_name:
            raise KeyError(target)
        if max_depth < 0:
            return []

        if norm_root == norm_target:
            return [tuple()]

        queue: Deque[Tuple[str, Tuple[str, ...]]] = deque([(norm_root, tuple())])
        best_depth: Dict[str, int] = {norm_root: 0}
        paths: List[Tuple[str, ...]] = []
        min_target_depth: int | None = None

        while queue:
            current_entity, path = queue.popleft()
            depth = len(path)

            if min_target_depth is not None and depth >= min_target_depth:
                continue

            for relation in self.adj.get(current_entity, []):
                next_entity = _normalize_name(
                    relation.to_entity if _normalize_name(relation.from_entity) == current_entity else relation.from_entity
                )
                new_path = path + (relation.name,)
                new_depth = depth + 1

                if new_depth > max_depth:
                    continue

                if min_target_depth is not None and new_depth > min_target_depth:
                    continue

                current_best = best_depth.get(next_entity)
                if current_best is None or new_depth < current_best:
                    best_depth[next_entity] = new_depth
                    queue.append((next_entity, new_path))
                elif new_depth == current_best:
                    queue.append((next_entity, new_path))

                if next_entity == norm_target:
                    min_target_depth = new_depth if min_target_depth is None else min(min_target_depth, new_depth)
                    paths.append(new_path)

        if min_target_depth is None:
            return []

        shortest_paths = [p for p in paths if len(p) == min_target_depth]
        return sorted(shortest_paths)

    def field_candidates(
        self,
        root: str,
        field: str,
        *,
        max_depth: int,
        declared_with: List[str] | None = None,
    ) -> List[FieldCandidate]:
        norm_root = _normalize_name(root)
        norm_field = _normalize_name(field)

        if norm_root not in self.entity_by_name:
            raise KeyError(root)
        if max_depth < 0:
            return []

        declared_set = {_normalize_name(r) for r in declared_with} if declared_with else None
        candidates: List[FieldCandidate] = []

        root_fields = self.fields_by_entity.get(norm_root, {})
        if norm_field in root_fields:
            column = root_fields[norm_field]
            candidates.append(
                FieldCandidate(
                    entity=self.entity_by_name[norm_root].name,
                    field=column.name,
                    field_type=column.type,
                    join_path=tuple(),
                )
            )

        for norm_entity, fields in self.fields_by_entity.items():
            if norm_entity == norm_root:
                continue
            if norm_field not in fields:
                continue

            paths = self.find_paths(self.entity_by_name[norm_root].name, self.entity_by_name[norm_entity].name, max_depth=max_depth)
            if not paths:
                continue

            path = paths[0]
            column = fields[norm_field]
            candidates.append(
                FieldCandidate(
                    entity=self.entity_by_name[norm_entity].name,
                    field=column.name,
                    field_type=column.type,
                    join_path=path,
                )
            )

        def sort_key(candidate: FieldCandidate):
            join_path = candidate.join_path
            declared_first = 0
            if declared_set is not None:
                normalized_path = {_normalize_name(r) for r in join_path}
                declared_first = 0 if normalized_path.issubset(declared_set) else 1
            return (
                len(join_path),
                declared_first,
                join_path,
                candidate.entity,
                candidate.field,
            )

        candidates.sort(key=sort_key)
        return candidates
