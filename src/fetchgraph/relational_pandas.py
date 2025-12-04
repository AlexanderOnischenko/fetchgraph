from __future__ import annotations

"""Pandas-backed relational provider for in-memory datasets."""

from typing import Any, Dict, List, Mapping, Optional, cast

import pandas as pd

from .relational_base import RelationalDataProvider
from .relational_models import (
    AggregationResult,
    AggregationSpec,
    ComparisonFilter,
    EntityDescriptor,
    FilterClause,
    GroupBySpec,
    LogicalFilter,
    QueryResult,
    RelationalQuery,
    RowResult,
    RelationDescriptor,
    SemanticClause,
    SemanticOnlyResult,
)
from .semantic_backend import SemanticBackend


class PandasRelationalDataProvider(RelationalDataProvider):
    """Pandas-backed relational provider for in-memory datasets.

    >>> import pandas as pd
    >>> customers = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
    >>> orders = pd.DataFrame({"id": [10, 11], "customer_id": [1, 2], "total": [100, 200]})
    >>> from .relational_models import ColumnDescriptor, EntityDescriptor, RelationDescriptor, RelationJoin
    >>> entities = [
    ...     EntityDescriptor(name="customer", columns=[ColumnDescriptor(name="id", role="primary_key"), ColumnDescriptor(name="name")]),
    ...     EntityDescriptor(name="order", columns=[ColumnDescriptor(name="id", role="primary_key"), ColumnDescriptor(name="customer_id", role="foreign_key"), ColumnDescriptor(name="total", type="int")]),
    ... ]
    >>> relations = [
    ...     RelationDescriptor(
    ...         name="order_customer",
    ...         from_entity="order",
    ...         to_entity="customer",
    ...         join=RelationJoin(from_entity="order", from_column="customer_id", to_entity="customer", to_column="id"),
    ...     )
    ... ]
    >>> provider = PandasRelationalDataProvider(
    ...     name="orders_rel",
    ...     entities=entities,
    ...     relations=relations,
    ...     frames={"customer": customers, "order": orders},
    ... )
    >>> req = RelationalQuery(root_entity="order", relations=["order_customer"], limit=5)
    >>> res = provider.fetch("demo", selectors=req.model_dump())
    >>> len(res.rows)
    2
    >>> res.rows[0].related["customer"]["name"]
    'Alice'
    """

    def __init__(
        self,
        name: str,
        entities: list[EntityDescriptor],
        relations: list[RelationDescriptor],
        frames: Mapping[str, pd.DataFrame],
        semantic_backend: Optional[SemanticBackend] = None,
        primary_keys: Optional[Mapping[str, str]] = None,
    ):
        super().__init__(name, entities, relations)
        self.frames = frames
        self.semantic_backend = semantic_backend
        self.primary_keys = primary_keys or {}
        self._entity_index: Dict[str, Any] = {e.name: e for e in entities}

    def _get_frame(self, entity: str) -> pd.DataFrame:
        if entity not in self.frames:
            raise KeyError(f"No dataframe provided for entity '{entity}'")
        return self.frames[entity]

    def _pk_column(self, entity: str) -> Optional[str]:
        if entity in self.primary_keys:
            return self.primary_keys[entity]
        desc = self._entity_index.get(entity)
        if not desc:
            return None
        for col in desc.columns:
            if col.role == "primary_key":
                return col.name
        return None

    def _resolve_column(self, df: pd.DataFrame, root_entity: str, field: str, entity: Optional[str] = None) -> str:
        ent = entity
        fld = field
        if ent is None and "." in field:
            ent, fld = field.split(".", 1)
        if ent is None or ent == root_entity:
            if fld in df.columns:
                return fld
        prefixed = f"{ent}.{fld}" if ent else fld
        prefixed_alt = f"{ent}__{fld}" if ent else fld
        for candidate in (prefixed_alt, prefixed):
            if candidate in df.columns:
                return candidate
        raise KeyError(f"Column not found for entity '{ent or root_entity}': {fld}")

    def _apply_comparison(self, series: pd.Series, op: str, value: Any) -> pd.Series:
        if op == "=":
            return series == value
        if op == "!=":
            return series != value
        if op == ">":
            return series > value
        if op == "<":
            return series < value
        if op == ">=":
            return series >= value
        if op == "<=":
            return series <= value
        if op == "in":
            return series.isin(value)
        if op == "not_in":
            return ~series.isin(value)
        if op == "like":
            return series.astype(str).str.contains(str(value), case=True, regex=False)
        if op == "ilike":
            return series.astype(str).str.contains(str(value), case=False, regex=False)
        raise ValueError(f"Unsupported comparison operator: {op}")

    def _apply_filters(self, df: pd.DataFrame, root_entity: str, clause: Optional[FilterClause]) -> pd.DataFrame:
        if clause is None:
            return df
        if isinstance(clause, ComparisonFilter):
            col = self._resolve_column(df, root_entity, clause.field, clause.entity)
            series = df[col]
            if isinstance(series, pd.DataFrame):
                series = series.squeeze(axis=1)
            series = cast(pd.Series, series)
            mask = self._apply_comparison(series, clause.op, clause.value)
            mask = cast(pd.Series, mask).astype(bool)
            return df.loc[mask]
        if isinstance(clause, LogicalFilter):
            if clause.op == "and":
                for sub in clause.clauses:
                    df = self._apply_filters(df, root_entity, sub)
                return df
            masks = [self._apply_filters(df, root_entity, sub).index for sub in clause.clauses]
            if not masks:
                return df
            combined = masks[0]
            for idx in masks[1:]:
                combined = combined.union(idx)
            return df.loc[combined]
        return df

    def _apply_semantic_clauses(self, df: pd.DataFrame, root_entity: str, clauses: List[SemanticClause]) -> pd.DataFrame:
        if not clauses:
            return df
        if not self.semantic_backend:
            raise RuntimeError("Semantic backend is not configured")
        result_df: pd.DataFrame = df
        has_boost = False
        for clause in clauses:
            pk = self._pk_column(clause.entity)
            if not pk:
                raise ValueError(f"Primary key not defined for entity '{clause.entity}'")
            matches = self.semantic_backend.search(clause.entity, clause.fields, clause.query, clause.top_k)
            if clause.threshold is not None:
                matches = [m for m in matches if m.score >= clause.threshold]
            match_ids = [m.id for m in matches]
            col = self._resolve_column(result_df, root_entity, pk, clause.entity)
            if clause.mode == "filter":
                result_df = result_df.loc[result_df[col].isin(match_ids)]
            elif clause.mode == "boost":
                scores = {m.id: m.score for m in matches}
                if "__semantic_score" not in result_df.columns:
                    result_df["__semantic_score"] = 0.0
                boost_scores = result_df[col].map(lambda rid: scores.get(rid, 0.0)).fillna(0).astype(float)
                result_df["__semantic_score"] = result_df["__semantic_score"].astype(float) + boost_scores
                has_boost = True
        if has_boost:
            result_df = result_df.sort_values("__semantic_score", ascending=False)
        return result_df

    def _relation_by_name(self, name: str):
        for r in self.relations:
            if r.name == name:
                return r
        raise KeyError(f"Relation '{name}' not found")

    def _perform_join(self, df: pd.DataFrame, relation, root_entity: str) -> pd.DataFrame:
        left_entity: Optional[str] = None
        right_entity: Optional[str] = None
        left_field: Optional[str] = None
        right_field: Optional[str] = None

        if relation.from_entity == relation.to_entity:
            left_entity = relation.from_entity
            right_entity = relation.to_entity
            left_field = relation.join.from_column
            right_field = relation.join.to_column
        elif relation.from_entity == root_entity or any(col.startswith(f"{relation.from_entity}__") or col == relation.from_entity for col in df.columns):
            left_entity = relation.from_entity
            right_entity = relation.to_entity
            left_field = relation.join.from_column
            right_field = relation.join.to_column
        elif relation.to_entity == root_entity or any(col.startswith(f"{relation.to_entity}__") or col == relation.to_entity for col in df.columns):
            left_entity = relation.to_entity
            right_entity = relation.from_entity
            left_field = relation.join.to_column
            right_field = relation.join.from_column
        else:
            raise ValueError(f"Neither entity of relation '{relation.name}' present in dataframe")

        if left_entity is None or right_entity is None or left_field is None or right_field is None:
            raise ValueError(f"Relation '{relation.name}' is missing join information")

        left_col = self._resolve_column(df, root_entity, left_field, left_entity)
        right_df = self._get_frame(right_entity).copy()
        right_df["__merge_key"] = right_df[right_field]
        rename_map = {col: f"{right_entity}__{col}" for col in right_df.columns if col != "__merge_key"}
        right_df = right_df.rename(columns=rename_map)
        merged = df.merge(
            right_df,
            how=relation.join.join_type,
            left_on=left_col,
            right_on="__merge_key",
            suffixes=("", f"_{right_entity}"),
        )
        merged = merged.drop(columns=[col for col in merged.columns if col.endswith("__merge_key")], errors="ignore")
        return merged

    def _apply_select(self, df: pd.DataFrame, root_entity: str, select: List):
        if not select:
            return df
        cols: List[str] = []
        alias_map: Dict[str, str] = {}
        for expr in select:
            if "." in expr.expr:
                ent, fld = expr.expr.split(".", 1)
                col = self._resolve_column(df, root_entity, fld, ent)
            else:
                col = self._resolve_column(df, root_entity, expr.expr)
            cols.append(col)
            if expr.alias:
                alias_map[col] = expr.alias
        selected = df[cols].copy()
        if alias_map:
            selected = selected.rename(columns=alias_map)
        return selected

    def _handle_query(self, req: RelationalQuery):
        df = self._get_frame(req.root_entity).copy()
        base_columns = list(df.columns)

        for rel_name in req.relations:
            relation = self._relation_by_name(rel_name)
            df = self._perform_join(df, relation, req.root_entity)

        df = self._apply_semantic_clauses(df, req.root_entity, req.semantic_clauses)

        if req.filters:
            df = self._apply_filters(df, req.root_entity, req.filters)

        if req.group_by or req.aggregations:
            return self._aggregate(df, req)

        if req.select:
            df = self._apply_select(df, req.root_entity, req.select)

        if req.offset:
            df = df.iloc[req.offset :]
        if req.limit is not None:
            df = df.iloc[: req.limit]

        rows = [self._row_from_series(row, base_columns, req.root_entity) for _, row in df.iterrows()]
        return QueryResult(rows=rows, meta={"relations_used": req.relations})

    def _aggregate(self, df: pd.DataFrame, req: RelationalQuery):
        if req.group_by:
            group_cols = [self._resolve_column(df, req.root_entity, g.field, g.entity) for g in req.group_by]
            grouped = df.groupby(group_cols, dropna=False)
            agg_kwargs: Dict[str, Any] = {}
            for spec in req.aggregations:
                col = self._resolve_column(df, req.root_entity, spec.field)
                func: Any
                if spec.agg == "count_distinct":
                    func = lambda s: s.nunique(dropna=True)
                elif spec.agg == "avg":
                    func = "mean"
                else:
                    func = spec.agg
                alias = spec.alias or f"{spec.agg}_{spec.field}"
                agg_kwargs[alias] = pd.NamedAgg(column=col, aggfunc=func)
            if agg_kwargs:
                agg_df = grouped.agg(**agg_kwargs).reset_index()
            else:
                agg_df = grouped.size().reset_index()
                agg_df = agg_df.rename(columns={0: "count"})
            if req.offset:
                agg_df = agg_df.iloc[req.offset :]
            if req.limit is not None:
                agg_df = agg_df.iloc[: req.limit]
            rows = [RowResult(entity=req.root_entity, data=row.to_dict()) for _, row in agg_df.iterrows()]
            return QueryResult(rows=rows, meta={"group_by": group_cols, "relations_used": req.relations})

        agg_results: Dict[str, AggregationResult] = {}
        for spec in req.aggregations:
            col = self._resolve_column(df, req.root_entity, spec.field)
            if spec.agg == "count_distinct":
                value = df[col].nunique(dropna=True)
            elif spec.agg == "count":
                value = df[col].count()
            elif spec.agg == "avg":
                value = df[col].mean()
            else:
                value = getattr(df[col], spec.agg)()
            alias = spec.alias or f"{spec.agg}_{spec.field}"
            agg_results[alias] = AggregationResult(key=alias, value=value)
        return QueryResult(aggregations=agg_results, meta={"relations_used": req.relations})

    def _row_from_series(self, row: pd.Series, base_columns: List[str], root_entity: str) -> RowResult:
        data = {col: row[col] for col in base_columns if col in row.index}
        related: Dict[str, Dict[str, Any]] = {}
        for col in row.index:
            if isinstance(col, str) and "__" in col:
                ent, fld = col.split("__", 1)
                related.setdefault(ent, {})[fld] = row[col]
        return RowResult(entity=root_entity, data=data, related=related)

    def _handle_semantic_only(self, req):
        if not self.semantic_backend:
            raise RuntimeError("Semantic backend is not configured")
        matches = self.semantic_backend.search(req.entity, req.fields, req.query, req.top_k)
        return SemanticOnlyResult(matches=matches)


__all__ = ["PandasRelationalDataProvider"]
