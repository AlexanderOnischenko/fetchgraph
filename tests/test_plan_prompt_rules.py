from fetchgraph.core.context import provider_catalog_text
from fetchgraph.core.models import TaskProfile
from fetchgraph.core.utils import load_pkg_text, render_prompt
from fetchgraph.relational.models import ColumnDescriptor, EntityDescriptor, RelationDescriptor, RelationJoin
from fetchgraph.relational.providers.base import RelationalDataProvider


class MiniRelProvider(RelationalDataProvider):
    def __init__(self):
        entities = [
            EntityDescriptor(
                name="customer",
                columns=[
                    ColumnDescriptor(name="id", role="primary_key"),
                    ColumnDescriptor(name="name", semantic=True),
                ],
            ),
            EntityDescriptor(
                name="order",
                columns=[
                    ColumnDescriptor(name="id", role="primary_key"),
                    ColumnDescriptor(name="customer_id", role="foreign_key"),
                ],
            ),
        ]
        relations = [
            RelationDescriptor(
                name="order_customer",
                from_entity="order",
                to_entity="customer",
                join=RelationJoin(
                    from_entity="order",
                    from_column="customer_id",
                    to_entity="customer",
                    to_column="id",
                ),
            )
        ]
        super().__init__(name="mini_rel", entities=entities, relations=relations)

    def _handle_schema(self):  # pragma: no cover - not used
        raise NotImplementedError

    def _handle_semantic_only(self, req):  # pragma: no cover - not used
        raise NotImplementedError

    def _handle_query(self, req):  # pragma: no cover - not used
        raise NotImplementedError


def test_field_qualification_rule_present_in_prompt():
    provider = MiniRelProvider()
    catalog = provider_catalog_text({"mini": provider})
    tpl = load_pkg_text("prompts/plan_generic.md")
    profile = TaskProfile()
    prompt = render_prompt(
        tpl,
        task_name=profile.task_name,
        goal=profile.goal,
        user_query="q",
        output_format=profile.output_format,
        acceptance_criteria="",
        constraints="",
        focus_hints="",
        provider_catalog=catalog,
        lite_context_json="{}",
    )

    assert "entity: \"<to_entity>\"" in prompt
    assert "field: \"<to_entity>.<field>\"" in prompt
    assert "ilike \"%...%\"" in prompt
    assert "semantic_only" in prompt
    assert "$dsl" in prompt
    assert "$subquery" in prompt
    assert "нет поля fields" in prompt
