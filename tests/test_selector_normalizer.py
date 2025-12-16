import pytest

from fetchgraph.relational.models import EntityDescriptor
from fetchgraph.relational.selector_normalizer import normalize_relational_selectors


def test_rejects_subquery_still():
    provider = type(
        "P",
        (),
        {"entities": [EntityDescriptor(name="as", columns=[])], "relations": []},
    )()

    selectors = {"op": "query", "$subquery": {}}

    with pytest.raises(ValueError) as exc:
        normalize_relational_selectors(provider, selectors)

    assert "$subquery" in str(exc.value)
