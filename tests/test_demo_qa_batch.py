from __future__ import annotations

import itertools

import pytest

from examples.demo_qa.batch import bad_statuses, is_failure, render_markdown


@pytest.mark.parametrize(
    "fail_on,require_assert",
    itertools.product(["bad", "error", "unchecked", "any", "skipped"], [False, True]),
)
def test_is_failure_matches_bad_statuses(fail_on: str, require_assert: bool) -> None:
    statuses = ["ok", "mismatch", "failed", "error", "unchecked", "plan_only", "skipped"]
    bad = bad_statuses(fail_on, require_assert)
    assert bad  # sanity check
    for status in statuses:
        assert is_failure(status, fail_on, require_assert) == (status in bad)


def test_render_markdown_uses_fail_policy() -> None:
    compare = {
        "base_counts": {"ok": 0, "mismatch": 2, "error": 1, "failed": 0},
        "new_counts": {"ok": 1, "mismatch": 0, "error": 0, "failed": 0},
        "base_bad_total": 1,
        "new_bad_total": 0,
        "fail_on": "error",
        "require_assert": False,
        "new_fail": [],
        "fixed": [],
        "still_fail": [],
        "all_ids": [],
    }
    report = render_markdown(compare, None)
    assert "- Base OK: 0, Bad: 1" in report
    assert "- New  OK: 1, Bad: 0" in report
