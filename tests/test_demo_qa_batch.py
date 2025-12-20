from __future__ import annotations

import itertools

import pytest

from examples.demo_qa.batch import bad_statuses, is_failure


@pytest.mark.parametrize(
    "fail_on,require_assert",
    itertools.product(["bad", "error", "mismatch", "unchecked", "any", "skipped"], [False, True]),
)
def test_is_failure_matches_bad_statuses(fail_on: str, require_assert: bool) -> None:
    statuses = ["ok", "mismatch", "failed", "error", "unchecked", "plan_only", "skipped"]
    bad = bad_statuses(fail_on, require_assert)
    assert bad  # sanity check
    for status in statuses:
        assert is_failure(status, fail_on, require_assert) == (status in bad)
