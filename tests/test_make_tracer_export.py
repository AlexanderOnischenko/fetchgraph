from __future__ import annotations

import subprocess


def test_make_tracer_export_no_default_filters() -> None:
    result = subprocess.run(
        [
            "make",
            "-n",
            "tracer-export",
            "REPLAY_ID=plan_normalize.spec_v1",
            "CASE=agg_003",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    output = result.stdout + result.stderr
    assert "--provider" not in output
    assert "--spec-idx" not in output
