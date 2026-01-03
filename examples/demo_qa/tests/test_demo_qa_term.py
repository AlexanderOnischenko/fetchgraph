from __future__ import annotations

from examples.demo_qa.term import color, render_table, strip_ansi


def test_render_table_aligns_colored_cells() -> None:
    headers = ["name", "num"]
    rows = [
        [color("alpha", "red", use_color=True), color("2", "green", use_color=True)],
        [color("beta", "yellow", use_color=True), color("10", "green", use_color=True)],
    ]
    output = render_table(headers, rows, align_right={1})
    lines = output.splitlines()
    plain = [strip_ansi(line) for line in lines]

    assert len(lines) == 3
    assert any("\x1b[" in line for line in lines[1:])
    num_start = plain[0].index("num")
    assert plain[1].index("2") == num_start + len("num") - len("2")
    assert plain[2].index("10") == num_start + len("num") - len("10")
