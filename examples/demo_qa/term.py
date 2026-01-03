from __future__ import annotations

import re
from typing import Iterable

ANSI: dict[str, str] = {
    "reset": "\x1b[0m",
    "red": "\x1b[31m",
    "green": "\x1b[32m",
    "yellow": "\x1b[33m",
    "gray": "\x1b[90m",
}

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def should_use_color(mode: str, stream) -> bool:
    if mode == "always":
        return True
    if mode == "never":
        return False
    return bool(getattr(stream, "isatty", lambda: False)())


def color(text: str, name: str | None, use_color: bool) -> str:
    if not name or not use_color:
        return text
    prefix = ANSI.get(name)
    if not prefix:
        return text
    return f"{prefix}{text}{ANSI['reset']}"


def strip_ansi(text: str) -> str:
    return _ANSI_RE.sub("", text)


def truncate(text: object | None, max_len: int) -> str:
    if text is None:
        return "-"
    if max_len <= 0:
        return ""
    s = str(text)
    plain = strip_ansi(s)
    if len(plain) <= max_len:
        return s
    if max_len == 1:
        truncated_plain = plain[:1]
    else:
        truncated_plain = plain[: max_len - 1] + "…"
    match = re.match(r"^(\x1b\[[0-9;]*m)(.*)(\x1b\[0m)$", s)
    if match:
        prefix, _, suffix = match.groups()
        return f"{prefix}{truncated_plain}{suffix}"
    return truncated_plain


def fmt_pct(x: float | None, digits: int = 1) -> str:
    return f"{x*100:.{digits}f}%" if x is not None else "-"


def fmt_num(x: int | float | None) -> str:
    if x is None:
        return "-"
    if isinstance(x, float) and x.is_integer():
        return str(int(x))
    return str(x)


def _pad(text: str, width: int, *, right: bool) -> str:
    visible = len(strip_ansi(text))
    pad = max(width - visible, 0)
    return f"{' ' * pad}{text}" if right else f"{text}{' ' * pad}"


def render_table(
    headers: list[str],
    rows: list[list[str]],
    *,
    align_right: set[int] | None = None,
    indent: str = "",
    col_max: dict[int, int] | None = None,
) -> str:
    align_right = align_right or set()
    col_max = col_max or {}

    processed: list[list[str]] = []
    for row in rows:
        processed_row: list[str] = []
        for idx, cell in enumerate(row):
            cell_text = "-" if cell is None else str(cell)
            if idx in col_max:
                cell_text = truncate(cell_text, col_max[idx])
            processed_row.append(cell_text)
        processed.append(processed_row)

    widths = [len(strip_ansi(h)) for h in headers]
    for row in processed:
        for idx, cell in enumerate(row):
            if idx >= len(widths):
                widths.append(0)
            widths[idx] = max(widths[idx], len(strip_ansi(cell)))

    def _format_row(row: Iterable[str]) -> str:
        cells = []
        for idx, cell in enumerate(row):
            right = idx in align_right
            cells.append(_pad(cell, widths[idx], right=right))
        return f"{indent}" + "  ".join(cells)

    lines = [_format_row(headers)]
    for row in processed:
        lines.append(_format_row(row))
    return "\n".join(lines)


__all__ = [
    "ANSI",
    "color",
    "should_use_color",
    "strip_ansi",
    "truncate",
    "fmt_pct",
    "fmt_num",
    "render_table",
]
