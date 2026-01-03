from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

from ..runner import RunResult


def write_results(out_path: Path, results: Iterable[RunResult]) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        for res in results:
            f.write(json.dumps(res.to_json(), ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n")


__all__ = ["write_results"]
