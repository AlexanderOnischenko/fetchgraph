from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

VALID_BUCKETS = {"fixed", "known_bad"}


@dataclass(frozen=True)
class FixtureLayout:
    root: Path
    bucket: str

    @property
    def bucket_dir(self) -> Path:
        return self.root / self.bucket

    def case_path(self, stem: str) -> Path:
        return self.bucket_dir / f"{stem}.case.json"

    def expected_path(self, stem: str) -> Path:
        return self.bucket_dir / f"{stem}.expected.json"

    def resources_dir(self, stem: str) -> Path:
        return self.bucket_dir / "resources" / stem


def find_case_bundles(
    *,
    root: Path,
    bucket: str | None,
    name: str | None,
    pattern: str | None,
) -> list[Path]:
    if name and pattern:
        raise ValueError("Use only one of name or pattern.")

    root = root.resolve()
    if bucket in (None, "all"):
        buckets = sorted(VALID_BUCKETS)
    elif bucket in VALID_BUCKETS:
        buckets = [bucket]
    else:
        raise ValueError(f"Unsupported bucket: {bucket}")

    if name:
        matches: list[Path] = []
        for entry in buckets:
            layout = FixtureLayout(root, entry)
            case_path = layout.case_path(name)
            if case_path.exists():
                matches.append(case_path)
        return matches

    if pattern:
        if pattern.endswith(".case.json"):
            glob_pattern = pattern
        else:
            glob_pattern = f"{pattern}.case.json"
    else:
        glob_pattern = "*.case.json"

    matches = []
    for entry in buckets:
        layout = FixtureLayout(root, entry)
        if not layout.bucket_dir.exists():
            continue
        matches.extend(layout.bucket_dir.glob(glob_pattern))
    return sorted(matches)
