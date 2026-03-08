from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from fnmatch import fnmatchcase

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


def _iter_bucket_case_paths(layout: FixtureLayout) -> list[Path]:
    if not layout.bucket_dir.exists():
        return []
    matches: list[Path] = []
    for case_path in layout.bucket_dir.rglob("*.case.json"):
        try:
            rel = case_path.relative_to(layout.bucket_dir)
        except ValueError:
            continue
        if rel.parts and rel.parts[0] == "resources":
            continue
        matches.append(case_path)
    return sorted(matches)


def _stem_from_case_path(layout: FixtureLayout, case_path: Path) -> str:
    rel = case_path.relative_to(layout.bucket_dir).as_posix()
    return rel.removesuffix(".case.json")


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

    matches: list[Path] = []
    for entry in buckets:
        layout = FixtureLayout(root, entry)
        if name:
            case_path = layout.case_path(name)
            if case_path.exists():
                matches.append(case_path)
            continue

        all_cases = _iter_bucket_case_paths(layout)
        if not pattern:
            matches.extend(all_cases)
            continue

        normalized_pattern = pattern.removesuffix(".case.json")
        for case_path in all_cases:
            stem = _stem_from_case_path(layout, case_path)
            if fnmatchcase(stem, normalized_pattern):
                matches.append(case_path)

    return sorted(matches)
