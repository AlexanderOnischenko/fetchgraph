import json
import os
import shutil
from datetime import datetime
from pathlib import Path

tag = os.environ["TAG"]
data = Path(os.environ["DATA"])
purge_runs = os.environ.get("PURGE_RUNS","0") in ("1","true","yes","on")
prune_history = os.environ.get("PRUNE_HISTORY","0") in ("1","true","yes","on")
prune_case_history = os.environ.get("PRUNE_CASE_HISTORY","0") in ("1","true","yes","on")
dry = os.environ.get("DRY","0") in ("1","true","yes","on")

artifacts_dir = data / ".runs"
runs_root = artifacts_dir / "runs"

def sanitize(t: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in "-_." else "_" for ch in t)
    return cleaned or "tag"

slug = sanitize(tag)

tag_dir = runs_root / "tags" / slug
tag_markers = [
    runs_root / f"tag-latest-complete-{slug}.txt",
    runs_root / f"tag-latest-results-{slug}.txt",
    runs_root / f"tag-latest-any-{slug}.txt",
    runs_root / f"tag-latest-{slug}.txt",
]

global_markers = {
    "latest_any": runs_root / "latest_any.txt",
    "latest_complete": runs_root / "latest_complete.txt",
    "latest_results": runs_root / "latest_results.txt",
    "latest_legacy": runs_root / "latest.txt",
}

def rm_file(p: Path):
    if not p.exists():
        return
    print("rm", p)
    if not dry:
        p.unlink()

def rm_dir(p: Path):
    if not p.exists():
        return
    print("rm -r", p)
    if not dry:
        shutil.rmtree(p)

def parse_dt(s: str) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z","+00:00"))
    except Exception:
        return None

def iter_run_dirs():
    if not runs_root.exists():
        return
    for p in runs_root.iterdir():
        if not p.is_dir():
            continue
        if p.name == "tags":
            continue
        yield p

print(f"== Deleting tag {tag!r} (slug={slug}) ==")
print(f"artifacts_dir: {artifacts_dir}")
print(f"DRY={dry} PURGE_RUNS={purge_runs} PRUNE_HISTORY={prune_history} PRUNE_CASE_HISTORY={prune_case_history}")

for m in tag_markers:
    rm_file(m)
rm_dir(tag_dir)

deleted_runs: list[Path] = []

if purge_runs and runs_root.exists():
    for run_dir in iter_run_dirs():
        meta = run_dir / "run_meta.json"
        if not meta.exists():
            continue
        try:
            obj = json.loads(meta.read_text(encoding="utf-8"))
        except Exception:
            continue
        if obj.get("tag") == tag:
            deleted_runs.append(run_dir)
            rm_dir(run_dir)

if prune_history:
    hist = artifacts_dir / "history.jsonl"
    if hist.exists():
        print("prune", hist, "(remove entries with tag ==)", tag)
        if not dry:
            tmp = hist.with_suffix(".jsonl.tmp")
            bak = hist.with_suffix(".jsonl.bak")
            with hist.open("r", encoding="utf-8") as r, tmp.open("w", encoding="utf-8") as w:
                for line in r:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                    except Exception:
                        w.write(line + "\n")
                        continue
                    if obj.get("tag") == tag:
                        continue
                    w.write(json.dumps(obj, ensure_ascii=False) + "\n")
            if bak.exists():
                bak.unlink()
            hist.replace(bak)
            tmp.replace(hist)
            print("backup written:", bak)

if prune_case_history:
    cases_dir = runs_root / "cases"
    if cases_dir.exists():
        print("prune case history under", cases_dir)
        if not dry:
            for p in cases_dir.glob("*.jsonl"):
                tmp = p.with_suffix(".jsonl.tmp")
                changed = False
                with p.open("r", encoding="utf-8") as r, tmp.open("w", encoding="utf-8") as w:
                    for line in r:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            obj = json.loads(line)
                        except Exception:
                            w.write(line + "\n")
                            continue
                        if obj.get("tag") == tag:
                            changed = True
                            continue
                        w.write(json.dumps(obj, ensure_ascii=False) + "\n")
                if changed:
                    p.replace(p.with_suffix(".jsonl.bak"))
                    tmp.replace(p)
                else:
                    tmp.unlink(missing_ok=True)

if purge_runs and deleted_runs:
    def pick_latest(require_complete: bool):
        best_dt = None
        best_run = None
        best_results = None
        for rd in iter_run_dirs():
            summ = rd / "summary.json"
            if not summ.exists():
                continue
            try:
                s = json.loads(summ.read_text(encoding="utf-8"))
            except Exception:
                continue
            if require_complete and not s.get("results_complete", False):
                continue
            dt = parse_dt(s.get("ended_at") or s.get("started_at") or "")
            if dt is None:
                continue
            if best_dt is None or dt > best_dt:
                best_dt = dt
                best_run = rd
                rp = s.get("results_path")
                best_results = Path(rp) if rp else (rd / "results.jsonl")
        return best_run, best_results

    any_run, _ = pick_latest(require_complete=False)
    complete_run, complete_results = pick_latest(require_complete=True)

    def write_marker(p: Path, val: Path | None):
        if val is None:
            if p.exists():
                print("rm", p, "(no replacement)")
                if not dry:
                    p.unlink()
            return
        print("write", p, "->", val)
        if not dry:
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(str(val), encoding="utf-8")

    write_marker(global_markers["latest_any"], any_run)
    write_marker(global_markers["latest_complete"], complete_run)
    write_marker(global_markers["latest_legacy"], complete_run)
    write_marker(global_markers["latest_results"], complete_results)

print("== Done ==")