from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

from fetchgraph.core.protocols import LLMInvoke


@dataclass
class CacheEntry:
    hash: str
    sender: str
    prompt: str
    response: str

    def to_json(self) -> str:
        return json.dumps(
            {
                "hash": self.hash,
                "sender": self.sender,
                "prompt": self.prompt,
                "response": self.response,
            },
            ensure_ascii=False,
        )


class LLMCacheWrapper(LLMInvoke):
    def __init__(self, llm: LLMInvoke, *, mode: str, path: Path, namespace: str = ""):
        self.llm = llm
        self.mode = mode
        self.path = path
        self.namespace = namespace or ""
        self._cache: Dict[str, CacheEntry] = {}
        self._load_existing()

    def __call__(self, prompt: str, /, sender: str) -> str:  # type: ignore[override]
        key = self._hash(sender, prompt)
        if self.mode in {"record", "replay"} and key in self._cache:
            return self._cache[key].response
        if self.mode == "replay":
            raise RuntimeError(f"LLM cache miss for sender={sender!r}.")
        response = self.llm(prompt, sender=sender)
        if self.mode == "record":
            entry = CacheEntry(hash=key, sender=sender, prompt=prompt, response=response)
            self._cache[key] = entry
            self._append(entry)
        return response

    def _load_existing(self) -> None:
        if not self.path.exists():
            return
        with self.path.open("r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if not isinstance(obj, dict) or "hash" not in obj or "response" not in obj:
                    continue
                entry = CacheEntry(
                    hash=str(obj["hash"]),
                    sender=str(obj.get("sender", "")),
                    prompt=str(obj.get("prompt", "")),
                    response=str(obj["response"]),
                )
                self._cache[entry.hash] = entry

    def _append(self, entry: CacheEntry) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as f:
            f.write(entry.to_json() + "\n")

    def _hash(self, sender: str, prompt: str) -> str:
        h = hashlib.sha256()
        if self.namespace:
            h.update(self.namespace.encode("utf-8"))
            h.update(b"\n")
        h.update(sender.encode("utf-8"))
        h.update(b"\n")
        h.update(prompt.encode("utf-8"))
        return h.hexdigest()


def apply_llm_cache(llm: LLMInvoke, *, mode: str, path: Optional[Path], namespace: str = "") -> LLMInvoke:
    normalized_mode = mode.lower()
    if normalized_mode not in {"record", "replay", "off"}:
        raise ValueError("llm-cache mode must be one of: record, replay, off.")
    if normalized_mode == "off":
        return llm
    if path is None:
        raise ValueError("Cache file path must be provided when llm-cache is not 'off'.")
    return LLMCacheWrapper(llm, mode=normalized_mode, path=path, namespace=namespace)


__all__ = ["apply_llm_cache", "LLMCacheWrapper", "CacheEntry"]
