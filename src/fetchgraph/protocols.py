from __future__ import annotations

from typing import Any, Dict, List, Optional, Protocol, overload, runtime_checkable

from .models import ProviderInfo, RawLLMOutput


class LLMInvoke(Protocol):
    @overload
    def __call__(self, prompt: str, /, sender: str) -> str: ...

    @overload
    def __call__(self, prompt: str, *, sender: str) -> str: ...

    def __call__(self, *args: Any, **kwargs: Any) -> str: ...


class Verifier(Protocol):
    name: str

    def check(self, output_text: RawLLMOutput) -> List[str]: ...


class Saver(Protocol):
    def save(self, feature_name: str, parsed: Any) -> None: ...


class ContextProvider(Protocol):
    name: str

    def fetch(self, feature_name: str, selectors: Optional[Dict[str, Any]] = None, **kwargs: Any) -> Any: ...

    def serialize(self, obj: Any) -> str: ...


@runtime_checkable
class SupportsFilter(Protocol):
    def filter(self, obj: Any, selectors: Optional[Dict[str, Any]] = None) -> Any: ...


@runtime_checkable
class SupportsDescribe(Protocol):
    def describe(self) -> ProviderInfo: ...


__all__ = [
    "LLMInvoke",
    "Verifier",
    "Saver",
    "ContextProvider",
    "SupportsFilter",
    "SupportsDescribe",
]
