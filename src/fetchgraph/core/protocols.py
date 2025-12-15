from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Protocol, overload, runtime_checkable

from .models import ProviderInfo, RawLLMOutput

if TYPE_CHECKING:
    from ..relational.types import SelectorsDict
    from ..relational.models import EntityDescriptor, RelationDescriptor
else:
    SelectorsDict = Dict[str, Any]
    EntityDescriptor = Any
    RelationDescriptor = Any


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

    def fetch(self, feature_name: str, selectors: Optional[SelectorsDict] = None, **kwargs: Any) -> Any:
        """Fetch data for a feature, optionally constrained by selectors.

        The ``selectors`` argument is a strictly JSON-compatible dictionary (see
        ``SelectorsDict``) and is part of the planned contract produced by the
        planner/LLM. Arbitrary Python objects (DataFrames, connections, models,
        etc.) are not allowed in ``selectors``; pass any non-serializable values
        via ``**kwargs`` instead. ``**kwargs`` is reserved for runtime hints or
        options that do not flow through the planner and may contain
        non-serializable objects.
        """

        ...

    def serialize(self, obj: Any) -> str: ...


@runtime_checkable
class SupportsFilter(Protocol):
    def filter(self, obj: Any, selectors: Optional[SelectorsDict] = None) -> Any: ...


@runtime_checkable
class SupportsDescribe(Protocol):
    entities: List[EntityDescriptor]
    relations: List[RelationDescriptor]

    def describe(self) -> ProviderInfo:
        """Describe the provider, including selector expectations.

        ``ProviderInfo.selectors_schema`` (when present) should be a JSON
        Schema (Draft 7 or similar) for the ``selectors`` field. Any
        ``ProviderInfo.examples`` entries should be stringified JSON samples of
        valid ``selectors`` ready to be inserted directly into a plan.
        """

        ...


__all__ = [
    "LLMInvoke",
    "Verifier",
    "Saver",
    "ContextProvider",
    "SupportsFilter",
    "SupportsDescribe",
]
