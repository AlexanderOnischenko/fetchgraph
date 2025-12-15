from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Literal

from pydantic import BaseModel, Field


class RawLLMOutput(BaseModel):
    text: str


class TaskProfile(BaseModel):
    task_name: str = "Generic Task"
    goal: str = "Synthesize output based on fetched context"
    output_format: str = "{}"
    acceptance_criteria: List[str] = Field(default_factory=list)
    constraints: List[str] = Field(default_factory=list)
    focus_hints: List[str] = Field(default_factory=list)
    lite_context_keys: List[str] = Field(default_factory=list)


class SelectorDialectInfo(BaseModel):
    id: str
    description: str = ""
    envelope_example: str = ""
    notes: str = ""
    payload_format: Optional[Literal["json5-string", "json-object"]] = None


class ProviderInfo(BaseModel):
    """Metadata describing a provider and its selector contract.

    - ``selectors_schema`` is a JSON Schema describing the expected ``SelectorsDict``
      structure for ``selectors`` passed to the provider.
    - ``examples`` are stringified JSON examples of valid ``selectors`` payloads.
    """

    name: str
    description: str = ""
    selectors_schema: Dict[str, Any] = Field(default_factory=dict)
    capabilities: List[str] = Field(default_factory=list)
    examples: List[str] = Field(default_factory=list)
    typical_cost: Optional[str] = None
    selector_dialects: List[SelectorDialectInfo] = Field(default_factory=list)
    selectors_digest: Dict[str, Any] = Field(default_factory=dict)
    preferred_selectors: Optional[Literal["dsl", "native", "none"]] = None


class ContextItem(BaseModel):
    key: str
    raw: Any
    text: str
    tokens: int


ProviderType = str


class ContextFetchSpec(BaseModel):
    """Specification for a single provider fetch step.

    ``selectors`` is a JSON-serializable object describing provider-specific
    fetch parameters. Its allowed structure is defined by the provider via
    ``ProviderInfo.selectors_schema``.
    """

    provider: ProviderType
    mode: str = "full"
    selectors: Dict[str, Any] = Field(default_factory=dict)
    max_tokens: Optional[int] = None


@dataclass(frozen=True)
class BaselineSpec:
    spec: ContextFetchSpec
    required: bool = True


class Plan(BaseModel):
    required_context: List[ProviderType] = Field(default_factory=list)
    context_plan: List[ContextFetchSpec] = Field(default_factory=list)
    adr_queries: Optional[List[str]] = None
    constraints: Optional[List[str]] = None
    entities: List[Dict[str, Any]] = Field(default_factory=list)
    dtos: List[Dict[str, Any]] = Field(default_factory=list)


class RefetchDecision(BaseModel):
    add_specs: List[ContextFetchSpec] = Field(default_factory=list)
    stop: bool = True
    notes: Optional[str] = None

__all__ = [
    "RawLLMOutput",
    "TaskProfile",
    "ProviderInfo",
    "SelectorDialectInfo",
    "ContextItem",
    "ProviderType",
    "ContextFetchSpec",
    "BaselineSpec",
    "Plan",
    "RefetchDecision",
]
