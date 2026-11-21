"""Transport-independent retrieval results."""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue

from fetchgraph.core.models import Plan


class RetrievalSource(BaseModel):
    model_config = ConfigDict(extra="forbid")
    id: str
    kind: str


class Provenance(BaseModel):
    model_config = ConfigDict(extra="forbid")
    source: str
    locator: str = Field(min_length=1, max_length=240)
    selectors: dict[str, JsonValue]
    retrieved_at: str


class RetrievedEvidence(BaseModel):
    model_config = ConfigDict(extra="forbid")
    ref: str = Field(pattern=r"^ev_[0-9a-f]{32}$")
    source: str
    locator: str = Field(min_length=1, max_length=240)
    text: str = Field(min_length=1, max_length=16384)
    data: JsonValue = None
    provenance: Provenance


class RetrievalDiagnostics(BaseModel):
    model_config = ConfigDict(extra="forbid")
    status: Literal["complete", "partial", "insufficient"]
    profile: str
    gaps: list[str] = Field(default_factory=list)
    llm_calls: int


class EvidenceBundle(BaseModel):
    """Executed plan and bounded evidence, without answer synthesis."""

    model_config = ConfigDict(extra="forbid")
    plan: Plan
    evidence: list[RetrievedEvidence] = Field(default_factory=list, max_length=8)
    sources: list[RetrievalSource]
    diagnostics: RetrievalDiagnostics


class AnswerResult(BaseModel):
    model_config = ConfigDict(extra="forbid")
    status: Literal["answered", "partial", "insufficient"]
    answer: str
    gaps: list[str]
    bundle: EvidenceBundle
