"""Public retrieval API with explicit per-client configuration."""

import asyncio
import os
from pathlib import Path

from .models import AnswerResult, EvidenceBundle


class FetchGraph:
    """A configured client. Async calls share one event loop per instance."""

    def __init__(self, config: str | Path):
        from .config import load_config
        from .engine import RetrievalEngine

        self._engine = RetrievalEngine(load_config(config))

    async def aretrieve(
        self,
        question: str,
        *,
        source: str | None = None,
        inference_profile: str | None = None,
    ) -> EvidenceBundle:
        return await self._engine.execute(
            question,
            source_id=source,
            inference_profile=inference_profile,
        )

    def retrieve(
        self,
        question: str,
        *,
        source: str | None = None,
        inference_profile: str | None = None,
    ) -> EvidenceBundle:
        self._require_sync_context()
        return asyncio.run(
            self.aretrieve(question, source=source, inference_profile=inference_profile)
        )

    async def aanswer(
        self,
        question: str,
        *,
        source: str | None = None,
        inference_profile: str | None = None,
    ) -> AnswerResult:
        bundle, answer = await self._engine.execute(
            question,
            source_id=source,
            inference_profile=inference_profile,
            synthesize=True,
        )
        return AnswerResult(
            status=answer["status"],
            answer=answer["answer"],
            gaps=answer["gaps"],
            bundle=bundle,
        )

    def answer(
        self,
        question: str,
        *,
        source: str | None = None,
        inference_profile: str | None = None,
    ) -> AnswerResult:
        self._require_sync_context()
        return asyncio.run(
            self.aanswer(question, source=source, inference_profile=inference_profile)
        )

    @staticmethod
    def _require_sync_context():
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return
        raise RuntimeError("Use aretrieve() or aanswer() inside an async event loop.")


def _client(config):
    selected = config or os.environ.get("FETCHGRAPH_CONFIG")
    if selected is None:
        raise ValueError("Provide config=... or set FETCHGRAPH_CONFIG.")
    return FetchGraph(selected)


def retrieve(
    question: str,
    *,
    config: str | Path | None = None,
    source: str | None = None,
    inference_profile: str | None = None,
) -> EvidenceBundle:
    return _client(config).retrieve(
        question, source=source, inference_profile=inference_profile
    )


def answer(
    question: str,
    *,
    config: str | Path | None = None,
    source: str | None = None,
    inference_profile: str | None = None,
) -> AnswerResult:
    return _client(config).answer(
        question, source=source, inference_profile=inference_profile
    )
