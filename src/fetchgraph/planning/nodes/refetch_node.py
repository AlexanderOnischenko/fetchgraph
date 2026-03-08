"""Refetch/Replan Node (LLM retry).

This node handles LLM retry when self-heal fails:
- Request LLM to regenerate plan with error feedback
- Track retry count and stop conditions
- Cache previous attempts to avoid loops

This is the "escape hatch" when deterministic repair fails.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from .base import NodeContext, NodeResult
from .llm_plan_node import LLMPlanResult

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RefetchResult:
    """Result from refetch/replan."""
    
    # New raw plan text from LLM
    raw_text: Optional[str]
    
    # Whether refetch was attempted
    was_attempted: bool
    
    # Why refetch was skipped (if applicable)
    skip_reason: Optional[str] = None
    
    # Error from LLM (if refetch failed)
    llm_error: Optional[str] = None
    
    # Retry metadata
    attempt_number: int = 0
    max_attempts: int = 3
    
    # Notes
    notes: List[str] = field(default_factory=list)
    
    @property
    def is_success(self) -> bool:
        return self.was_attempted and self.raw_text is not None and self.llm_error is None
    
    @property
    def can_retry_again(self) -> bool:
        return self.attempt_number < self.max_attempts


@dataclass(frozen=True)
class RefetchRequest:
    """Request for LLM refetch."""
    
    # Original prompt/context
    original_prompt: str
    
    # Error feedback to give LLM
    error_feedback: str
    
    # Previous attempts (for context)
    previous_attempts: List[str] = field(default_factory=list)
    
    # Specific guidance for retry
    guidance: Optional[str] = None


class RefetchNode:
    """Refetch/Replan Node (LLM retry).
    
    Responsibilities:
    - Request LLM to regenerate plan with error feedback
    - Track retry count and stop conditions
    - Cache previous attempts to avoid loops
    - Decide when to stop retrying
    
    When to invoke:
    - SelfHealNode returns needs_refetch=True
    - Unfixable errors from any stage
    - Validation fails after max self-heal attempts
    """
    
    def __init__(
        self,
        llm_fn: Optional[Callable[[str], str]] = None,
        max_refetch_attempts: int = 3,
        cache_attempts: bool = True,
        sender: str = "generic_plan",  # Sender for LLMInvoke protocol
    ) -> None:
        self.llm_fn = llm_fn
        self.max_refetch_attempts = max_refetch_attempts
        self.cache_attempts = cache_attempts
        self.sender = sender

        # State
        self._attempt_history: List[str] = []
        self._attempt_count = 0
    
    def execute(
        self,
        ctx: NodeContext,
        request: RefetchRequest,
    ) -> NodeResult[RefetchResult]:
        """Execute refetch/replan.
        
        Args:
            ctx: Pipeline context
            request: Refetch request with error feedback
        
        Returns:
            NodeResult with new raw plan or failure
        """
        logger.info(f"RefetchNode: executing (attempt {self._attempt_count + 1}/{self.max_refetch_attempts})")
        
        self._attempt_count += 1
        
        # Check if we've exceeded max attempts
        if self._attempt_count > self.max_refetch_attempts:
            return NodeResult(
                value=RefetchResult(
                    raw_text=None,
                    was_attempted=False,
                    skip_reason=f"Max refetch attempts ({self.max_refetch_attempts}) exceeded",
                    attempt_number=self._attempt_count,
                    max_attempts=self.max_refetch_attempts,
                    notes=["RefetchNode: max attempts exceeded"],
                ),
                notes=["RefetchNode: max attempts exceeded"],
            )
        
        # CRITICAL FIX #4: Removed loop detection by prompt.
        # Loop detection is now done in orchestrator by response hash.
        # This prevents false positives where same prompt legitimately produces different responses.
        
        # Build retry prompt with error feedback
        retry_prompt = self._build_retry_prompt(request)
        
        # Invoke LLM
        if self.llm_fn is None:
            return NodeResult(
                value=RefetchResult(
                    raw_text=None,
                    was_attempted=False,
                    skip_reason="LLM function not configured",
                    attempt_number=self._attempt_count,
                    max_attempts=self.max_refetch_attempts,
                    notes=["RefetchNode: LLM not configured (stub)"],
                ),
                notes=["RefetchNode: LLM not configured"],
            )
        
        try:
            # Call LLM with sender argument (LLMInvoke protocol requires it)
            raw_text = self.llm_fn(retry_prompt, sender=self.sender)

            # Cache this attempt (for history tracking, not loop detection)
            if self.cache_attempts:
                self._attempt_history.append(retry_prompt)
            
            # Check if LLM returned something useful
            if not raw_text or not raw_text.strip():
                return NodeResult(
                    value=RefetchResult(
                        raw_text=None,
                        was_attempted=True,
                        llm_error="LLM returned empty response",
                        attempt_number=self._attempt_count,
                        max_attempts=self.max_refetch_attempts,
                        notes=["RefetchNode: empty response from LLM"],
                    ),
                    notes=["RefetchNode: empty response"],
                )
            
            return NodeResult(
                value=RefetchResult(
                    raw_text=raw_text,
                    was_attempted=True,
                    attempt_number=self._attempt_count,
                    max_attempts=self.max_refetch_attempts,
                    notes=[f"RefetchNode: received response ({len(raw_text)} chars)"],
                ),
                notes=["RefetchNode: success"],
            )
            
        except Exception as e:
            return NodeResult(
                value=RefetchResult(
                    raw_text=None,
                    was_attempted=True,
                    llm_error=str(e),
                    attempt_number=self._attempt_count,
                    max_attempts=self.max_refetch_attempts,
                    notes=[f"RefetchNode: LLM error: {e}"],
                ),
                notes=[f"RefetchNode: LLM error: {e}"],
            )
    
    def _build_retry_prompt(self, request: RefetchRequest) -> str:
        """Build prompt for LLM retry with error feedback."""
        parts = [
            request.original_prompt,
            "\n\n--- ERROR FEEDBACK ---",
            f"The previous plan had errors: {request.error_feedback}",
        ]
        
        if request.guidance:
            parts.append(f"\n\n--- GUIDANCE FOR RETRY ---\n{request.guidance}")
        
        if request.previous_attempts:
            parts.append("\n\n--- PREVIOUS ATTEMPTS (do not repeat these) ---")
            for i, attempt in enumerate(request.previous_attempts[-3:], 1):
                parts.append(f"\nAttempt {i}: {attempt[:200]}...")
        
        parts.append("\n\nPlease generate a corrected plan that addresses the errors above.")
        
        return "".join(parts)
    
    def reset(self) -> None:
        """Reset state (call between pipeline runs)."""
        self._attempt_history = []
        self._attempt_count = 0
    
    def get_attempt_count(self) -> int:
        """Get current attempt count."""
        return self._attempt_count
