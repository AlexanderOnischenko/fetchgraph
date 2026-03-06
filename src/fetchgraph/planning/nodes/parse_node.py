"""Stage 1: Parse raw text → Plan model.

This node extracts JSON from raw text and parses it into a Plan model.
Currently a stub - just passes through the parsed JSON.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from typing import Any, Dict, Optional

from pydantic import BaseModel

from .base import NodeContext, NodeResult

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ParseResult:
    """Result from parsing raw plan text."""
    
    # Extracted JSON
    raw_json: Dict[str, Any]
    
    # Parsed Plan model
    plan: BaseModel
    
    # Parsing metadata
    json_extracted: bool = True
    parse_method: str = "direct"  # direct, regex_extract, etc.


class JsonParser:
    """Extract JSON from raw text."""
    
    def extract(self, raw_text: str) -> Optional[Dict[str, Any]]:
        """Extract JSON from raw text.
        
        Strategy:
        1. Try to parse entire text as JSON
        2. Look for JSON blocks in text (```json ... ```)
        3. Look for first { ... } or [ ... ] block
        
        Returns:
            Extracted JSON dict or None
        """
        # Try direct parse
        try:
            return json.loads(raw_text)
        except json.JSONDecodeError:
            pass
        
        # Try to extract JSON from markdown code blocks
        code_block_pattern = r'```(?:json)?\s*(\{.*?\})\s*```'
        match = re.search(code_block_pattern, raw_text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(1))
            except json.JSONDecodeError:
                pass
        
        # Try to find first JSON object
        brace_pattern = r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}'
        match = re.search(brace_pattern, raw_text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(0))
            except json.JSONDecodeError:
                pass
        
        return None


class PlanParser:
    """Parse extracted JSON into Plan model."""
    
    def __init__(self, plan_model: type[BaseModel]) -> None:
        self.plan_model = plan_model
    
    def parse(self, raw_json: Dict[str, Any]) -> BaseModel:
        """Parse JSON into Plan model.
        
        Currently just validates against the model.
        Future: may do field mapping, coercion, etc.
        """
        return self.plan_model.model_validate(raw_json)


class ParseNode:
    """Node 1: Parse raw text → Plan model.
    
    Responsibilities:
    - Extract JSON from raw text (JsonParser)
    - Parse JSON into Plan model (PlanParser)
    
    Currently a stub that expects already-parsed JSON.
    """
    
    def __init__(self, plan_model: type[BaseModel]) -> None:
        self.plan_model = plan_model
        self.json_parser = JsonParser()
        self.plan_parser = PlanParser(plan_model)
    
    def execute(
        self,
        ctx: NodeContext,
        raw_text: str,
    ) -> NodeResult[ParseResult]:
        """Execute parsing.
        
        Args:
            ctx: Pipeline context
            raw_text: Raw plan text from LLM
        
        Returns:
            NodeResult with parsed Plan
        """
        logger.debug("ParseNode: executing")
        
        # Main path: use JsonParser.extract() to handle markdown blocks, etc.
        raw_json = self.json_parser.extract(raw_text)
        
        if raw_json is None:
            # Fallback: try direct parse
            try:
                raw_json = json.loads(raw_text)
                parse_method = "direct"
            except json.JSONDecodeError as e:
                return NodeResult(
                    error=f"ParseNode: failed to extract JSON - {e}",
                    notes=["ParseNode: JSON extraction failed"],
                )
        else:
            parse_method = "extract"
        
        try:
            plan = self.plan_parser.parse(raw_json)
            
            result = ParseResult(
                raw_json=raw_json,
                plan=plan,
                json_extracted=True,
                parse_method=parse_method,
            )
            
            return NodeResult(
                value=result,
                notes=[f"ParseNode: parsed successfully (method={parse_method})"],
            )
        except Exception as e:
            return NodeResult(
                error=f"ParseNode: failed to parse Plan - {e}",
                notes=["ParseNode: Plan parsing failed"],
            )
