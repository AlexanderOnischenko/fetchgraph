"""Parsing-specific exception types."""


class OutputParserException(Exception):
    """Raised when LLM output cannot be parsed into the expected structure."""
