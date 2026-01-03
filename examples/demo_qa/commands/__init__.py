"""Lightweight command entrypoints for demo QA CLI."""

from .history import handle_history_case
from .report import handle_report_run, handle_report_tag

__all__ = ["handle_history_case", "handle_report_run", "handle_report_tag"]
