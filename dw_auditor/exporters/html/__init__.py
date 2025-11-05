"""
HTML export functionality for audit reports

This module provides HTML export capabilities with a modular structure:
- assets.py: CSS and JavaScript
- structure.py: Page structure (header, summary, metadata)
- insights.py: Column insights rendering
- checks.py: Quality checks rendering
- export.py: Main orchestration
"""

from .export import export_to_html

__all__ = ['export_to_html']
