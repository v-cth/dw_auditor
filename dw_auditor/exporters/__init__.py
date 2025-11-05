"""
Export functionality for audit results

This module provides multiple export formats for database audit results:
- JSON: Machine-readable format for integration with other tools
- DataFrame: Polars DataFrame for data analysis
- HTML: Beautiful, human-readable reports with XSS protection

All exporters include:
- Input validation
- Error handling
- Path sanitization
- Proper logging
- Type safety
"""

from .dataframe_export import export_to_dataframe
from .json_export import export_to_json
from .html_export import export_to_html

# Export exceptions for error handling
from .exceptions import (
    ExporterError,
    InvalidResultsError,
    FileExportError,
    PathValidationError
)

# Export types for type hints
from .types import (
    AuditResultsDict,
    ColumnDataDict,
    IssueDict
)

__all__ = [
    # Export functions
    "export_to_dataframe",
    "export_to_json",
    "export_to_html",
    # Exceptions
    "ExporterError",
    "InvalidResultsError",
    "FileExportError",
    "PathValidationError",
    # Types
    "AuditResultsDict",
    "ColumnDataDict",
    "IssueDict",
]

__version__ = "1.0.0"
