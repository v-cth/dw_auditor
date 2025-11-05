"""
Export audit results to JSON format
"""

import json
import logging
from typing import Optional
from pathlib import Path

from .types import AuditResultsDict
from .exceptions import FileExportError, PathValidationError
from .utils import validate_file_path, validate_audit_results, DEFAULT_JSON_FILENAME

logger = logging.getLogger(__name__)


def export_to_json(
    results: AuditResultsDict,
    file_path: Optional[str] = None,
    indent: int = 2,
    ensure_ascii: bool = False
) -> str:
    """
    Export audit results to JSON format

    This function serializes audit results to JSON and optionally saves them to a file.
    All Unicode characters are preserved by default (ensure_ascii=False).

    Args:
        results: Audit results dictionary with required structure
        file_path: Optional path to save JSON file. If None, only returns JSON string
        indent: Number of spaces for indentation (default: 2)
        ensure_ascii: If True, escape non-ASCII characters (default: False)

    Returns:
        JSON string representation of results

    Raises:
        InvalidResultsError: If results dictionary has invalid structure
        PathValidationError: If file_path is invalid or unsafe
        FileExportError: If file write operation fails

    Examples:
        >>> results = {
        ...     'table_name': 'users',
        ...     'total_rows': 1000,
        ...     'analyzed_rows': 1000,
        ...     'sampled': False,
        ...     'columns': {}
        ... }
        >>> json_str = export_to_json(results)
        >>> 'users' in json_str
        True
        >>> export_to_json(results, 'report.json')  # doctest: +SKIP
        '{"table_name": "users", ...}'
    """
    # Validate input
    try:
        validated_results = validate_audit_results(results)
    except Exception as e:
        logger.error(f"Results validation failed: {e}")
        raise

    # Serialize to JSON
    try:
        json_str = json.dumps(
            validated_results,
            indent=indent,
            default=str,
            ensure_ascii=ensure_ascii
        )
    except (TypeError, ValueError) as e:
        logger.error(f"JSON serialization failed: {e}")
        raise FileExportError(f"Failed to serialize results to JSON: {e}") from e

    # Write to file if path provided
    if file_path:
        try:
            # Validate and resolve path
            validated_path = validate_file_path(file_path)

            # Write with explicit UTF-8 encoding
            validated_path.write_text(json_str, encoding='utf-8')

            logger.info(f"JSON results exported to: {validated_path}")
            print(f"📄 Results saved to: {validated_path}")

        except PathValidationError:
            raise
        except (OSError, IOError) as e:
            logger.error(f"Failed to write JSON file: {e}")
            raise FileExportError(f"Failed to write file '{file_path}': {e}") from e

    return json_str
