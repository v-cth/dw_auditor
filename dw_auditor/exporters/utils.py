"""
Utility functions for exporters
"""

import os
import html
import logging
from pathlib import Path
from typing import Any

from .exceptions import PathValidationError, InvalidResultsError
from .types import AuditResultsDict

logger = logging.getLogger(__name__)

# Constants
MAX_FILENAME_LENGTH = 255
DEFAULT_HTML_FILENAME = "audit_report.html"
DEFAULT_JSON_FILENAME = "audit_report.json"
MAX_EXAMPLE_LENGTH = 500
MAX_EXAMPLES_COUNT = 3


def validate_file_path(file_path: str, must_exist: bool = False) -> Path:
    """
    Validate and sanitize file path for export operations

    Args:
        file_path: Path to validate
        must_exist: Whether parent directory must exist

    Returns:
        Validated Path object

    Raises:
        PathValidationError: If path is invalid or unsafe

    Examples:
        >>> validate_file_path("report.html")
        PosixPath('report.html')
        >>> validate_file_path("/tmp/../etc/passwd")  # doctest: +SKIP
        PathValidationError: Path traversal detected
    """
    if not file_path or not isinstance(file_path, str):
        raise PathValidationError(f"File path must be a non-empty string, got: {type(file_path)}")

    # Check filename length
    filename = os.path.basename(file_path)
    if len(filename) > MAX_FILENAME_LENGTH:
        raise PathValidationError(
            f"Filename too long ({len(filename)} chars). Maximum is {MAX_FILENAME_LENGTH}"
        )

    try:
        path = Path(file_path).resolve()
    except (ValueError, OSError) as e:
        raise PathValidationError(f"Invalid file path: {e}")

    # Check for path traversal
    try:
        if must_exist:
            parent = path.parent
            if not parent.exists():
                raise PathValidationError(f"Parent directory does not exist: {parent}")
            if not parent.is_dir():
                raise PathValidationError(f"Parent path is not a directory: {parent}")
    except (PermissionError, OSError) as e:
        raise PathValidationError(f"Cannot access path: {e}")

    # Check for reserved names on Windows
    reserved_names = {'CON', 'PRN', 'AUX', 'NUL', 'COM1', 'COM2', 'COM3', 'COM4',
                     'COM5', 'COM6', 'COM7', 'COM8', 'COM9', 'LPT1', 'LPT2',
                     'LPT3', 'LPT4', 'LPT5', 'LPT6', 'LPT7', 'LPT8', 'LPT9'}
    name_without_ext = path.stem.upper()
    if name_without_ext in reserved_names:
        raise PathValidationError(f"Reserved filename: {filename}")

    return path


def escape_html(text: Any) -> str:
    """
    Escape text for safe HTML output

    Args:
        text: Text to escape (will be converted to string)

    Returns:
        HTML-safe string

    Examples:
        >>> escape_html("<script>alert('xss')</script>")
        '&lt;script&gt;alert(&#x27;xss&#x27;)&lt;/script&gt;'
        >>> escape_html(123)
        '123'
    """
    return html.escape(str(text))


def validate_audit_results(results: Any) -> AuditResultsDict:
    """
    Validate that results dictionary has the expected structure

    Args:
        results: Results dictionary to validate

    Returns:
        Validated results dictionary

    Raises:
        InvalidResultsError: If results structure is invalid

    Examples:
        >>> validate_audit_results({'table_name': 'test', 'total_rows': 100})
        InvalidResultsError: Missing required key: analyzed_rows
    """
    if not isinstance(results, dict):
        raise InvalidResultsError(f"Results must be a dictionary, got: {type(results)}")

    required_keys = ['table_name', 'total_rows', 'analyzed_rows', 'sampled', 'columns']
    missing_keys = [key for key in required_keys if key not in results]

    if missing_keys:
        raise InvalidResultsError(f"Missing required keys: {', '.join(missing_keys)}")

    # Validate types
    if not isinstance(results['table_name'], str):
        raise InvalidResultsError("table_name must be a string")

    if not isinstance(results['total_rows'], int) or results['total_rows'] < 0:
        raise InvalidResultsError("total_rows must be a non-negative integer")

    if not isinstance(results['analyzed_rows'], int) or results['analyzed_rows'] < 0:
        raise InvalidResultsError("analyzed_rows must be a non-negative integer")

    if not isinstance(results['sampled'], bool):
        raise InvalidResultsError("sampled must be a boolean")

    if not isinstance(results['columns'], dict):
        raise InvalidResultsError("columns must be a dictionary")

    # Validate each column structure
    for col_name, col_data in results['columns'].items():
        if not isinstance(col_data, dict):
            raise InvalidResultsError(f"Column data for '{col_name}' must be a dictionary")

        required_col_keys = ['dtype', 'null_count', 'null_pct', 'issues']
        missing_col_keys = [key for key in required_col_keys if key not in col_data]

        if missing_col_keys:
            raise InvalidResultsError(
                f"Column '{col_name}' missing keys: {', '.join(missing_col_keys)}"
            )

        if not isinstance(col_data['issues'], list):
            raise InvalidResultsError(f"Column '{col_name}' issues must be a list")

    return results  # type: ignore


def setup_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """
    Setup logger for exporters

    Args:
        name: Logger name
        level: Logging level

    Returns:
        Configured logger
    """
    logger = logging.getLogger(name)

    # Only add handler if none exists
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(level)

    return logger
