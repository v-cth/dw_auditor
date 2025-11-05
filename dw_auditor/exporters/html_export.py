"""
Export audit results to HTML report
"""

import logging
from pathlib import Path
from typing import Optional

try:
    from jinja2 import Environment, FileSystemLoader, select_autoescape
    HAS_JINJA2 = True
except ImportError:
    HAS_JINJA2 = False
    Environment = None  # type: ignore
    FileSystemLoader = None  # type: ignore
    select_autoescape = None  # type: ignore

from .types import AuditResultsDict
from .exceptions import FileExportError, ExporterError
from .utils import validate_file_path, validate_audit_results, DEFAULT_HTML_FILENAME

logger = logging.getLogger(__name__)

# Version for footer
__version__ = "1.0.0"


def export_to_html(
    results: AuditResultsDict,
    file_path: str = DEFAULT_HTML_FILENAME,
    template_name: Optional[str] = None
) -> str:
    """
    Export audit results to a beautiful HTML report using Jinja2 templates

    This function generates a professionally formatted HTML report with automatic
    XSS protection via Jinja2's autoescape feature. The report includes:
    - Summary statistics and metadata
    - Detailed issue breakdown by column
    - Visual styling for easy readability
    - Safe HTML rendering (XSS protected)

    Args:
        results: Audit results dictionary with required structure
        file_path: Path to save HTML file (default: "audit_report.html")
        template_name: Optional custom template name (default: "audit_report.html")

    Returns:
        Absolute path to saved HTML file

    Raises:
        ExporterError: If Jinja2 is not installed
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
        >>> path = export_to_html(results)  # doctest: +SKIP
        >>> 'audit_report.html' in path  # doctest: +SKIP
        True
    """
    # Check if Jinja2 is available
    if not HAS_JINJA2:
        logger.error("Jinja2 is not installed")
        raise ExporterError(
            "Jinja2 is required for HTML export. "
            "Install it with: pip install jinja2"
        )

    # Validate input
    try:
        validated_results = validate_audit_results(results)
    except Exception as e:
        logger.error(f"Results validation failed: {e}")
        raise

    # Validate output path
    try:
        validated_path = validate_file_path(file_path)
    except Exception as e:
        logger.error(f"Path validation failed: {e}")
        raise

    # Setup Jinja2 environment
    template_dir = Path(__file__).parent / "templates"
    if not template_dir.exists():
        logger.error(f"Template directory not found: {template_dir}")
        raise FileExportError(f"Template directory not found: {template_dir}")

    try:
        env = Environment(
            loader=FileSystemLoader(str(template_dir)),
            autoescape=select_autoescape(['html', 'xml']),  # XSS protection
            trim_blocks=True,
            lstrip_blocks=True
        )
    except Exception as e:
        logger.error(f"Failed to initialize Jinja2 environment: {e}")
        raise FileExportError(f"Failed to initialize template engine: {e}") from e

    # Load template
    template_file = template_name or "audit_report.html"
    try:
        template = env.get_template(template_file)
    except Exception as e:
        logger.error(f"Failed to load template '{template_file}': {e}")
        raise FileExportError(f"Failed to load template '{template_file}': {e}") from e

    # Prepare template context
    has_issues = any(col_data['issues'] for col_data in validated_results['columns'].values())
    issue_count = sum(len(col_data['issues']) for col_data in validated_results['columns'].values())

    context = {
        'table_name': validated_results['table_name'],
        'total_rows': validated_results['total_rows'],
        'analyzed_rows': validated_results['analyzed_rows'],
        'sampled': validated_results['sampled'],
        'timestamp': validated_results.get('timestamp', 'N/A'),
        'columns': validated_results['columns'],
        'has_issues': has_issues,
        'issue_count': issue_count,
        'columns_count': len(validated_results['columns']),
        'version': __version__
    }

    # Render template
    try:
        html_content = template.render(**context)
    except Exception as e:
        logger.error(f"Template rendering failed: {e}")
        raise FileExportError(f"Failed to render template: {e}") from e

    # Write HTML file
    try:
        validated_path.write_text(html_content, encoding='utf-8')
        logger.info(f"HTML report exported to: {validated_path}")
        print(f"📄 HTML report saved to: {validated_path}")
    except (OSError, IOError) as e:
        logger.error(f"Failed to write HTML file: {e}")
        raise FileExportError(f"Failed to write file '{file_path}': {e}") from e

    return str(validated_path.resolve())
