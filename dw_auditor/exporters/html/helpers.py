"""
HTML component helper functions to replace manual concatenation
"""

from typing import Optional, Dict, List


def meta_item(label: str, value: str, mono: bool = False) -> str:
    """
    Generate a metadata item (label + value pair)

    Args:
        label: Label text
        value: Value text
        mono: Use monospace font for value

    Returns:
        HTML string
    """
    value_class = "meta-value-mono" if mono else "meta-value"
    return f"""
            <div class="meta-item">
                <span class="meta-label">{label}:</span>
                <span class="{value_class}">{value}</span>
            </div>
"""


def status_badge(status: str) -> str:
    """
    Generate a status badge

    Args:
        status: Status string (OK, ERROR, SKIPPED_COMPLEX_TYPE, NOT_LOADED, etc.)

    Returns:
        HTML string with badge
    """
    badge_map = {
        'OK': ('badge-ok', 'OK'),
        'ERROR': ('badge-error', 'Error'),
        'SKIPPED_COMPLEX_TYPE': ('badge-skipped', 'Skipped'),
        'NOT_LOADED': ('badge-not-loaded', 'Not Loaded'),
    }

    badge_class, badge_text = badge_map.get(status, ('badge-na', 'N/A'))
    return f'<span class="badge {badge_class}">{badge_text}</span>'


def info_box(content: str, box_type: str = 'success') -> str:
    """
    Generate an info box

    Args:
        content: HTML content for the box
        box_type: Type of box ('success' or 'info')

    Returns:
        HTML string
    """
    return f'<div class="info-box info-box-{box_type}">{content}</div>\n'


def table_row(cells: List[str], bold_indices: Optional[List[int]] = None,
              error_indices: Optional[List[int]] = None) -> str:
    """
    Generate a table row

    Args:
        cells: List of cell contents
        bold_indices: Indices of cells to make bold (0-based)
        error_indices: Indices of cells to style as errors (0-based)

    Returns:
        HTML string
    """
    bold_indices = bold_indices or []
    error_indices = error_indices or []

    html = "                    <tr>\n"
    for i, cell in enumerate(cells):
        classes = []
        if i in bold_indices:
            classes.append('td-bold')
        if i in error_indices:
            classes.append('td-error')

        class_attr = f' class="{" ".join(classes)}"' if classes else ''
        html += f"                        <td{class_attr}>{cell}</td>\n"

    html += "                    </tr>\n"
    return html


def section_header(text: str, first: bool = False) -> str:
    """
    Generate a section header

    Args:
        text: Header text
        first: Whether this is the first header in a section (no top margin)

    Returns:
        HTML string
    """
    class_name = "section-header-first" if first else "section-header"
    return f'<h2 class="{class_name}">{text}</h2>\n'


def subsection_header(title: str, description: Optional[str] = None) -> str:
    """
    Generate a subsection header with optional description

    Args:
        title: Subsection title
        description: Optional description text

    Returns:
        HTML string
    """
    html = f'<h3 class="subsection-header">{title}</h3>\n'
    if description:
        html += f'<p class="subsection-description">{description}</p>\n'
    return html


def stat_grid(stats: Dict[str, str]) -> str:
    """
    Generate a statistics grid

    Args:
        stats: Dictionary of label -> value

    Returns:
        HTML string
    """
    html = '<div class="stat-grid">\n'
    for label, value in stats.items():
        html += f"""    <div class="stat-item">
        <div class="stat-label">{label}</div>
        <div class="stat-value">{value}</div>
    </div>\n"""
    html += '</div>\n'
    return html


def data_table(headers: List[str], rows: List[List[str]],
               row_formatters: Optional[List[callable]] = None) -> str:
    """
    Generate a data table

    Args:
        headers: List of column headers
        rows: List of row data (each row is a list of cell values)
        row_formatters: Optional list of functions to format each row

    Returns:
        HTML string
    """
    html = '<div class="data-table">\n'
    html += '    <table>\n'

    # Headers
    html += '        <thead>\n            <tr>\n'
    for header in headers:
        html += f'                <th>{header}</th>\n'
    html += '            </tr>\n        </thead>\n'

    # Body
    html += '        <tbody>\n'
    for i, row in enumerate(rows):
        if row_formatters and i < len(row_formatters) and row_formatters[i]:
            html += row_formatters[i](row)
        else:
            html += table_row(row)
    html += '        </tbody>\n'
    html += '    </table>\n'
    html += '</div>\n'

    return html


def insight_section(header: str, content: str) -> str:
    """
    Generate an insight section with header and content

    Args:
        header: Section header text
        content: HTML content

    Returns:
        HTML string
    """
    return f"""<div class="insight-section">
    <h4 class="insight-header">{header}</h4>
    <div class="insight-content">
{content}
    </div>
</div>
"""
