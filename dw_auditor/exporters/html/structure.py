"""
HTML page structure components (header, summary, metadata)
"""

from html import escape as html_escape
from typing import Dict
from .assets import _generate_css_styles, _generate_javascript
from .helpers import meta_item, section_header, subsection_header, status_badge, info_box, table_row


def _format_bytes(size_bytes: int) -> str:
    """Format bytes to human-readable size"""
    if size_bytes is None:
        return "N/A"

    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PB"


def _generate_header(results: Dict) -> str:
    """Generate the HTML header section"""
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Data Quality Audit Report - {results['table_name']}</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet">
    <style>
{_generate_css_styles()}    </style>
    <script>
{_generate_javascript()}    </script>
</head>
<body>
    <header class="report-header">
        <div class="report-title">Data Quality Audit Report</div>
        <div class="table-name">{results['table_name']}</div>"""

    # Add table type tag
    table_type = "Unknown"
    if 'table_metadata' in results and 'table_type' in results['table_metadata']:
        table_type = results['table_metadata']['table_type']

    html += f"""
        <div class="table-tag">{table_type}</div>

        <div class="meta-line">"""

    # Add generated timestamp
    html += f"""
            <div class="meta-block">
                <div class="meta-label">Generated</div>
                <div class="summary-meta-value">{results.get('timestamp', 'N/A')}</div>
            </div>"""

    # Add partition information
    if 'table_metadata' in results:
        if 'partition_column' in results['table_metadata'] and results['table_metadata']['partition_column']:
            partition_col = results['table_metadata']['partition_column']
            partition_type = results['table_metadata'].get('partition_type', '')
            html += f"""
            <div class="meta-block">
                <div class="meta-label">Partitioned By</div>
                <div class="summary-meta-value">{partition_col} ({partition_type})</div>
            </div>"""

        # Add clustering information (BigQuery style)
        if 'clustering_columns' in results['table_metadata'] and results['table_metadata']['clustering_columns']:
            cluster_cols = ', '.join(results['table_metadata']['clustering_columns'])
            html += f"""
            <div class="meta-block">
                <div class="meta-label">Clustered By</div>
                <div class="summary-meta-value">{cluster_cols}</div>
            </div>"""

        # Add clustering information (Snowflake style)
        elif 'clustering_key' in results['table_metadata'] and results['table_metadata']['clustering_key']:
            html += f"""
            <div class="meta-block">
                <div class="meta-label">Clustering Key</div>
                <div class="summary-meta-value">{results['table_metadata']['clustering_key']}</div>
            </div>"""

    html += """
        </div>
    </header>
"""
    return html


def _generate_metadata_cards(results: Dict, has_issues: bool) -> str:
    """Generate metadata cards section for Summary tab"""
    # Format total rows with commas
    total_rows = f"{results['total_rows']:,}"
    analyzed_rows = f"{results['analyzed_rows']:,}"
    columns_audited = str(len(results['columns'])) if has_issues else 'All'

    # Status with appropriate styling
    if has_issues:
        status_class = 'status-warning'
        status_text = 'Issues Found'
    else:
        status_text = 'Clean'
        status_class = ''

    return f"""
    <section id="summary" class="tab-content active">
        <div class="summary-cards">
            <div class="card">
                <h2>{total_rows}</h2>
                <p>Total Rows</p>
            </div>
            <div class="card">
                <h2>{analyzed_rows}</h2>
                <p>Analyzed Rows</p>
            </div>
            <div class="card">
                <h2>{columns_audited}</h2>
                <p>Columns Audited</p>
            </div>
            <div class="card">
                <h2 class="{status_class}">{status_text}</h2>
                <p>Status</p>
            </div>
        </div>
"""


def _generate_metadata_section(results: Dict) -> str:
    """Generate the metadata tab section with detailed table information"""
    html = """
    <section id="metadata" class="tab-content">
"""

    # Config metadata section (if available)
    if 'config_metadata' in results:
        config_meta = results['config_metadata']
        # Only show section if at least one field is present
        if any(config_meta.values()):
            html += section_header("Audit Configuration", first=True)

            if config_meta.get('project'):
                html += meta_item("Project", config_meta['project'])

            if config_meta.get('description'):
                html += meta_item("Description", config_meta['description'])

            if config_meta.get('version'):
                html += meta_item("Config Version", str(config_meta['version']))

            if config_meta.get('last_modified'):
                html += meta_item("Config Last Modified", config_meta['last_modified'])

            html += '            <div class="divider"></div>\n'

    # Table metadata section
    html += section_header("Table Details")

    # Schema and basic info
    if 'table_metadata' in results:
        metadata = results['table_metadata']

        # Table ID (fully qualified name)
        if 'table_uid' in metadata:
            html += meta_item("Table ID", metadata['table_uid'], mono=True)

        # Table type
        if 'table_type' in metadata:
            html += meta_item("Type", metadata['table_type'])

        # Table description
        if 'description' in metadata and metadata['description']:
            html += meta_item("Description", metadata['description'])

        # Schema
        if 'schema' in metadata:
            html += meta_item("Schema", metadata['schema'])

        # Table size
        if 'size_bytes' in metadata and metadata['size_bytes'] is not None:
            formatted_size = _format_bytes(metadata['size_bytes'])
            html += meta_item("Size", formatted_size)

        # Created at
        if 'created_at' in metadata and metadata['created_at']:
            html += meta_item("Created", metadata['created_at'])

        # Modified at
        if 'modified_at' in metadata and metadata['modified_at']:
            html += meta_item("Last Modified", metadata['modified_at'])

        # Partition information
        if 'partition_column' in metadata and metadata['partition_column']:
            partition_type = metadata.get('partition_type', 'UNKNOWN')
            html += meta_item("Partitioned By", f"{metadata['partition_column']} ({partition_type})")

        # Clustering information (BigQuery)
        if 'clustering_columns' in metadata and metadata['clustering_columns']:
            cluster_cols = ', '.join(metadata['clustering_columns'])
            html += meta_item("Clustered By", cluster_cols)

        # Clustering information (Snowflake)
        if 'clustering_key' in metadata and metadata['clustering_key']:
            html += meta_item("Clustering Key", metadata['clustering_key'])

        # Primary key with source badge
        if 'primary_key_columns' in metadata and metadata['primary_key_columns']:
            pk_cols = ', '.join(metadata['primary_key_columns'])
            pk_source = metadata.get('primary_key_source', 'unknown')

            # Create inline badge for metadata display
            if pk_source == 'user_config':
                badge = ' <span style="background: #6606dc; color: white; padding: 1px 5px; border-radius: 3px; font-size: 10px;">CONFIG</span>'
            elif pk_source == 'information_schema':
                badge = ' <span style="background: #3b82f6; color: white; padding: 1px 5px; border-radius: 3px; font-size: 10px;">DATABASE</span>'
            else:
                badge = ''

            html += meta_item("Primary Key", f"{pk_cols}{badge}")

    # Audit metadata
    html += '            <div class="divider"></div>\n'
    html += '                <h3 class="subsection-title">Audit Information</h3>\n'
    html += meta_item("Generated", results.get('timestamp', 'N/A'))

    # Use phase_timings total if available (more accurate), otherwise fall back to duration_seconds
    duration = sum(results['phase_timings'].values()) if 'phase_timings' in results else results.get('duration_seconds', 0)
    html += meta_item("Duration", f"{duration:.2f}s")
    html += meta_item("Sampled", 'Yes' if results.get('sampled', False) else 'No')

    html += """ 
    </section>
"""

    return html


def _generate_column_summary_table(results: Dict) -> str:
    """Generate the column summary table for the Summary tab"""
    if 'column_summary' not in results or not results['column_summary']:
        return ""

    html = subsection_header("Column Summary", "Basic metrics for all columns in the table")

    # Show primary key information with source badge
    primary_keys = []
    pk_source = None

    # Priority 1: Explicit PKs (config or database metadata)
    if 'table_metadata' in results and 'primary_key_columns' in results['table_metadata']:
        primary_keys = results['table_metadata']['primary_key_columns']
        pk_source = results['table_metadata'].get('primary_key_source', 'unknown')

    # Priority 2: Auto-detected (only if no explicit PK)
    elif 'potential_primary_keys' in results:
        primary_keys = results['potential_primary_keys']
        pk_source = 'auto_detected'

    if primary_keys:
        # Create source badge
        if pk_source == 'user_config':
            badge = '<span style="background: #6606dc; color: white; padding: 2px 6px; border-radius: 3px; font-size: 11px; margin-left: 8px;">CONFIG</span>'
        elif pk_source == 'information_schema':
            badge = '<span style="background: #3b82f6; color: white; padding: 2px 6px; border-radius: 3px; font-size: 11px; margin-left: 8px;">DATABASE</span>'
        elif pk_source == 'auto_detected':
            badge = '<span style="background: #f59e0b; color: #1f2937; padding: 2px 6px; border-radius: 3px; font-size: 11px; margin-left: 8px;">AUTO-DETECTED</span>'
        else:
            badge = ''

        html += info_box(
            f"<strong>Primary Key Column(s):</strong> {', '.join(primary_keys)}{badge}",
            box_type='success'
        )

    html += '    <div class="data-table">\n'
    html += '        <table>\n'
    html += """            <thead>
                <tr>
                    <th>Column Name</th>
                    <th>Data Type</th>
                    <th>Status</th>
                    <th>Null Count</th>
                    <th>Null %</th>
                    <th>Distinct Values</th>
                    <th>Description</th>
                </tr>
            </thead>
            <tbody>
"""

    for col_name, col_data in results['column_summary'].items():
        null_pct = col_data['null_pct']
        null_count = col_data['null_count']
        distinct_count = col_data['distinct_count']
        is_primary_key = col_name in primary_keys
        status = col_data.get('status', 'UNKNOWN')

        # Handle N/A values for unloaded columns
        if null_pct == 'N/A' or null_count == 'N/A':
            null_display = "N/A"
            null_pct_display = "N/A"
            null_pct_numeric = 0
        else:
            null_display = f"{null_count:,}"
            null_pct_display = f"{null_pct:.1f}%"
            null_pct_numeric = null_pct

        # Handle distinct_count
        if distinct_count == 'N/A':
            distinct_display = "N/A"
        elif distinct_count is not None:
            distinct_display = f"{distinct_count:,}"
        else:
            distinct_display = "N/A"

        # Generate status badge
        badge_html = status_badge(status)

        col_name_display = col_name if not is_primary_key else f"{col_name} (PK)"

        # Display database type - show conversion if it happened
        dtype_display = col_data['dtype']
        if 'converted_to' in col_data:
            # Show as "ORIGINAL → converted"
            dtype_display = f"{col_data['dtype']} → {col_data['converted_to']}"

        # Build cells
        bold_class = ' class="td-bold"' if is_primary_key else ''
        error_class = ' class="td-error"' if null_pct_numeric > 10 else ''

        # Get description and escape HTML special characters
        description = col_data.get('description', None)
        description_display = html_escape(description) if description else ''

        html += f"""                <tr>
                    <td{bold_class}>{col_name_display}</td>
                    <td>{dtype_display}</td>
                    <td>{badge_html}</td>
                    <td>{null_display}</td>
                    <td{error_class}>{null_pct_display}</td>
                    <td>{distinct_display}</td>
                    <td>{description_display}</td>
                </tr>
"""

    html += """            </tbody>
        </table>
    </div>
"""

    # Show conversion summary if any columns were converted
    converted_cols = [(col_name, col_data) for col_name, col_data in results['column_summary'].items()
                      if 'converted_to' in col_data]
    if converted_cols:
        html += '    <div style="margin-top: 16px; padding: 12px; background: #fffbeb; border-left: 4px solid #f59e0b; border-radius: 4px;">\n'
        html += '        <div style="font-weight: 600; color: #92400e; margin-bottom: 8px;">💡 Auto-converted columns:</div>\n'
        html += '        <ul style="margin: 0; padding-left: 20px; color: #78350f;">\n'
        for col_name, col_data in converted_cols:
            source = col_data['dtype'].lower()
            converted = col_data['converted_to'].lower()
            html += f'            <li><code>{col_name}</code>: {source} → {converted}</li>\n'
        html += '        </ul>\n'
        html += '    </div>\n'

    return html
