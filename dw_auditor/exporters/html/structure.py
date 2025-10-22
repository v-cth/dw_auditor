"""
HTML page structure components (header, summary, metadata)
"""

from typing import Dict
from .assets import _generate_css_styles, _generate_javascript


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
                <div class="meta-value">{results.get('timestamp', 'N/A')}</div>
            </div>"""

    # Add partition information
    if 'table_metadata' in results:
        if 'partition_column' in results['table_metadata'] and results['table_metadata']['partition_column']:
            partition_col = results['table_metadata']['partition_column']
            partition_type = results['table_metadata'].get('partition_type', '')
            html += f"""
            <div class="meta-block">
                <div class="meta-label">Partitioned By</div>
                <div class="meta-value">{partition_col} ({partition_type})</div>
            </div>"""

        # Add clustering information (BigQuery style)
        if 'clustering_columns' in results['table_metadata'] and results['table_metadata']['clustering_columns']:
            cluster_cols = ', '.join(results['table_metadata']['clustering_columns'])
            html += f"""
            <div class="meta-block">
                <div class="meta-label">Clustered By</div>
                <div class="meta-value">{cluster_cols}</div>
            </div>"""

        # Add clustering information (Snowflake style)
        elif 'clustering_key' in results['table_metadata'] and results['table_metadata']['clustering_key']:
            html += f"""
            <div class="meta-block">
                <div class="meta-label">Clustering Key</div>
                <div class="meta-value">{results['table_metadata']['clustering_key']}</div>
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
    </section>
"""


def _generate_metadata_section(results: Dict) -> str:
    """Generate the metadata tab section with detailed table information"""
    html = """
    <section id="metadata" class="tab-content">
        <div style="background: white; padding: 25px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1);">
            <h2 style="margin-top: 0; color: #1f2937; font-size: 1.3rem;">Table Details</h2>
"""

    # Schema and basic info
    if 'table_metadata' in results:
        metadata = results['table_metadata']

        # Table type
        if 'table_type' in metadata:
            html += f"""
            <div style="margin-bottom: 16px;">
                <span style="color: #6b7280; font-size: 0.9em;">Type:</span>
                <span style="color: #1f2937; font-weight: 500; margin-left: 8px;">{metadata['table_type']}</span>
            </div>
"""

        # Schema
        if 'schema' in metadata:
            html += f"""
            <div style="margin-bottom: 16px;">
                <span style="color: #6b7280; font-size: 0.9em;">Schema:</span>
                <span style="color: #1f2937; font-weight: 500; margin-left: 8px;">{metadata['schema']}</span>
            </div>
"""

        # Partition information
        if 'partition_column' in metadata and metadata['partition_column']:
            partition_type = metadata.get('partition_type', 'UNKNOWN')
            html += f"""
            <div style="margin-bottom: 16px;">
                <span style="color: #6b7280; font-size: 0.9em;">Partitioned By:</span>
                <span style="color: #1f2937; font-weight: 500; margin-left: 8px;">{metadata['partition_column']} ({partition_type})</span>
            </div>
"""

        # Clustering information (BigQuery)
        if 'clustering_columns' in metadata and metadata['clustering_columns']:
            cluster_cols = ', '.join(metadata['clustering_columns'])
            html += f"""
            <div style="margin-bottom: 16px;">
                <span style="color: #6b7280; font-size: 0.9em;">Clustered By:</span>
                <span style="color: #1f2937; font-weight: 500; margin-left: 8px;">{cluster_cols}</span>
            </div>
"""

        # Clustering information (Snowflake)
        if 'clustering_key' in metadata and metadata['clustering_key']:
            html += f"""
            <div style="margin-bottom: 16px;">
                <span style="color: #6b7280; font-size: 0.9em;">Clustering Key:</span>
                <span style="color: #1f2937; font-weight: 500; margin-left: 8px;">{metadata['clustering_key']}</span>
            </div>
"""

        # Primary key
        if 'primary_key_columns' in metadata and metadata['primary_key_columns']:
            pk_cols = ', '.join(metadata['primary_key_columns'])
            html += f"""
            <div style="margin-bottom: 16px;">
                <span style="color: #6b7280; font-size: 0.9em;">Primary Key:</span>
                <span style="color: #1f2937; font-weight: 500; margin-left: 8px;">{pk_cols}</span>
            </div>
"""

    # Audit metadata
    html += f"""
            <div style="margin-top: 24px; padding-top: 16px; border-top: 1px solid #e5e7eb;">
                <h3 style="color: #1f2937; font-size: 1.1rem; margin-bottom: 12px;">Audit Information</h3>
                <div style="margin-bottom: 12px;">
                    <span style="color: #6b7280; font-size: 0.9em;">Generated:</span>
                    <span style="color: #1f2937; font-weight: 500; margin-left: 8px;">{results.get('timestamp', 'N/A')}</span>
                </div>
                <div style="margin-bottom: 12px;">
                    <span style="color: #6b7280; font-size: 0.9em;">Duration:</span>
                    <span style="color: #1f2937; font-weight: 500; margin-left: 8px;">{results.get('duration_seconds', 0):.2f}s</span>
                </div>
                <div style="margin-bottom: 12px;">
                    <span style="color: #6b7280; font-size: 0.9em;">Sampled:</span>
                    <span style="color: #1f2937; font-weight: 500; margin-left: 8px;">{'Yes' if results.get('sampled', False) else 'No'}</span>
                </div>
            </div>
        </div>
    </section>
"""

    return html
