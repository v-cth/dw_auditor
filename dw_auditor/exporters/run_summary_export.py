"""
Export run-level summary of multiple table audits
"""

from typing import List, Dict
import polars as pl
from pathlib import Path


def export_run_summary_to_dataframe(all_results: List[Dict]) -> pl.DataFrame:
    """
    Export summary of all audited tables to a DataFrame

    Args:
        all_results: List of audit results dictionaries (one per table)

    Returns:
        DataFrame with one row per table showing high-level metrics
    """
    summary_rows = []

    for results in all_results:
        # Count issues
        total_issues = 0
        columns_with_issues = 0

        for col_name, col_data in results.get('columns', {}).items():
            issues = col_data.get('issues', [])
            if issues:
                total_issues += len(issues)
                columns_with_issues += 1

        # Determine status
        if total_issues == 0:
            status = 'OK'
        elif columns_with_issues <= len(results.get('column_summary', {})) // 2:
            status = 'WARNING'
        else:
            status = 'ERROR'

        # Get table metadata
        table_metadata = results.get('table_metadata', {})

        # Extract partition and clustering info
        partition_col = table_metadata.get('partition_column', '')
        partition_type = table_metadata.get('partition_type', '')
        clustering_cols = ', '.join(table_metadata.get('clustering_columns', [])) if 'clustering_columns' in table_metadata else table_metadata.get('clustering_key', '')

        summary_rows.append({
            'table_name': results.get('table_name', 'unknown'),
            'total_rows': results.get('total_rows', 0),
            'sampled': results.get('sampled', False),
            'analyzed_rows': results.get('analyzed_rows', 0),
            'column_count': len(results.get('column_summary', {})),
            'issues_found': total_issues,
            'columns_with_issues': columns_with_issues,
            'status': status,
            'duration_seconds': results.get('duration_seconds', 0.0),
            'audit_timestamp': results.get('timestamp', ''),
            'table_type': table_metadata.get('table_type', ''),
            'created_time': str(table_metadata.get('created_time', '')),
            'partition_column': partition_col,
            'partition_type': partition_type,
            'clustering_columns': clustering_cols
        })

    return pl.DataFrame(summary_rows)


def export_run_summary_to_json(all_results: List[Dict], file_path: str = None, relationships: List[Dict] = None) -> Dict:
    """
    Export run-level summary to JSON

    Args:
        all_results: List of audit results dictionaries (one per table)
        file_path: Optional path to save JSON file
        relationships: Optional list of detected relationships

    Returns:
        Dictionary with run summary
    """
    import json
    from datetime import datetime

    # Calculate run-level metrics
    total_tables = len(all_results)
    total_duration = sum(r.get('duration_seconds', 0) for r in all_results)
    total_rows_analyzed = sum(r.get('analyzed_rows', 0) for r in all_results)
    total_issues = sum(
        sum(len(col_data.get('issues', [])) for col_data in r.get('columns', {}).values())
        for r in all_results
    )

    # Count status breakdown
    status_counts = {'OK': 0, 'WARNING': 0, 'ERROR': 0}

    table_summaries = []
    for results in all_results:
        # Count issues for this table
        table_issues = 0
        columns_with_issues = 0

        for col_name, col_data in results.get('columns', {}).items():
            issues = col_data.get('issues', [])
            if issues:
                table_issues += len(issues)
                columns_with_issues += 1

        # Determine status
        if table_issues == 0:
            status = 'OK'
        elif columns_with_issues <= len(results.get('column_summary', {})) // 2:
            status = 'WARNING'
        else:
            status = 'ERROR'

        status_counts[status] += 1

        # Get table metadata
        table_metadata = results.get('table_metadata', {})

        table_summaries.append({
            'table_name': results.get('table_name', 'unknown'),
            'total_rows': results.get('total_rows', 0),
            'sampled': results.get('sampled', False),
            'analyzed_rows': results.get('analyzed_rows', 0),
            'column_count': len(results.get('column_summary', {})),
            'issues_found': table_issues,
            'columns_with_issues': columns_with_issues,
            'status': status,
            'duration_seconds': results.get('duration_seconds', 0.0),
            'audit_timestamp': results.get('timestamp', ''),
            'table_metadata': table_metadata
        })

    # Get first and last timestamps
    timestamps = [r.get('timestamp', '') for r in all_results if r.get('timestamp')]
    run_start = min(timestamps) if timestamps else ''
    run_end = max(timestamps) if timestamps else ''

    summary = {
        'run_metadata': {
            'total_tables_audited': total_tables,
            'run_start_time': run_start,
            'run_end_time': run_end,
            'total_duration_seconds': round(total_duration, 2),
            'total_rows_analyzed': total_rows_analyzed,
            'total_issues_found': total_issues,
            'status_breakdown': status_counts
        },
        'tables': table_summaries
    }

    # Add relationships if detected
    if relationships:
        summary['relationships'] = relationships
        summary['run_metadata']['total_relationships_found'] = len(relationships)

    if file_path:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, default=str)

    return summary


def export_run_summary_to_html(all_results: List[Dict], file_path: str = "summary.html", relationships: List[Dict] = None) -> str:
    """
    Export run-level summary to HTML dashboard

    Args:
        all_results: List of audit results dictionaries (one per table)
        file_path: Path to save HTML file
        relationships: Optional list of detected relationships

    Returns:
        Path to saved HTML file
    """
    from datetime import datetime
    from .html.assets import _generate_css_styles
    from .html.relationships import generate_relationships_summary_section, generate_standalone_relationships_report

    # Calculate run-level metrics
    total_tables = len(all_results)
    total_duration = sum(r.get('duration_seconds', 0) for r in all_results)
    total_rows_analyzed = sum(r.get('analyzed_rows', 0) for r in all_results)
    total_issues = sum(
        sum(len(col_data.get('issues', [])) for col_data in r.get('columns', {}).values())
        for r in all_results
    )

    # Count status breakdown
    status_counts = {'OK': 0, 'WARNING': 0, 'ERROR': 0}

    # Prepare table rows
    table_rows_html = ""
    for results in all_results:
        table_name = results.get('table_name', 'unknown')
        total_rows = results.get('total_rows', 0)
        analyzed_rows = results.get('analyzed_rows', 0)
        sampled = results.get('sampled', False)
        column_count = len(results.get('column_summary', {}))
        duration = results.get('duration_seconds', 0.0)

        # Count issues for this table
        table_issues = 0
        columns_with_issues = 0

        for col_name, col_data in results.get('columns', {}).items():
            issues = col_data.get('issues', [])
            if issues:
                table_issues += len(issues)
                columns_with_issues += 1

        # Determine status
        if table_issues == 0:
            status = 'OK'
            status_color = '#10b981'
            status_bg = '#d1fae5'
        elif columns_with_issues <= column_count // 2:
            status = 'WARNING'
            status_color = '#f59e0b'
            status_bg = '#fef3c7'
        else:
            status = 'ERROR'
            status_color = '#ef4444'
            status_bg = '#fee2e2'

        status_counts[status] += 1

        # Status badge
        if status == 'OK':
            status_badge = '<span style="font-size: 0.75rem; font-weight: 600; text-transform: uppercase; padding: 2px 8px; border-radius: 999px; background: #dcfce7; color: #166534;">OK</span>'
        elif status == 'WARNING':
            status_badge = '<span style="font-size: 0.75rem; font-weight: 600; text-transform: uppercase; padding: 2px 8px; border-radius: 999px; background: #fef3c7; color: #92400e;">Warning</span>'
        else:
            status_badge = '<span style="font-size: 0.75rem; font-weight: 600; text-transform: uppercase; padding: 2px 8px; border-radius: 999px; background: #fee2e2; color: #b91c1c;">Error</span>'

        # Build row HTML
        table_rows_html += f"""
                    <tr style="border-bottom: 1px solid #f2f2f2;">
                        <td style="padding: 12px 16px; color: #222;"><a href="{table_name}/audit.html" style="color: var(--accent); text-decoration: none; font-weight: 500;">{table_name}</a></td>
                        <td style="padding: 12px 16px; text-align: right; color: #222;">{total_rows:,}</td>
                        <td style="padding: 12px 16px; text-align: right; color: #222;">{analyzed_rows:,}</td>
                        <td style="padding: 12px 16px; color: #222;">{'Yes' if sampled else 'No'}</td>
                        <td style="padding: 12px 16px; text-align: right; color: #222;">{column_count}</td>
                        <td style="padding: 12px 16px; text-align: right; color: #222;">{table_issues}</td>
                        <td style="padding: 12px 16px; text-align: right; color: #222;">{columns_with_issues}</td>
                        <td style="padding: 12px 16px;">{status_badge}</td>
                        <td style="padding: 12px 16px; text-align: right; color: #222;">{duration:.2f}s</td>
                    </tr>
"""

    # Get run timestamp
    timestamps = [r.get('timestamp', '') for r in all_results if r.get('timestamp')]
    run_timestamp = min(timestamps) if timestamps else datetime.now().isoformat()

    # Get shared CSS
    base_styles = _generate_css_styles()

    html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Audit Run Summary</title>
    <style>
        {base_styles}

        /* Summary-specific overrides */
        .summary-cards {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
            gap: 24px;
            margin-top: 32px;
            margin-bottom: 32px;
        }}

        .status-ok {{ color: #10b981; }}
        .status-warning {{ color: #f59e0b; }}
        .status-error {{ color: #ef4444; }}
    </style>
</head>
<body>
    <div class="container">
        <!-- Header -->
        <header>
            <h1 style="font-size: 2rem; font-weight: 700; margin: 0 0 0.5rem 0;">Audit Run Summary</h1>
            <div style="display: flex; gap: 24px; flex-wrap: wrap; font-size: 0.9rem; color: var(--text-muted);">
                <span>Generated: {run_timestamp}</span>
                <span>Duration: {total_duration:.2f}s</span>
            </div>
        </header>

        <!-- Summary Cards -->
        <div class="summary-cards" style="border-top: 1px solid var(--border-light); padding-top: 24px; border-bottom: 1px solid var(--border-light); padding-bottom: 24px;">
            <div class="card">
                <h2>{total_tables}</h2>
                <p>Tables Audited</p>
            </div>
            <div class="card">
                <h2>{total_rows_analyzed:,}</h2>
                <p>Rows Analyzed</p>
            </div>
            <div class="card">
                <h2 class="status-error">{total_issues}</h2>
                <p>Issues Found</p>
            </div>
            <div class="card">
                <h2 class="status-ok">{status_counts['OK']}</h2>
                <p>OK Tables</p>
            </div>
            <div class="card">
                <h2 class="status-warning">{status_counts['WARNING']}</h2>
                <p>Warning Tables</p>
            </div>
            <div class="card">
                <h2 class="status-error">{status_counts['ERROR']}</h2>
                <p>Error Tables</p>
            </div>
        </div>

        <!-- Tables List -->
        <h3 style="font-size: 1.25rem; font-weight: 600; margin-top: 2rem; margin-bottom: 0.25rem; color: #000;">Audited Tables</h3>
        <p style="font-size: 0.9rem; color: #666; margin-bottom: 1rem;">Detailed results for each table</p>

        <div style="background: #fff; border: 1px solid #eee; border-radius: 12px; box-shadow: 0 1px 3px rgba(0, 0, 0, 0.04); overflow: hidden;">
            <table>
                <thead>
                    <tr style="background: #fafafa; border-bottom: 1px solid #eee;">
                        <th style="padding: 12px 16px; text-align: left; font-size: 0.8rem; font-weight: 600; color: #666; text-transform: uppercase;">Table Name</th>
                        <th style="padding: 12px 16px; text-align: right; font-size: 0.8rem; font-weight: 600; color: #666; text-transform: uppercase;">Total Rows</th>
                        <th style="padding: 12px 16px; text-align: right; font-size: 0.8rem; font-weight: 600; color: #666; text-transform: uppercase;">Analyzed</th>
                        <th style="padding: 12px 16px; text-align: left; font-size: 0.8rem; font-weight: 600; color: #666; text-transform: uppercase;">Sampled</th>
                        <th style="padding: 12px 16px; text-align: right; font-size: 0.8rem; font-weight: 600; color: #666; text-transform: uppercase;">Columns</th>
                        <th style="padding: 12px 16px; text-align: right; font-size: 0.8rem; font-weight: 600; color: #666; text-transform: uppercase;">Issues</th>
                        <th style="padding: 12px 16px; text-align: right; font-size: 0.8rem; font-weight: 600; color: #666; text-transform: uppercase;">Cols w/ Issues</th>
                        <th style="padding: 12px 16px; text-align: left; font-size: 0.8rem; font-weight: 600; color: #666; text-transform: uppercase;">Status</th>
                        <th style="padding: 12px 16px; text-align: right; font-size: 0.8rem; font-weight: 600; color: #666; text-transform: uppercase;">Duration</th>
                    </tr>
                </thead>
                <tbody>
{table_rows_html}
                </tbody>
            </table>
        </div>

        <!-- Relationships Section -->
{generate_relationships_summary_section(relationships) if relationships else ''}
    </div>
</body>
</html>
"""

    # Write HTML file
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(html)

    # Generate standalone interactive relationships report if relationships exist
    if relationships:
        # Prepare tables metadata for the interactive report
        tables_metadata = {}
        for result in all_results:
            tables_metadata[result.get('table_name')] = {
                'total_rows': result.get('total_rows', 0),
                'column_count': len(result.get('column_summary', {}))
            }

        # Generate interactive report in same directory as summary
        from pathlib import Path
        summary_path = Path(file_path)
        interactive_path = summary_path.parent / 'relationships_interactive.html'

        generate_standalone_relationships_report(
            relationships=relationships,
            tables_metadata=tables_metadata,
            output_path=str(interactive_path)
        )

    return file_path
