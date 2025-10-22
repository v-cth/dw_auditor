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


def export_run_summary_to_json(all_results: List[Dict], file_path: str = None) -> Dict:
    """
    Export run-level summary to JSON

    Args:
        all_results: List of audit results dictionaries (one per table)
        file_path: Optional path to save JSON file

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

    if file_path:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, default=str)

    return summary


def export_run_summary_to_html(all_results: List[Dict], file_path: str = "summary.html") -> str:
    """
    Export run-level summary to HTML dashboard

    Args:
        all_results: List of audit results dictionaries (one per table)
        file_path: Path to save HTML file

    Returns:
        Path to saved HTML file
    """
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

        # Build row HTML
        table_rows_html += f"""
        <tr>
            <td><a href="{table_name}/audit.html" style="color: #667eea; text-decoration: none; font-weight: 500;">{table_name}</a></td>
            <td>{total_rows:,}</td>
            <td>{analyzed_rows:,}</td>
            <td>{'Yes' if sampled else 'No'}</td>
            <td>{column_count}</td>
            <td>{table_issues}</td>
            <td>{columns_with_issues}</td>
            <td><span style="background: {status_bg}; color: {status_color}; padding: 4px 12px; border-radius: 12px; font-size: 0.85em; font-weight: 600;">{status}</span></td>
            <td>{duration:.2f}s</td>
        </tr>
        """

    # Get run timestamp
    timestamps = [r.get('timestamp', '') for r in all_results if r.get('timestamp')]
    run_timestamp = min(timestamps) if timestamps else datetime.now().isoformat()

    html = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Audit Run Summary</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            line-height: 1.6;
            color: #333;
            max-width: 1400px;
            margin: 0 auto;
            padding: 20px;
            background: #f5f5f5;
        }}
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            border-radius: 10px;
            margin-bottom: 30px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }}
        .header h1 {{
            margin: 0 0 10px 0;
            font-size: 2em;
        }}
        .header p {{
            margin: 5px 0;
            opacity: 0.9;
        }}
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}
        .stat-card {{
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .stat-card h3 {{
            margin: 0 0 5px 0;
            font-size: 0.9em;
            color: #666;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}
        .stat-card .value {{
            font-size: 2em;
            font-weight: bold;
            color: #667eea;
        }}
        .status-ok {{ color: #10b981; }}
        .status-warning {{ color: #f59e0b; }}
        .status-error {{ color: #ef4444; }}
        .table-container {{
            background: white;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            overflow: hidden;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
        }}
        th {{
            background: #f9fafb;
            padding: 12px;
            text-align: left;
            font-weight: 600;
            color: #374151;
            border-bottom: 2px solid #e5e7eb;
        }}
        td {{
            padding: 12px;
            border-bottom: 1px solid #e5e7eb;
        }}
        tr:hover {{
            background: #f9fafb;
        }}
        .footer {{
            margin-top: 30px;
            text-align: center;
            color: #666;
            font-size: 0.9em;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>🔍 Database Audit Run Summary</h1>
        <p>Run Time: {run_timestamp}</p>
        <p>Total Duration: {total_duration:.2f} seconds</p>
    </div>

    <div class="stats-grid">
        <div class="stat-card">
            <h3>Tables Audited</h3>
            <div class="value">{total_tables}</div>
        </div>
        <div class="stat-card">
            <h3>Total Rows Analyzed</h3>
            <div class="value">{total_rows_analyzed:,}</div>
        </div>
        <div class="stat-card">
            <h3>Issues Found</h3>
            <div class="value status-error">{total_issues}</div>
        </div>
        <div class="stat-card status-ok">
            <h3>OK Tables</h3>
            <div class="value">{status_counts['OK']}</div>
        </div>
        <div class="stat-card status-warning">
            <h3>Warning Tables</h3>
            <div class="value">{status_counts['WARNING']}</div>
        </div>
        <div class="stat-card status-error">
            <h3>Error Tables</h3>
            <div class="value">{status_counts['ERROR']}</div>
        </div>
    </div>

    <div class="table-container">
        <table>
            <thead>
                <tr>
                    <th>Table Name</th>
                    <th>Total Rows</th>
                    <th>Analyzed Rows</th>
                    <th>Sampled</th>
                    <th>Columns</th>
                    <th>Issues</th>
                    <th>Cols w/ Issues</th>
                    <th>Status</th>
                    <th>Duration</th>
                </tr>
            </thead>
            <tbody>
                {table_rows_html}
            </tbody>
        </table>
    </div>

    <div class="footer">
        <p>Generated by Database Auditor</p>
    </div>
</body>
</html>
"""

    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(html)

    return file_path
