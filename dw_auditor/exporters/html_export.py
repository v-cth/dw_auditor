"""
Export audit results to HTML report
"""

from typing import Dict


def export_to_html(results: Dict, file_path: str = "audit_report.html") -> str:
    """
    Export audit results to a beautiful HTML report

    Args:
        results: Audit results dictionary
        file_path: Path to save HTML file

    Returns:
        Path to saved HTML file
    """
    has_issues = any(col_data['issues'] for col_data in results['columns'].values())

    html = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Audit Report: {results['table_name']}</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            line-height: 1.6;
            color: #333;
            max-width: 1200px;
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
        .metadata {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}
        .metadata-card {{
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .metadata-card .label {{
            color: #666;
            font-size: 0.9em;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}
        .metadata-card .value {{
            font-size: 1.5em;
            font-weight: bold;
            color: #667eea;
            margin-top: 5px;
        }}
        .summary {{
            background: white;
            padding: 20px;
            border-radius: 8px;
            margin-bottom: 30px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .summary.success {{
            border-left: 4px solid #10b981;
        }}
        .summary.warning {{
            border-left: 4px solid #f59e0b;
        }}
        .column-card {{
            background: white;
            padding: 25px;
            border-radius: 8px;
            margin-bottom: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .column-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 20px;
            padding-bottom: 15px;
            border-bottom: 2px solid #f0f0f0;
        }}
        .column-name {{
            font-size: 1.3em;
            font-weight: bold;
            color: #1f2937;
        }}
        .column-type {{
            background: #e5e7eb;
            padding: 5px 15px;
            border-radius: 20px;
            font-size: 0.9em;
            color: #4b5563;
        }}
        .issue {{
            background: #fef3c7;
            border-left: 4px solid #f59e0b;
            padding: 15px;
            margin: 15px 0;
            border-radius: 4px;
        }}
        .issue-type {{
            font-weight: bold;
            color: #d97706;
            margin-bottom: 8px;
        }}
        .issue-stats {{
            color: #666;
            margin: 5px 0;
        }}
        .suggestion {{
            background: #dbeafe;
            border-left: 3px solid #3b82f6;
            padding: 10px;
            margin-top: 10px;
            border-radius: 3px;
            font-style: italic;
        }}
        .examples {{
            background: #f9fafb;
            padding: 10px;
            margin-top: 10px;
            border-radius: 4px;
            font-family: 'Courier New', monospace;
            font-size: 0.9em;
            overflow-x: auto;
        }}
        .footer {{
            text-align: center;
            color: #666;
            margin-top: 50px;
            padding-top: 20px;
            border-top: 1px solid #ddd;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>📊 Data Quality Audit Report</h1>
        <p style="margin: 5px 0;">Table: <strong>{results['table_name']}</strong></p>"""

    # Add table metadata if available
    if 'table_metadata' in results:
        if 'table_type' in results['table_metadata'] and results['table_metadata']['table_type']:
            table_type = results['table_metadata']['table_type']
            # INFORMATION_SCHEMA.TABLES returns readable types like "BASE TABLE", "VIEW", etc.
            html += f"""
        <p style="margin: 5px 0; opacity: 0.9;">Type: <strong>{table_type}</strong></p>"""

        if 'table_uid' in results['table_metadata']:
            html += f"""
        <p style="margin: 5px 0; opacity: 0.9;">Table UID: <strong>{results['table_metadata']['table_uid']}</strong></p>"""

    html += f"""
        <p style="margin: 5px 0; opacity: 0.9;">Generated: {results.get('timestamp', 'N/A')}</p>
        <p style="margin: 5px 0; opacity: 0.9;">Duration: {results.get('duration_seconds', 0):.2f} seconds</p>
    </div>

    <div class="metadata">
        <div class="metadata-card">
            <div class="label">Total Rows</div>
            <div class="value">{results['total_rows']:,}</div>
        </div>
        <div class="metadata-card">
            <div class="label">Analyzed Rows</div>
            <div class="value">{results['analyzed_rows']:,}</div>
        </div>
        <div class="metadata-card">
            <div class="label">Columns Audited</div>
            <div class="value">{len(results['columns']) if has_issues else 'All'}</div>
        </div>
        <div class="metadata-card">
            <div class="label">Status</div>
            <div class="value">{'⚠️ Issues Found' if has_issues else '✅ Clean'}</div>
        </div>
        <div class="metadata-card">
            <div class="label">Duration</div>
            <div class="value">{results.get('duration_seconds', 0):.2f}s</div>
        </div>
    </div>

    <!-- Column Summary Table -->
"""

    # Add column summary for all columns if available
    if 'column_summary' in results and results['column_summary']:
        html += """
    <div style="background: white; padding: 25px; border-radius: 8px; margin-bottom: 30px; box-shadow: 0 2px 4px rgba(0,0,0,0.1);">
        <h2 style="margin-top: 0; color: #1f2937;">📋 Column Summary</h2>
        <p style="color: #666; margin-bottom: 20px;">Basic metrics for all columns in the table</p>"""

        # Show primary key information if available
        primary_keys = []
        if 'table_metadata' in results and 'primary_key_columns' in results['table_metadata']:
            primary_keys = results['table_metadata']['primary_key_columns']
        elif 'potential_primary_keys' in results:
            primary_keys = results['potential_primary_keys']

        if primary_keys:
            html += f"""
        <div style="background: #ecfdf5; border-left: 4px solid #10b981; padding: 12px 15px; margin-bottom: 15px; border-radius: 4px;">
            <strong>🔑 Primary Key Column(s):</strong> {', '.join(primary_keys)}
        </div>"""

        html += """
        <div style="overflow-x: auto;">
            <table style="width: 100%; border-collapse: collapse; font-size: 0.95em;">
                <thead>
                    <tr style="background: #f3f4f6; border-bottom: 2px solid #e5e7eb;">
                        <th style="padding: 12px; text-align: left; font-weight: 600;">Column Name</th>
                        <th style="padding: 12px; text-align: left; font-weight: 600;">Data Type</th>
                        <th style="padding: 12px; text-align: center; font-weight: 600;">Status</th>
                        <th style="padding: 12px; text-align: right; font-weight: 600;">Null Count</th>
                        <th style="padding: 12px; text-align: right; font-weight: 600;">Null %</th>
                        <th style="padding: 12px; text-align: right; font-weight: 600;">Distinct Values</th>
                    </tr>
                </thead>
                <tbody>
"""
        for col_name, col_data in results['column_summary'].items():
            null_pct = col_data['null_pct']
            is_primary_key = col_name in primary_keys
            status = col_data.get('status', 'UNKNOWN')

            # Determine row color based on status and other factors
            if status == 'ERROR':
                row_color = '#fef2f2'  # Light red for errors
            elif is_primary_key:
                row_color = '#ecfdf5'  # Light green for primary keys
            elif null_pct > 10:
                row_color = '#fef2f2'  # Light red for high nulls
            else:
                row_color = 'white'

            # Status badge styling
            if status == 'OK':
                status_badge = '<span style="background: #dcfce7; color: #166534; padding: 4px 8px; border-radius: 12px; font-size: 0.85em; font-weight: 600;">✓ OK</span>'
            elif status == 'ERROR':
                status_badge = '<span style="background: #fee2e2; color: #991b1b; padding: 4px 8px; border-radius: 12px; font-size: 0.85em; font-weight: 600;">✗ ERROR</span>'
            else:
                status_badge = '<span style="background: #f3f4f6; color: #6b7280; padding: 4px 8px; border-radius: 12px; font-size: 0.85em;">- N/A</span>'

            col_name_display = f"🔑 {col_name}" if is_primary_key else col_name
            html += f"""
                    <tr style="border-bottom: 1px solid #e5e7eb; background: {row_color};">
                        <td style="padding: 10px; font-weight: {'bold' if is_primary_key else '500'};">{col_name_display}</td>
                        <td style="padding: 10px; color: #6b7280;">{col_data['dtype']}</td>
                        <td style="padding: 10px; text-align: center;">{status_badge}</td>
                        <td style="padding: 10px; text-align: right; color: #6b7280;">{col_data['null_count']:,}</td>
                        <td style="padding: 10px; text-align: right; color: {'#dc2626' if null_pct > 10 else '#6b7280'}; font-weight: {'bold' if null_pct > 10 else 'normal'};">{null_pct:.1f}%</td>
                        <td style="padding: 10px; text-align: right; color: #6b7280;">{col_data['distinct_count']:,}</td>
                    </tr>
"""
        html += """
                </tbody>
            </table>
        </div>
    </div>
"""

    if not has_issues:
        html += """
    <div class="summary success">
        <h2>✅ No Data Quality Issues Found</h2>
        <p>All columns passed the audit checks. The data appears to be clean and well-structured.</p>
    </div>
"""
    else:
        issue_count = sum(len(col_data['issues']) for col_data in results['columns'].values())
        html += f"""
    <div class="summary warning">
        <h2>⚠️ Data Quality Issues Detected</h2>
        <p>Found <strong>{issue_count}</strong> issue(s) across <strong>{len(results['columns'])}</strong> column(s). Review details below.</p>
    </div>
"""

        for col_name, col_data in results['columns'].items():
            if not col_data['issues']:
                continue

            html += f"""
    <div class="column-card">
        <div class="column-header">
            <div class="column-name">{col_name}</div>
            <div class="column-type">{col_data['dtype']}</div>
        </div>
        <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin-bottom: 20px; padding: 15px; background: #f9fafb; border-radius: 6px;">
            <div>
                <div style="color: #666; font-size: 0.9em;">Null Values</div>
                <div style="font-size: 1.2em; font-weight: bold; color: {'#ef4444' if col_data['null_pct'] > 10 else '#6b7280'};">
                    {col_data['null_count']:,} ({col_data['null_pct']:.1f}%)
                </div>
            </div>
            <div>
                <div style="color: #666; font-size: 0.9em;">Distinct Values</div>
                <div style="font-size: 1.2em; font-weight: bold; color: #6b7280;">
                    {col_data.get('distinct_count', 'N/A'):,}
                </div>
            </div>
        </div>
"""

            for issue in col_data['issues']:
                issue_type = issue['type'].replace('_', ' ').title()
                html += f"""
        <div class="issue">
            <div class="issue-type">⚠️ {issue_type}</div>
"""

                if 'count' in issue:
                    html += f"""
            <div class="issue-stats">
                Affected rows: <strong>{issue['count']:,}</strong> ({issue.get('pct', 0):.1f}%)
            </div>
"""

                if 'suggestion' in issue and issue['suggestion']:
                    html += f"""
            <div class="suggestion">
                💡 Suggestion: {issue['suggestion']}
            </div>
"""

                if 'examples' in issue:
                    examples_str = str(issue['examples'])[:500]
                    html += f"""
            <div class="examples">
                <strong>Examples:</strong><br>
                {examples_str}
            </div>
"""

                if 'special_chars' in issue:
                    html += f"""
            <div class="examples">
                <strong>Special characters found:</strong> {', '.join(issue['special_chars'])}
            </div>
"""

                html += """
        </div>
"""

            html += """
    </div>
"""

    html += """
    <div class="footer">
        <p>Generated by SecureTableAuditor</p>
    </div>
</body>
</html>
"""

    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f"📄 HTML report saved to: {file_path}")
    return file_path
