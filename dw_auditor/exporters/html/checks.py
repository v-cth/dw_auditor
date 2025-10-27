"""
Quality checks section rendering
"""

from typing import Dict


def _generate_issues_section(results: Dict, has_issues: bool) -> str:
    """Generate the quality checks results section"""
    # Check for skipped complex type columns
    skipped_complex_types = [col for col, data in results.get('column_summary', {}).items()
                             if data.get('status') == 'SKIPPED_COMPLEX_TYPE']

    html = ""
    if skipped_complex_types:
        html += f"""
    <div class="summary" style="background: #fffbeb; border-left: 4px solid #f59e0b;">
        <h3 style="color: #92400e; margin: 0 0 10px 0;">Skipped Complex Types</h3>
        <p style="margin: 0; color: #78350f;">
            {len(skipped_complex_types)} column(s) with complex data types (Struct, List, Array, Binary) were skipped from quality checks:
            <strong>{', '.join(skipped_complex_types)}</strong>
        </p>
    </div>
"""

    # Count total checks and issues
    total_checks = sum(len(col_data.get('checks_run', [])) for col_data in results['columns'].values())
    issue_count = sum(len(col_data['issues']) for col_data in results['columns'].values())
    passed_checks = total_checks - sum(1 for col_data in results['columns'].values()
                                       for check in col_data.get('checks_run', [])
                                       if check['status'] == 'FAILED')

    if not has_issues:
        html += f"""
    <div class="summary success">
        <h2>All Quality Checks Passed</h2>
        <p><strong>{total_checks}</strong> quality check(s) performed across all columns. No data quality issues detected.</p>
    </div>
"""
    else:
        html += f"""
    <div class="summary warning">
        <h2>Quality Check Results</h2>
        <p><strong>{total_checks}</strong> check(s) performed: <strong style="color: #10b981;">{passed_checks} passed</strong>, <strong style="color: #ef4444;">{total_checks - passed_checks} failed</strong> ({issue_count} total issues)</p>
    </div>
"""

    # Display all columns that have checks_run data
    issue_idx = 0
    for col_name, col_data in results['columns'].items():
        # Skip columns without checks
        if 'checks_run' not in col_data or not col_data['checks_run']:
            continue

        issue_id = f"issues-{issue_idx}"
        issue_idx += 1

        # Determine if this column has issues
        has_column_issues = len(col_data.get('issues', [])) > 0

        html += f"""
    <div class="column-card">
        <div class="collapsible-header" onclick="toggleCollapse('{issue_id}')" style="margin-bottom: 15px;">
            <span class="collapse-icon" id="{issue_id}-icon">▼</span>
            <div style="display: flex; justify-content: space-between; align-items: center; flex: 1;">
                <div class="column-name">{col_name}</div>
                <div class="column-type">{col_data['dtype']}</div>
            </div>
        </div>
        <div class="collapsible-content" id="{issue_id}">
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

        # Add checks performed section if available
        if 'checks_run' in col_data and col_data['checks_run']:
            html += """
        <div style="margin-bottom: 20px;">
            <h4 style="color: #4b5563; margin: 0 0 10px 0; font-size: 0.95em;">Checks Performed:</h4>
            <div style="display: flex; flex-wrap: wrap; gap: 8px;">
"""
            for check in col_data['checks_run']:
                if check['status'] == 'PASSED':
                    badge_color = '#dcfce7'
                    text_color = '#166534'
                else:
                    badge_color = '#fee2e2'
                    text_color = '#991b1b'

                html += f"""
                <span style="display: inline-block; background: {badge_color}; color: {text_color}; padding: 6px 12px; border-radius: 16px; font-size: 0.85em; font-weight: 500;">
                    {check['name']} ({check['issues_count']} issues)
                </span>
"""

            html += """
            </div>
        </div>
"""

        # Show results: either issues or success message
        if has_column_issues:
            for issue in col_data['issues']:
                issue_type = issue['type'].replace('_', ' ').title()
                html += f"""
        <div class="issue">
            <div class="issue-type">{issue_type}</div>
"""

                if 'count' in issue:
                    pattern_info = f" (pattern: '{issue['pattern']}')" if 'pattern' in issue else ""
                    html += f"""
            <div class="issue-stats">
                Affected rows: <strong>{issue['count']:,}</strong> ({issue.get('pct', 0):.1f}%){pattern_info}
            </div>
"""

                if 'suggestion' in issue and issue['suggestion']:
                    html += f"""
            <div class="suggestion">
                Suggestion: {issue['suggestion']}
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

                # Display regex pattern details
                if issue['type'] == 'REGEX_PATTERN':
                    mode_label = "Match validation" if issue.get('mode') == 'match' else "Pattern detection"
                    html += f"""
            <div class="issue-stats">
                <strong>Mode:</strong> {mode_label}<br>
                <strong>Pattern:</strong> <code>{issue.get('pattern', 'N/A')}</code>
            </div>
"""
                    if issue.get('description'):
                        html += f"""
            <div class="suggestion">
                {issue['description']}
            </div>
"""

                # Display threshold and operator for numeric range violations
                if 'threshold' in issue and 'operator' in issue:
                    html += f"""
            <div class="issue-stats">
                <strong>Expected:</strong> value {issue['operator']} {issue['threshold']}
            </div>
"""

                html += """
        </div>
"""
        else:
            # All checks passed - show success message
            html += """
        <div style="background: #ecfdf5; border-left: 4px solid #10b981; padding: 15px; border-radius: 6px; margin-top: 15px;">
            <div style="color: #065f46; font-weight: 500;">All quality checks passed</div>
            <div style="color: #047857; font-size: 0.9em; margin-top: 5px;">No data quality issues detected for this column</div>
        </div>
"""

        html += """
        </div>
    </div>
"""

    return html
