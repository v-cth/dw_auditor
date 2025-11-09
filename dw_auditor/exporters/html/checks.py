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
    <div class="summary alert-warning">
        <h3 class="alert-title">Skipped Complex Types</h3>
        <p class="alert-text">
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
        <p><strong>{total_checks}</strong> check(s) performed: <strong class="status-ok">{passed_checks} passed</strong>, <strong class="status-error">{total_checks - passed_checks} failed</strong> ({issue_count} total issues)</p>
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
        <div class="collapsible-header collapsible-section" onclick="toggleCollapse('{issue_id}')">
            <span class="collapse-icon" id="{issue_id}-icon">▼</span>
            <div class="flex flex-between flex-center flex-1">
                <div class="column-name">{col_name}</div>
                <div class="column-type">{col_data['dtype']}</div>
            </div>
        </div>
        <div class="collapsible-content" id="{issue_id}">
        <div class="checks-grid">
            <div>
                <div class="check-metric">Null Values</div>"""

        null_pct = col_data.get('null_pct')
        null_count = col_data.get('null_count')
        null_color_class = 'check-value-error' if (null_pct is not None and null_pct > 10) else 'check-value-neutral'
        null_display = f"{null_count:,} ({null_pct:.1f}%)" if (null_count is not None and null_pct is not None) else "N/A"

        html += f"""
                <div class="check-value {null_color_class}">
                    {null_display}
                </div>
            </div>
            <div>
                <div class="check-metric">Distinct Values</div>"""

        distinct_count = col_data.get('distinct_count')
        distinct_display = f"{distinct_count:,}" if distinct_count is not None and isinstance(distinct_count, int) else distinct_count if distinct_count else "N/A"

        html += f"""
                <div class="check-value check-value-neutral">
                    {distinct_display}
                </div>
            </div>
        </div>
"""

        # Add checks performed section if available
        if 'checks_run' in col_data and col_data['checks_run']:
            html += """
        <div class="checks-performed-section">
            <h4 class="checks-performed-title">Checks Performed:</h4>
            <div class="checks-badges-container">
"""
            for check in col_data['checks_run']:
                badge_class = 'check-badge-pass' if check['status'] == 'PASSED' else 'check-badge-fail'

                html += f"""
                <span class="check-badge {badge_class}">
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
                    pattern_info = f" (pattern: '{issue['pattern']}')" if 'pattern' in issue and issue['pattern'] is not None else ""
                    pct = issue.get('pct')
                    pct_str = f"{pct:.1f}%" if pct is not None else "N/A"
                    html += f"""
            <div class="issue-stats">
                Affected rows: <strong>{issue['count']:,}</strong> ({pct_str}){pattern_info}
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
                if 'threshold' in issue and 'operator' in issue and issue['threshold'] is not None and issue['operator'] is not None:
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
        <div class="alert-success">
            <div class="alert-success-title">All quality checks passed</div>
            <div class="alert-success-text">No data quality issues detected for this column</div>
        </div>
"""

        html += """
        </div>
    </div>
"""

    return html
