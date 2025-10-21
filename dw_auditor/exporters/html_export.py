"""
Export audit results to HTML report
"""

from typing import Dict


def _generate_css_styles() -> str:
    """Generate CSS styles for the HTML report"""
    return """
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            line-height: 1.6;
            color: #333;
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
            background: #f5f5f5;
        }
        .header {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            border-radius: 10px;
            margin-bottom: 30px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }
        .header h1 {
            margin: 0 0 10px 0;
            font-size: 2em;
        }
        .metadata {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }
        .metadata-card {
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .metadata-card .label {
            color: #666;
            font-size: 0.9em;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        .metadata-card .value {
            font-size: 1.5em;
            font-weight: bold;
            color: #667eea;
            margin-top: 5px;
        }
        .summary {
            background: white;
            padding: 20px;
            border-radius: 8px;
            margin-bottom: 30px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .summary.success {
            border-left: 4px solid #10b981;
        }
        .summary.warning {
            border-left: 4px solid #f59e0b;
        }
        .column-card {
            background: white;
            padding: 25px;
            border-radius: 8px;
            margin-bottom: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .column-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 20px;
            padding-bottom: 15px;
            border-bottom: 2px solid #f0f0f0;
        }
        .column-name {
            font-size: 1.3em;
            font-weight: bold;
            color: #1f2937;
        }
        .column-type {
            background: #e5e7eb;
            padding: 5px 15px;
            border-radius: 20px;
            font-size: 0.9em;
            color: #4b5563;
        }
        .issue {
            background: #fef3c7;
            border-left: 4px solid #f59e0b;
            padding: 15px;
            margin: 15px 0;
            border-radius: 4px;
        }
        .issue-type {
            font-weight: bold;
            color: #d97706;
            margin-bottom: 8px;
        }
        .issue-stats {
            color: #666;
            margin: 5px 0;
        }
        .suggestion {
            background: #dbeafe;
            border-left: 3px solid #3b82f6;
            padding: 10px;
            margin-top: 10px;
            border-radius: 3px;
            font-style: italic;
        }
        .examples {
            background: #f9fafb;
            padding: 10px;
            margin-top: 10px;
            border-radius: 4px;
            font-family: 'Courier New', monospace;
            font-size: 0.9em;
            overflow-x: auto;
        }
        .footer {
            text-align: center;
            color: #666;
            margin-top: 50px;
            padding-top: 20px;
            border-top: 1px solid #ddd;
        }
        .collapsible-header {
            cursor: pointer;
            user-select: none;
            display: flex;
            align-items: center;
            transition: opacity 0.2s;
        }
        .collapsible-header:hover {
            opacity: 0.7;
        }
        .collapse-icon {
            margin-right: 8px;
            font-size: 0.9em;
            transition: transform 0.3s;
            display: inline-block;
            font-weight: bold;
        }
        .collapse-icon.collapsed {
            transform: rotate(-90deg);
        }
        .collapsible-content {
            overflow: hidden;
            transition: max-height 0.3s ease-out, opacity 0.3s ease-out;
        }
        .collapsible-content.collapsed {
            max-height: 0 !important;
            opacity: 0;
        }
"""


def _generate_javascript() -> str:
    """Generate JavaScript for interactive features"""
    return """
        function toggleCollapse(id) {
            const content = document.getElementById(id);
            const icon = document.getElementById(id + '-icon');

            if (content.classList.contains('collapsed')) {
                content.classList.remove('collapsed');
                content.style.maxHeight = content.scrollHeight + 'px';
                icon.classList.remove('collapsed');
            } else {
                content.classList.add('collapsed');
                content.style.maxHeight = '0';
                icon.classList.add('collapsed');
            }
        }

        // Initialize all collapsible sections as collapsed on page load
        window.addEventListener('DOMContentLoaded', function() {
            document.querySelectorAll('.collapsible-content').forEach(function(element) {
                // Start collapsed by default
                element.classList.add('collapsed');
                element.style.maxHeight = '0';
            });

            document.querySelectorAll('.collapse-icon').forEach(function(icon) {
                // Set icons to collapsed state
                icon.classList.add('collapsed');
            });
        });
"""


def _generate_header(results: Dict) -> str:
    """Generate the HTML header section"""
    html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Audit Report: {results['table_name']}</title>
    <style>
{_generate_css_styles()}    </style>
    <script>
{_generate_javascript()}    </script>
</head>
<body>
    <div class="header">
        <h1>📊 Data Quality Audit Report</h1>
        <p style="margin: 5px 0;">Table: <strong>{results['table_name']}</strong></p>"""

    # Add table metadata if available
    if 'table_metadata' in results:
        if 'table_type' in results['table_metadata'] and results['table_metadata']['table_type']:
            table_type = results['table_metadata']['table_type']
            html += f"""
        <p style="margin: 5px 0; opacity: 0.9;">Type: <strong>{table_type}</strong></p>"""

        if 'table_uid' in results['table_metadata']:
            html += f"""
        <p style="margin: 5px 0; opacity: 0.9;">Table UID: <strong>{results['table_metadata']['table_uid']}</strong></p>"""

    html += f"""
        <p style="margin: 5px 0; opacity: 0.9;">Generated: {results.get('timestamp', 'N/A')}</p>
        <p style="margin: 5px 0; opacity: 0.9;">Duration: {results.get('duration_seconds', 0):.2f} seconds</p>
    </div>
"""
    return html


def _generate_metadata_cards(results: Dict, has_issues: bool) -> str:
    """Generate metadata cards section"""
    return f"""
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
"""


def _generate_column_summary_table(results: Dict) -> str:
    """Generate the column summary table"""
    if 'column_summary' not in results or not results['column_summary']:
        return ""

    html = """
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
            row_color = '#fef2f2'
        elif is_primary_key:
            row_color = '#ecfdf5'
        elif null_pct > 10:
            row_color = '#fef2f2'
        else:
            row_color = 'white'

        # Status badge styling
        if status == 'OK':
            status_badge = '<span style="background: #dcfce7; color: #166534; padding: 4px 8px; border-radius: 12px; font-size: 0.85em; font-weight: 600;">✓ OK</span>'
        elif status == 'ERROR':
            status_badge = '<span style="background: #fee2e2; color: #991b1b; padding: 4px 8px; border-radius: 12px; font-size: 0.85em; font-weight: 600;">✗ ERROR</span>'
        elif status == 'SKIPPED_COMPLEX_TYPE':
            status_badge = '<span style="background: #fef3c7; color: #92400e; padding: 4px 8px; border-radius: 12px; font-size: 0.85em; font-weight: 600;">⊘ SKIPPED</span>'
        else:
            status_badge = '<span style="background: #f3f4f6; color: #6b7280; padding: 4px 8px; border-radius: 12px; font-size: 0.85em;">- N/A</span>'

        col_name_display = f"🔑 {col_name}" if is_primary_key else col_name
        distinct_display = f"{col_data['distinct_count']:,}" if col_data['distinct_count'] is not None else "N/A"

        html += f"""
                    <tr style="border-bottom: 1px solid #e5e7eb; background: {row_color};">
                        <td style="padding: 10px; font-weight: {'bold' if is_primary_key else '500'};">{col_name_display}</td>
                        <td style="padding: 10px; color: #6b7280;">{col_data['dtype']}</td>
                        <td style="padding: 10px; text-align: center;">{status_badge}</td>
                        <td style="padding: 10px; text-align: right; color: #6b7280;">{col_data['null_count']:,}</td>
                        <td style="padding: 10px; text-align: right; color: {'#dc2626' if null_pct > 10 else '#6b7280'}; font-weight: {'bold' if null_pct > 10 else 'normal'};">{null_pct:.1f}%</td>
                        <td style="padding: 10px; text-align: right; color: #6b7280;">{distinct_display}</td>
                    </tr>
"""
    html += """
                </tbody>
            </table>
        </div>
    </div>
"""
    return html


def _render_string_insights(insights: Dict) -> str:
    """Render string column insights"""
    html = ""

    # Top values with visual bar chart
    if 'top_values' in insights and insights['top_values']:
        html += """
            <div style="margin-bottom: 15px;">
                <h4 style="margin: 10px 0 8px 0; color: #6b7280; font-size: 0.95em;">Top Values:</h4>
                <div style="background: white; padding: 10px; border-radius: 6px; border: 1px solid #e5e7eb;">
"""
        for item in insights['top_values']:
            value_str = str(item['value'])
            full_value = value_str

            if len(value_str) > 35:
                value_str = value_str[:32] + '...'

            bar_width = item['percentage']

            if item['percentage'] > 50:
                bar_color = '#3b82f6'
            elif item['percentage'] > 20:
                bar_color = '#60a5fa'
            else:
                bar_color = '#93c5fd'

            html += f"""
                    <div style="background: linear-gradient(to right, {bar_color} 0%, {bar_color} {bar_width}%, #f3f4f6 {bar_width}%, #f3f4f6 100%);
                                border-radius: 4px; padding: 6px 10px; margin: 3px 0;
                                font-size: 0.9em; position: relative; overflow: hidden;"
                         title="{full_value}">
                        <span style="font-weight: 500; color: #1f2937;"><code>{value_str}</code></span>
                        <span style="float: right; font-weight: bold; color: #1f2937; margin-left: 8px;">{item['percentage']:.1f}%</span>
                        <span style="float: right; color: #6b7280;">({item['count']:,})</span>
                    </div>
"""
        html += """
                </div>
            </div>
"""

    # Length statistics
    if 'length_stats' in insights:
        stats = insights['length_stats']
        html += """
            <div style="margin-bottom: 15px;">
                <h4 style="margin: 10px 0 8px 0; color: #6b7280; font-size: 0.95em;">String Length Statistics:</h4>
                <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(120px, 1fr)); gap: 10px;">
"""
        for stat_name, stat_value in stats.items():
            html += f"""
                    <div style="background: white; padding: 10px; border-radius: 4px; border: 1px solid #e5e7eb;">
                        <div style="color: #9ca3af; font-size: 0.85em; text-transform: uppercase;">{stat_name}</div>
                        <div style="font-size: 1.2em; font-weight: bold; color: #667eea;">{stat_value}</div>
                    </div>
"""
        html += """
                </div>
            </div>
"""

    return html


def _render_numeric_insights(insights: Dict, thousand_separator: str = ",", decimal_places: int = 1) -> str:
    """Render numeric column insights with visual distribution

    Args:
        insights: Dictionary containing numeric insights
        thousand_separator: Character to use as thousand separator (default: ",")
        decimal_places: Number of decimal places to display (default: 1)
    """
    html = ""

    def format_number(value: float, separator: str = thousand_separator, decimals: int = decimal_places) -> str:
        """Format number with configurable thousand separator and decimal places"""
        # Format with specified decimal places
        formatted = f"{value:.{decimals}f}"
        # Split into integer and decimal parts
        parts = formatted.split('.')
        int_part = parts[0]
        dec_part = parts[1] if len(parts) > 1 else None

        # Handle negative numbers
        int_part = int_part.lstrip('-')
        is_negative = value < 0

        # Add separator every 3 digits from right to left
        int_with_sep = separator.join([int_part[max(0, i-3):i] for i in range(len(int_part), 0, -3)][::-1])

        # Build result
        if decimals > 0 and dec_part:
            result = f"{int_with_sep}.{dec_part}"
        else:
            result = int_with_sep

        return f"-{result}" if is_negative else result

    if 'min' in insights and 'max' in insights:
        min_val = insights['min']
        max_val = insights['max']
        value_range = max_val - min_val if max_val != min_val else 1

        # Get quantile values or use median/mean
        q1 = insights.get('quantiles', {}).get('p25', insights.get('quantiles', {}).get('Q1', None))
        median = insights.get('median', insights.get('quantiles', {}).get('p50', insights.get('quantiles', {}).get('Q2', None)))
        q3 = insights.get('quantiles', {}).get('p75', insights.get('quantiles', {}).get('Q3', None))
        mean = insights.get('mean')

        html += """
            <div style="margin-bottom: 15px;">
                <h4 style="margin: 10px 0 8px 0; color: #6b7280; font-size: 0.95em;">Distribution Range:</h4>
                <div style="background: white; padding: 12px; border-radius: 6px; border: 1px solid #e5e7eb;">
                    <div style="position: relative; height: 70px; margin: 10px 0; overflow: hidden;">
                        <!-- Range bar -->
                        <div style="position: absolute; top: 30px; left: 0; right: 0; height: 8px; background: linear-gradient(to right, #e0e7ff 0%, #c7d2fe 25%, #a5b4fc 50%, #818cf8 75%, #6366f1 100%); border-radius: 4px;"></div>
"""

        # Calculate positions for all stats
        stats_data = []
        if q1 is not None and value_range > 0:
            stats_data.append({'name': 'Q1', 'value': q1, 'pos': ((q1 - min_val) / value_range) * 100,
                             'color': '#6b7280', 'marker': 'line', 'row': 'top'})
        if median is not None and value_range > 0:
            stats_data.append({'name': 'Median', 'value': median, 'pos': ((median - min_val) / value_range) * 100,
                             'color': '#4338ca', 'marker': 'thick_line', 'row': 'top'})
        if q3 is not None and value_range > 0:
            stats_data.append({'name': 'Q3', 'value': q3, 'pos': ((q3 - min_val) / value_range) * 100,
                             'color': '#6b7280', 'marker': 'line', 'row': 'top'})
        if mean is not None and value_range > 0:
            stats_data.append({'name': 'μ', 'value': mean, 'pos': ((mean - min_val) / value_range) * 100,
                             'color': '#f59e0b', 'marker': 'dot', 'row': 'bottom'})

        # Group overlapping labels (within 8% of range)
        OVERLAP_THRESHOLD = 8.0
        label_groups = []
        used_indices = set()

        for i, stat in enumerate(stats_data):
            if i in used_indices:
                continue

            # Find all stats that overlap with this one
            group = [stat]
            used_indices.add(i)

            for j, other_stat in enumerate(stats_data):
                if j <= i or j in used_indices:
                    continue

                # Check if positions are within threshold and on same row
                if stat['row'] == other_stat['row'] and abs(stat['pos'] - other_stat['pos']) < OVERLAP_THRESHOLD:
                    group.append(other_stat)
                    used_indices.add(j)

            label_groups.append(group)

        # Render all visual markers first (adjusted for new bar position at top: 30px)
        for stat in stats_data:
            if stat['marker'] == 'line':
                html += f"""
                        <div style="position: absolute; top: 25px; left: {stat['pos']}%; width: 2px; height: 18px; background: #4f46e5; opacity: 0.7;"></div>
"""
            elif stat['marker'] == 'thick_line':
                html += f"""
                        <div style="position: absolute; top: 25px; left: {stat['pos']}%; width: 3px; height: 18px; background: #4338ca; font-weight: bold;"></div>
"""
            elif stat['marker'] == 'dot':
                html += f"""
                        <div style="position: absolute; top: 28px; left: {stat['pos']}%; width: 8px; height: 8px; background: #f59e0b; border: 2px solid white; border-radius: 50%; transform: translateX(-50%);"></div>
"""

        # Detect horizontal overlaps and assign vertical positions
        # Sort all individual stats and combined groups by position for stacking
        all_labels = []
        for group in label_groups:
            if len(group) == 1:
                stat = group[0]
                all_labels.append({
                    'pos': stat['pos'],
                    'row': stat['row'],
                    'label': f"{stat['name']}: {format_number(stat['value'])}",
                    'color': stat['color'],
                    'is_combined': False
                })
            else:
                # Combined label
                order = {'Q1': 1, 'Median': 2, 'Q3': 3, 'μ': 4}
                group.sort(key=lambda x: order.get(x['name'], 99))
                combined_label = '/'.join([s['name'] for s in group])
                value = group[0]['value']
                avg_pos = sum(s['pos'] for s in group) / len(group)

                if any(s['name'] == 'Median' for s in group):
                    color = '#4338ca'
                elif any(s['name'] == 'μ' for s in group):
                    color = '#f59e0b'
                else:
                    color = '#6b7280'

                all_labels.append({
                    'pos': avg_pos,
                    'row': group[0]['row'],
                    'label': f"{combined_label}: {format_number(value)}",
                    'color': color,
                    'is_combined': True
                })

        # Add Min and Max labels to the stacking system
        all_labels.append({
            'pos': 0,
            'row': 'bottom',
            'label': f"Min: {format_number(min_val)}",
            'color': '#6b7280',
            'is_combined': False,
            'is_edge': True
        })
        all_labels.append({
            'pos': 100,
            'row': 'bottom',
            'label': f"Max: {format_number(max_val)}",
            'color': '#6b7280',
            'is_combined': False,
            'is_edge': True
        })

        # Sort by row and position
        all_labels.sort(key=lambda x: (x['row'], x['pos']))

        # Assign vertical offsets to prevent overlap within each row
        LABEL_WIDTH_ESTIMATE = 20  # Estimate ~20% width per label (increased for safety)

        for row_type in ['top', 'bottom']:
            row_labels = [l for l in all_labels if l['row'] == row_type]

            # Track which vertical offset to use (0, 1, 2...)
            offset_idx = 0
            last_end_pos = -100  # Track where last label ended

            for label in row_labels:
                # Check if this label would overlap with previous
                if label['pos'] - LABEL_WIDTH_ESTIMATE/2 < last_end_pos:
                    # Overlap detected - use next vertical offset
                    offset_idx = 1 if offset_idx == 0 else 0
                else:
                    # No overlap - reset to primary position
                    offset_idx = 0

                label['v_offset'] = offset_idx
                last_end_pos = label['pos'] + LABEL_WIDTH_ESTIMATE/2

        # Render labels with vertical stacking
        for label in all_labels:
            if label['row'] == 'top':
                # Top row: primary at 5px, secondary at 15px (ensures labels stay within container)
                y_pos = '5px' if label['v_offset'] == 0 else '15px'
            else:
                # Bottom row: primary at 55px, secondary at 45px
                y_pos = '55px' if label['v_offset'] == 0 else '45px'

            # Handle edge labels (Min at 0%, Max at 100%) with proper alignment
            if label.get('is_edge'):
                if label['pos'] == 0:
                    # Min label - left aligned
                    html += f"""
                        <div style="position: absolute; top: {y_pos}; left: 0%; font-size: 0.7em; color: {label['color']}; font-weight: 600; white-space: nowrap;">{label['label']}</div>
"""
                else:
                    # Max label - right aligned
                    html += f"""
                        <div style="position: absolute; top: {y_pos}; right: 0%; font-size: 0.7em; color: {label['color']}; font-weight: 600; white-space: nowrap;">{label['label']}</div>
"""
            else:
                # Regular labels - center aligned
                html += f"""
                        <div style="position: absolute; top: {y_pos}; left: {label['pos']}%; transform: translateX(-50%); font-size: 0.7em; color: {label['color']}; font-weight: 600; white-space: nowrap;">{label['label']}</div>
"""

        html += """
                    </div>
"""

        # Additional stats row (std dev)
        if 'std' in insights:
            html += f"""
                    <div style="margin-top: 8px; padding-top: 8px; border-top: 1px solid #f3f4f6; font-size: 0.8em; color: #6b7280; display: flex; gap: 15px;">
                        <span><span style="font-weight: 600;">σ (Std Dev):</span> {insights['std']:.2f}</span>
                    </div>
"""

        html += """
                </div>
            </div>
"""

    # Fallback for incomplete data
    elif 'min' in insights or 'max' in insights or 'mean' in insights:
        html += """
            <div style="margin-bottom: 15px;">
                <h4 style="margin: 10px 0 8px 0; color: #6b7280; font-size: 0.95em;">Numeric Statistics:</h4>
                <div style="background: white; padding: 8px 10px; border-radius: 6px; border: 1px solid #e5e7eb; display: flex; flex-wrap: wrap; gap: 15px; align-items: center;">
"""
        if 'min' in insights:
            html += f"""
                    <div style="display: inline-flex; align-items: baseline; gap: 5px;">
                        <span style="color: #9ca3af; font-size: 0.75em; text-transform: uppercase; font-weight: 600;">Min:</span>
                        <span style="font-size: 0.95em; font-weight: bold; color: #667eea;">{insights['min']:.2f}</span>
                    </div>
"""
        if 'max' in insights:
            html += f"""
                    <div style="display: inline-flex; align-items: baseline; gap: 5px;">
                        <span style="color: #9ca3af; font-size: 0.75em; text-transform: uppercase; font-weight: 600;">Max:</span>
                        <span style="font-size: 0.95em; font-weight: bold; color: #667eea;">{insights['max']:.2f}</span>
                    </div>
"""
        if 'mean' in insights:
            html += f"""
                    <div style="display: inline-flex; align-items: baseline; gap: 5px;">
                        <span style="color: #9ca3af; font-size: 0.75em; text-transform: uppercase; font-weight: 600;">Mean:</span>
                        <span style="font-size: 0.95em; font-weight: bold; color: #667eea;">{insights['mean']:.2f}</span>
                    </div>
"""
        if 'median' in insights:
            html += f"""
                    <div style="display: inline-flex; align-items: baseline; gap: 5px;">
                        <span style="color: #9ca3af; font-size: 0.75em; text-transform: uppercase; font-weight: 600;">Median:</span>
                        <span style="font-size: 0.95em; font-weight: bold; color: #667eea;">{insights['median']:.2f}</span>
                    </div>
"""
        if 'std' in insights:
            html += f"""
                    <div style="display: inline-flex; align-items: baseline; gap: 5px;">
                        <span style="color: #9ca3af; font-size: 0.75em; text-transform: uppercase; font-weight: 600;">Std Dev:</span>
                        <span style="font-size: 0.95em; font-weight: bold; color: #667eea;">{insights['std']:.2f}</span>
                    </div>
"""
        html += """
                </div>
            </div>
"""

    return html


def _render_datetime_insights(insights: Dict) -> str:
    """Render datetime column insights"""
    html = ""

    # Date range - visual timeline
    if 'min_date' in insights and 'max_date' in insights:
        min_date_str = str(insights['min_date'])
        max_date_str = str(insights['max_date'])
        days = insights.get('date_range_days', 0)

        # Format days nicely
        if days > 365:
            duration_text = f"{days:,} days ({days/365:.1f} years)"
        else:
            duration_text = f"{days:,} days"

        html += f"""
            <div style="margin-bottom: 15px;">
                <h4 style="margin: 10px 0 8px 0; color: #6b7280; font-size: 0.95em;">Date Range:</h4>
                <div style="background: white; padding: 12px; border-radius: 6px; border: 1px solid #e5e7eb;">
                    <div style="position: relative; height: 50px; margin: 5px 0; overflow: hidden;">
                        <!-- Timeline bar -->
                        <div style="position: absolute; top: 20px; left: 0; right: 0; height: 8px; background: linear-gradient(to right, #dbeafe 0%, #93c5fd 25%, #60a5fa 50%, #3b82f6 75%, #2563eb 100%); border-radius: 4px;"></div>

                        <!-- Start marker -->
                        <div style="position: absolute; top: 15px; left: 0%; width: 3px; height: 18px; background: #1e40af;"></div>

                        <!-- End marker -->
                        <div style="position: absolute; top: 15px; right: 0%; width: 3px; height: 18px; background: #1e40af;"></div>

                        <!-- Start date label -->
                        <div style="position: absolute; top: 35px; left: 0%; font-size: 0.7em; color: #1e40af; font-weight: 600; white-space: nowrap;">{min_date_str}</div>

                        <!-- Duration label (center) -->
                        <div style="position: absolute; top: 2px; left: 50%; transform: translateX(-50%); font-size: 0.75em; color: #6b7280; font-weight: 600; background: white; padding: 0 8px; border-radius: 3px; border: 1px solid #e5e7eb;">{duration_text}</div>

                        <!-- End date label -->
                        <div style="position: absolute; top: 35px; right: 0%; font-size: 0.7em; color: #1e40af; font-weight: 600; white-space: nowrap;">{max_date_str}</div>
                    </div>
                </div>
            </div>
"""
    elif 'min_date' in insights or 'max_date' in insights:
        # Fallback for incomplete date range
        html += """
            <div style="margin-bottom: 15px;">
                <h4 style="margin: 10px 0 8px 0; color: #6b7280; font-size: 0.95em;">Date Range:</h4>
                <div style="background: white; padding: 10px 12px; border-radius: 6px; border: 1px solid #e5e7eb; font-size: 0.9em;">
"""
        if 'min_date' in insights:
            html += f"""<strong>From:</strong> {insights['min_date']}<br>"""
        if 'max_date' in insights:
            html += f"""<strong>To:</strong> {insights['max_date']}"""
        html += """
                </div>
            </div>
"""

    # Timezone - compact inline display
    if 'timezone' in insights:
        tz_value = insights['timezone']
        if tz_value == "None (timezone-naive)":
            tz_display = "Timezone-naive"
            tz_icon = "🕐"
        else:
            tz_display = tz_value
            tz_icon = "🌍"

        html += f"""
            <div style="margin-bottom: 15px;">
                <div style="background: white; padding: 8px 12px; border-radius: 6px; border: 1px solid #e5e7eb; display: inline-flex; align-items: center; gap: 8px;">
                    <span style="font-size: 1em;">{tz_icon}</span>
                    <span style="color: #6b7280; font-size: 0.8em; font-weight: 600;">Timezone:</span>
                    <span style="color: #1f2937; font-size: 0.85em; font-weight: 500;">{tz_display}</span>
                </div>
            </div>
"""

    # Most common dates
    if 'most_common_dates' in insights and insights['most_common_dates']:
        html += """
            <div style="margin-bottom: 15px;">
                <h4 style="margin: 10px 0 8px 0; color: #6b7280; font-size: 0.95em;">Most Common Dates:</h4>
                <div style="background: white; padding: 10px; border-radius: 6px; border: 1px solid #e5e7eb;">
"""
        for item in insights['most_common_dates']:
            bar_width = item['percentage']

            if item['percentage'] > 50:
                bar_color = '#10b981'
            elif item['percentage'] > 20:
                bar_color = '#34d399'
            else:
                bar_color = '#6ee7b7'

            html += f"""
                    <div style="background: linear-gradient(to right, {bar_color} 0%, {bar_color} {bar_width}%, #f3f4f6 {bar_width}%, #f3f4f6 100%);
                                border-radius: 4px; padding: 6px 10px; margin: 3px 0;
                                font-size: 0.9em; position: relative; overflow: hidden;">
                        <span style="font-weight: 500; color: #1f2937;">{item['date']}</span>
                        <span style="float: right; font-weight: bold; color: #1f2937; margin-left: 8px;">{item['percentage']:.1f}%</span>
                        <span style="float: right; color: #6b7280;">({item['count']:,})</span>
                    </div>
"""
        html += """
                </div>
            </div>
"""

    # Most common days of week
    if 'most_common_days' in insights and insights['most_common_days']:
        html += """
            <div style="margin-bottom: 15px;">
                <h4 style="margin: 10px 0 8px 0; color: #6b7280; font-size: 0.95em;">Most Common Days of Week:</h4>
                <div style="background: white; padding: 10px; border-radius: 6px; border: 1px solid #e5e7eb;">
"""
        for item in insights['most_common_days']:
            bar_width = item['percentage']

            if item['percentage'] > 50:
                bar_color = '#8b5cf6'
            elif item['percentage'] > 20:
                bar_color = '#a78bfa'
            else:
                bar_color = '#c4b5fd'

            html += f"""
                    <div style="background: linear-gradient(to right, {bar_color} 0%, {bar_color} {bar_width}%, #f3f4f6 {bar_width}%, #f3f4f6 100%);
                                border-radius: 4px; padding: 6px 10px; margin: 3px 0;
                                font-size: 0.9em; position: relative; overflow: hidden;">
                        <span style="font-weight: 500; color: #1f2937;">{item['day']}</span>
                        <span style="float: right; font-weight: bold; color: #1f2937; margin-left: 8px;">{item['percentage']:.1f}%</span>
                        <span style="float: right; color: #6b7280;">({item['count']:,})</span>
                    </div>
"""
        html += """
                </div>
            </div>
"""

    # Most common hours
    if 'most_common_hours' in insights and insights['most_common_hours']:
        html += """
            <div style="margin-bottom: 15px;">
                <h4 style="margin: 10px 0 8px 0; color: #6b7280; font-size: 0.95em;">Most Common Hours:</h4>
                <div style="background: white; padding: 10px; border-radius: 6px; border: 1px solid #e5e7eb;">
"""
        for item in insights['most_common_hours']:
            bar_width = item['percentage']

            if item['percentage'] > 50:
                bar_color = '#f59e0b'
            elif item['percentage'] > 20:
                bar_color = '#fbbf24'
            else:
                bar_color = '#fcd34d'

            html += f"""
                    <div style="background: linear-gradient(to right, {bar_color} 0%, {bar_color} {bar_width}%, #f3f4f6 {bar_width}%, #f3f4f6 100%);
                                border-radius: 4px; padding: 6px 10px; margin: 3px 0;
                                font-size: 0.9em; position: relative; overflow: hidden;">
                        <span style="font-weight: 500; color: #1f2937;">{item['hour']:02d}:00</span>
                        <span style="float: right; font-weight: bold; color: #1f2937; margin-left: 8px;">{item['percentage']:.1f}%</span>
                        <span style="float: right; color: #6b7280;">({item['count']:,})</span>
                    </div>
"""
        html += """
                </div>
            </div>
"""

    return html


def _generate_column_insights(results: Dict, thousand_separator: str = ",", decimal_places: int = 1) -> str:
    """Generate the column insights section

    Args:
        results: Audit results dictionary
        thousand_separator: Character to use as thousand separator (default: ",")
        decimal_places: Number of decimal places to display (default: 1)
    """
    if 'column_insights' not in results or not results['column_insights']:
        return ""

    html = """
    <div style="background: white; padding: 25px; border-radius: 8px; margin-bottom: 30px; box-shadow: 0 2px 4px rgba(0,0,0,0.1);">
        <h2 style="margin-top: 0; color: #1f2937;">💡 Column Insights</h2>
        <p style="color: #666; margin-bottom: 20px;">Data profiling and distribution analysis</p>
"""

    col_idx = 0
    for col_name, insights in results['column_insights'].items():
        col_id = f"insights-{col_idx}"
        col_idx += 1

        html += f"""
        <div style="background: #f9fafb; border-left: 4px solid #667eea; padding: 20px; margin-bottom: 20px; border-radius: 6px;">
            <div class="collapsible-header" onclick="toggleCollapse('{col_id}')">
                <span class="collapse-icon" id="{col_id}-icon">▼</span>
                <h3 style="margin: 0; color: #4b5563;">📊 {col_name}</h3>
            </div>
            <div class="collapsible-content" id="{col_id}">
"""

        # Render different insight types
        html += _render_string_insights(insights)
        html += _render_numeric_insights(insights, thousand_separator, decimal_places)
        html += _render_datetime_insights(insights)

        html += """
            </div>
        </div>
"""

    html += """
    </div>
"""
    return html


def _generate_issues_section(results: Dict, has_issues: bool) -> str:
    """Generate the data quality issues section"""
    # Check for skipped complex type columns
    skipped_complex_types = [col for col, data in results.get('column_summary', {}).items()
                             if data.get('status') == 'SKIPPED_COMPLEX_TYPE']

    html = ""
    if skipped_complex_types:
        html += f"""
    <div class="summary" style="background: #fffbeb; border-left: 4px solid #f59e0b;">
        <h3 style="color: #92400e; margin: 0 0 10px 0;">ℹ️ Skipped Complex Types</h3>
        <p style="margin: 0; color: #78350f;">
            {len(skipped_complex_types)} column(s) with complex data types (Struct, List, Array, Binary) were skipped from quality checks:
            <strong>{', '.join(skipped_complex_types)}</strong>
        </p>
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

        issue_idx = 0
        for col_name, col_data in results['columns'].items():
            if not col_data['issues']:
                continue

            issue_id = f"issues-{issue_idx}"
            issue_idx += 1

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
    </div>
"""

    return html


def export_to_html(results: Dict, file_path: str = "audit_report.html", thousand_separator: str = ",", decimal_places: int = 1) -> str:
    """
    Export audit results to a beautiful HTML report

    Args:
        results: Audit results dictionary
        file_path: Path to save HTML file
        thousand_separator: Separator for thousands (default: ",")
        decimal_places: Number of decimal places to display (default: 1)

    Returns:
        Path to saved HTML file
    """
    has_issues = any(col_data['issues'] for col_data in results['columns'].values())

    # Assemble the HTML report from components
    html = _generate_header(results)
    html += _generate_metadata_cards(results, has_issues)
    html += _generate_column_summary_table(results)
    html += _generate_column_insights(results, thousand_separator, decimal_places)
    html += _generate_issues_section(results, has_issues)

    # Footer
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
