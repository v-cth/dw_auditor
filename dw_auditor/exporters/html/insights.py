"""
Column insights rendering (column summary table, string/numeric/datetime insights)
"""

from typing import Dict


def _generate_column_summary_table(results: Dict) -> str:
    """Generate the column summary table"""
    if 'column_summary' not in results or not results['column_summary']:
        return ""

    html = """
    <div style="background: white; padding: 25px; border-radius: 8px; margin-bottom: 30px; box-shadow: 0 2px 4px rgba(0,0,0,0.1);">
        <h2 style="margin-top: 0; color: #1f2937;">Column Summary</h2>
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
            <strong>Primary Key Column(s):</strong> {', '.join(primary_keys)}
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
        null_count = col_data['null_count']
        distinct_count = col_data['distinct_count']
        is_primary_key = col_name in primary_keys
        status = col_data.get('status', 'UNKNOWN')

        # Handle N/A values for unloaded columns
        if null_pct == 'N/A' or null_count == 'N/A':
            null_display = "N/A"
            null_pct_display = "N/A"
            null_pct_numeric = 0  # For comparison purposes, treat N/A as 0
        else:
            null_display = f"{null_count:,}"
            null_pct_display = f"{null_pct:.1f}%"
            null_pct_numeric = null_pct

        # Handle distinct_count (can be 'N/A' or None)
        if distinct_count == 'N/A':
            distinct_display = "N/A"
        elif distinct_count is not None:
            distinct_display = f"{distinct_count:,}"
        else:
            distinct_display = "N/A"

        # Determine row color based on status and other factors
        if status == 'ERROR':
            row_color = '#fef2f2'
        elif is_primary_key:
            row_color = '#ecfdf5'
        elif null_pct_numeric > 10:
            row_color = '#fef2f2'
        else:
            row_color = 'white'

        # Status badge styling
        if status == 'OK':
            status_badge = '<span style="background: #dcfce7; color: #166534; padding: 4px 8px; border-radius: 12px; font-size: 0.85em; font-weight: 600;">OK</span>'
        elif status == 'ERROR':
            status_badge = '<span style="background: #fee2e2; color: #991b1b; padding: 4px 8px; border-radius: 12px; font-size: 0.85em; font-weight: 600;">ERROR</span>'
        elif status == 'SKIPPED_COMPLEX_TYPE':
            status_badge = '<span style="background: #fef3c7; color: #92400e; padding: 4px 8px; border-radius: 12px; font-size: 0.85em; font-weight: 600;">SKIPPED</span>'
        elif status == 'NOT_LOADED':
            status_badge = '<span style="background: #f3f4f6; color: #6b7280; padding: 4px 8px; border-radius: 12px; font-size: 0.85em;">NOT LOADED</span>'
        else:
            status_badge = '<span style="background: #f3f4f6; color: #6b7280; padding: 4px 8px; border-radius: 12px; font-size: 0.85em;">N/A</span>'

        col_name_display = col_name if not is_primary_key else f"{col_name} (PK)"

        # Determine null percentage cell styling
        null_pct_color = '#dc2626' if null_pct_numeric > 10 else '#6b7280'
        null_pct_weight = 'bold' if null_pct_numeric > 10 else 'normal'

        html += f"""
                    <tr style="border-bottom: 1px solid #e5e7eb; background: {row_color};">
                        <td style="padding: 10px; font-weight: {'bold' if is_primary_key else '500'};">{col_name_display}</td>
                        <td style="padding: 10px; color: #6b7280;">{col_data['dtype']}</td>
                        <td style="padding: 10px; text-align: center;">{status_badge}</td>
                        <td style="padding: 10px; text-align: right; color: #6b7280;">{null_display}</td>
                        <td style="padding: 10px; text-align: right; color: {null_pct_color}; font-weight: {null_pct_weight};">{null_pct_display}</td>
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
            <div style="margin-bottom: 10px;">
                <span style="color: #6b7280; font-size: 0.9em; margin-right: 8px;">Length:</span>
"""
        for stat_name, stat_value in stats.items():
            html += f"""<span style="display: inline-block; background: #f3f4f6; color: #4b5563; padding: 4px 12px; border-radius: 16px; font-size: 0.85em; margin-right: 6px;"><span style="color: #9ca3af; text-transform: uppercase; font-size: 0.8em;">{stat_name}:</span> <span style="font-weight: 600; color: #667eea;">{stat_value}</span></span>"""
        html += """
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
        else:
            tz_display = tz_value

        html += f"""
            <div style="margin-bottom: 15px;">
                <div style="background: white; padding: 8px 12px; border-radius: 6px; border: 1px solid #e5e7eb; display: inline-flex; align-items: center; gap: 8px;">
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
        <h2 style="margin-top: 0; color: #1f2937;">Column Insights</h2>
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
                <h3 style="margin: 0; color: #4b5563;">{col_name}</h3>
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
