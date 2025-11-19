"""
Column insights rendering (string/numeric/datetime insights for the Insights tab)
"""

from typing import Dict, List, Any


def _insights_to_dict(insights: List[Dict]) -> Dict:
    """Convert List[dict] to Dict for easy lookup

    Args:
        insights: List of insight dictionaries (serialized from InsightResult objects)

    Returns:
        Dictionary with insight types as keys and values
    """
    result = {}

    for insight in insights:
        insight_type = insight['type']
        value = insight['value']

        # Handle different insight types
        if insight_type in ['top_values', 'boolean_distribution', 'most_common_dates', 'most_common_hours', 'most_common_days', 'histogram']:
            # Lists of dicts
            result[insight_type] = value

        elif insight_type == 'quantiles':
            # Dict like {'p25': 1.0, 'p50': 2.0, 'p75': 3.0}
            result['quantiles'] = value

        elif insight_type in ['min_length', 'max_length', 'avg_length']:
            # Length stats - group under length_stats key
            if 'length_stats' not in result:
                result['length_stats'] = {}
            stat_name = insight_type.replace('_length', '')
            result['length_stats'][stat_name] = value

        else:
            # Simple scalar values (min, max, mean, std, median, min_date, max_date, date_range_days, timezone, etc.)
            result[insight_type] = value

    return result





def _render_string_insights(insights: List[Any]) -> str:
    """Render string column insights"""
    insights_dict = _insights_to_dict(insights)
    html = ""

    # Top values with visual bar chart
    if 'top_values' in insights_dict and insights_dict['top_values']:
        html += """
            <div class="insight-section">
                <h4 class="insight-header">Top Values:</h4>
                <div class="insight-content">
"""
        for item in insights_dict['top_values']:
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
                    <div class="top-value-item" style="background: linear-gradient(to right, {bar_color} 0%, {bar_color} {bar_width}%, #f3f4f6 {bar_width}%, #f3f4f6 100%);" title="{full_value}">
                        <span class="top-value-label"><code>{value_str}</code></span>
                        <div class="top-value-stats">
                            <span class="top-value-count">({item['count']:,})</span>
                            <span class="top-value-pct">{item['percentage']:.1f}%</span>
                        </div>
                    </div>
"""
        html += """
                </div>
            </div>
"""

    # Length statistics
    if 'length_stats' in insights_dict:
        stats = insights_dict['length_stats']
        html += """
            <div class="mb-10">
                <span class="text-muted text-sm" style="margin-right: 8px;">Length:</span>
"""
        for stat_name, stat_value in stats.items():
            html += f"""<span class="stat-pill"><span class="stat-pill-label">{stat_name}:</span> <span class="stat-pill-value">{stat_value}</span></span>"""
        html += """
            </div>
"""

    return html


def _render_numeric_insights(insights: List[Any], thousand_separator: str = ",", decimal_places: int = 1) -> str:
    """Render numeric column insights with visual distribution

    Args:
        insights: List of InsightResult objects
        thousand_separator: Character to use as thousand separator (default: ",")
        decimal_places: Number of decimal places to display (default: 1)
    """
    insights_dict = _insights_to_dict(insights)
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

    # Check if we have any numeric insights to display
    has_stats = any(key in insights_dict for key in ['min', 'max', 'mean', 'median', 'std', 'quantiles'])

    if has_stats:
        min_val = insights_dict.get('min')
        max_val = insights_dict.get('max')
        mean = insights_dict.get('mean')
        median = insights_dict.get('median')
        std = insights_dict.get('std')

        # Get quantile values
        quantiles = insights_dict.get('quantiles', {})
        q1 = quantiles.get('p25')
        if median is None:
            median = quantiles.get('p50')
        q3 = quantiles.get('p75')

        # Track whether min/max were explicitly provided (vs derived)
        has_explicit_min = min_val is not None
        has_explicit_max = max_val is not None

        # If we don't have explicit min/max but have quantiles, derive range for positioning only
        if (min_val is None or max_val is None) and 'quantiles' in insights_dict and insights_dict['quantiles']:
            quantile_values = list(insights_dict['quantiles'].values())
            if min_val is None:
                min_val = min(quantile_values)
            if max_val is None:
                max_val = max(quantile_values)

        # Show visual distribution if we have a valid range
        if min_val is not None and max_val is not None:
            value_range = max_val - min_val if max_val != min_val else 1

            html += """
            <div class="insight-section">
                <h4 class="insight-header">Distribution Range:</h4>
                <div class="insight-content p-12">
                    <div class="distribution-container">
                        <!-- Range bar -->
                        <div class="distribution-gradient"></div>
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

            # Render all visual markers first (adjusted for new bar position at top: 30px)
            for stat in stats_data:
                if stat['marker'] == 'line':
                    html += f"""
                        <div class="distribution-marker" style="left: {stat['pos']}%;"></div>
"""
                elif stat['marker'] == 'thick_line':
                    html += f"""
                        <div class="distribution-marker-bold" style="left: {stat['pos']}%;"></div>
"""
                elif stat['marker'] == 'dot':
                    html += f"""
                        <div class="distribution-marker-mean" style="left: {stat['pos']}%;"></div>
"""

            # Create labels for all stats (no grouping, just individual labels)
            all_labels = []
            for stat in stats_data:
                all_labels.append({
                    'pos': stat['pos'],
                    'row': stat['row'],
                    'label': f"{stat['name']}: {format_number(stat['value'])}",
                    'color': stat['color'],
                    'is_combined': False
                })

            # Add Min and Max labels only if they were explicitly provided in config
            if has_explicit_min:
                all_labels.append({
                    'pos': 0,
                    'row': 'bottom',
                    'label': f"Min: {format_number(min_val)}",
                    'color': '#6b7280',
                    'is_combined': False,
                    'is_edge': True
                })
            if has_explicit_max:
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
                        <div class="distribution-label distribution-label-left" style="top: {y_pos}; color: {label['color']};">{label['label']}</div>
"""
                    else:
                        # Max label - right aligned
                        html += f"""
                        <div class="distribution-label distribution-label-right" style="top: {y_pos}; color: {label['color']};">{label['label']}</div>
"""
                else:
                    # Regular labels - center aligned
                    html += f"""
                        <div class="distribution-label" style="top: {y_pos}; left: {label['pos']}%; transform: translateX(-50%); color: {label['color']};">{label['label']}</div>
"""

            html += """
                    </div>
"""

            # Additional stats row (std dev)
            if std is not None:
                html += f"""
                    <div class="std-footer">
                        <span><span class="text-bold">σ (Std Dev):</span> {std:.2f}</span>
                    </div>
"""

            html += """
                </div>
            </div>
"""

    # Histogram visualization
    if 'histogram' in insights_dict and insights_dict['histogram']:
        buckets = insights_dict['histogram']
        max_count = max(bucket['count'] for bucket in buckets) if buckets else 1

        html += """
            <div class="insight-section" style="margin-top: 20px;">
                <h4 class="insight-header">Distribution Histogram:</h4>
                <div class="insight-content" style="padding: 15px;">
                    <div style="display: flex; align-items: flex-end; justify-content: space-between; height: 200px; gap: 2px; padding: 15px;">
"""

        for bucket in buckets:
            bar_height = (bucket['count'] / max_count * 100) if max_count > 0 else 0

            # Color gradient based on cumulative percentage
            cumulative_pct = bucket.get('cumulative_pct', 0)
            if cumulative_pct <= 25:
                bar_color = '#dbeafe'  # Very light blue
            elif cumulative_pct <= 50:
                bar_color = '#93c5fd'  # Light blue
            elif cumulative_pct <= 75:
                bar_color = '#60a5fa'  # Medium blue
            else:
                bar_color = '#3b82f6'  # Blue

            html += f"""
                        <div style="flex: 1; height: 100%; display: flex; flex-direction: column; align-items: center; justify-content: flex-end; position: relative;">
                            <span style="position: absolute; top: -20px; font-size: 0.75em; color: #374151; font-weight: 600;">{bucket['percentage']:.1f}%</span>
                            <div style="width: 100%; height: {bar_height}%; background: {bar_color}; border-radius: 4px 4px 0 0; position: relative; display: flex; flex-direction: column; justify-content: flex-end; align-items: center; transition: all 0.2s; cursor: pointer;" onmouseover="this.style.opacity='0.7'" onmouseout="this.style.opacity='1'" title="{bucket['bucket']}: {format_number(bucket['count'])} ({bucket['percentage']:.1f}%)">
                                <span style="font-size: 0.7em; color: #1f2937; font-weight: 600; writing-mode: vertical-rl; text-orientation: mixed; padding: 4px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;">{bucket['bucket']}</span>
                            </div>
                        </div>
"""

        html += """
                    </div>
                    <div class="std-footer">
                        <span><span class="text-bold">Buckets:</span> """ + str(len(buckets)) + """</span>
                        <span><span class="text-bold">Max:</span> """ + format_number(max_count,'',0) + """ rows</span>
                    </div>
                </div>
            </div>
"""
    return html


def _render_datetime_insights(insights: List[Any]) -> str:
    """Render datetime column insights"""
    insights_dict = _insights_to_dict(insights)
    html = ""

    # Date range - visual timeline
    if 'min_date' in insights_dict and 'max_date' in insights_dict:
        min_date_str = str(insights_dict['min_date'])
        max_date_str = str(insights_dict['max_date'])
        days = insights_dict.get('date_range_days', 0)

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
    elif 'min_date' in insights_dict or 'max_date' in insights_dict:
        # Fallback for incomplete date range
        html += """
            <div style="margin-bottom: 15px;">
                <h4 style="margin: 10px 0 8px 0; color: #6b7280; font-size: 0.95em;">Date Range:</h4>
                <div style="background: white; padding: 10px 12px; border-radius: 6px; border: 1px solid #e5e7eb; font-size: 0.9em;">
"""
        if 'min_date' in insights_dict:
            html += f"""<strong>From:</strong> {insights_dict['min_date']}<br>"""
        if 'max_date' in insights_dict:
            html += f"""<strong>To:</strong> {insights_dict['max_date']}"""
        html += """
                </div>
            </div>
"""

    # Timezone - compact inline display
    if 'timezone' in insights_dict:
        tz_value = insights_dict['timezone']
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
    if 'most_common_dates' in insights_dict and insights_dict['most_common_dates']:
        html += """
            <div style="margin-bottom: 15px;">
                <h4 style="margin: 10px 0 8px 0; color: #6b7280; font-size: 0.95em;">Most Common Dates:</h4>
                <div style="background: white; padding: 10px; border-radius: 6px; border: 1px solid #e5e7eb;">
"""
        for item in insights_dict['most_common_dates']:
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
    if 'most_common_days' in insights_dict and insights_dict['most_common_days']:
        html += """
            <div style="margin-bottom: 15px;">
                <h4 style="margin: 10px 0 8px 0; color: #6b7280; font-size: 0.95em;">Most Common Days of Week:</h4>
                <div style="background: white; padding: 10px; border-radius: 6px; border: 1px solid #e5e7eb;">
"""
        for item in insights_dict['most_common_days']:
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
    if 'most_common_hours' in insights_dict and insights_dict['most_common_hours']:
        html += """
            <div style="margin-bottom: 15px;">
                <h4 style="margin: 10px 0 8px 0; color: #6b7280; font-size: 0.95em;">Most Common Hours:</h4>
                <div style="background: white; padding: 10px; border-radius: 6px; border: 1px solid #e5e7eb;">
"""
        for item in insights_dict['most_common_hours']:
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


def _render_boolean_insights(insights: List[Any]) -> str:
    """Render boolean column insights"""
    insights_dict = _insights_to_dict(insights)
    html = ""

    # Boolean value distribution with visual bar chart (true/false/null)
    if 'boolean_distribution' in insights_dict and insights_dict['boolean_distribution']:
        html += """
            <div class="insight-section">
                <h4 class="insight-header">Value Distribution:</h4>
                <div class="insight-content">
"""
        for item in insights_dict['boolean_distribution']:
            # Format value display
            value = item['value']
            if value is None:
                value_str = 'null'
                full_value = 'null'
                bar_color = '#9ca3af'  # Gray for null
            elif value is True:
                value_str = 'true'
                full_value = 'true'
                bar_color = '#10b981'  # Green for true
            elif value is False:
                value_str = 'false'
                full_value = 'false'
                bar_color = '#ef4444'  # Red for false
            else:
                value_str = str(value)
                full_value = str(value)
                bar_color = '#3b82f6'  # Blue for other

            bar_width = item['percentage']

            html += f"""
                    <div class="top-value-item" style="background: linear-gradient(to right, {bar_color} 0%, {bar_color} {bar_width}%, #f3f4f6 {bar_width}%, #f3f4f6 100%);" title="{full_value}">
                        <span class="top-value-label"><code>{value_str}</code></span>
                        <div class="top-value-stats">
                            <span class="top-value-count">({item['count']:,})</span>
                            <span class="top-value-pct">{item['percentage']:.1f}%</span>
                        </div>
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
        <h2 style="margin-top: 0; color: #1f2937;">Column Insights</h2>
        <p style="color: #666; margin-bottom: 20px;">Data profiling and distribution analysis</p>
"""

    col_idx = 0
    for col_name, insights in results['column_insights'].items():
        col_id = f"insights-{col_idx}"
        col_idx += 1

        # Get column dtype from column_summary
        dtype = results.get('column_summary', {}).get(col_name, {}).get('dtype', 'unknown')

        html += f"""
        <div class="column-card">
            <div class="collapsible-header collapsible-section" onclick="toggleCollapse('{col_id}')">
                <span class="collapse-icon" id="{col_id}-icon">▼</span>
                <div class="flex flex-between flex-center flex-1">
                    <div class="column-name">{col_name}</div>
                    <div class="column-type">{dtype}</div>
                </div>
            </div>
            <div class="collapsible-content" id="{col_id}">
"""
        
        # Render different insight types
        html += _render_string_insights(insights)
        html += _render_numeric_insights(insights, thousand_separator, decimal_places)
        html += _render_datetime_insights(insights)
        html += _render_boolean_insights(insights)

        html += """
            </div>
        </div>
"""

    return html
