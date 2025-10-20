"""
DateTime column insights generation
"""

import polars as pl
from typing import Dict
from datetime import datetime, timedelta


def generate_datetime_insights(df: pl.DataFrame, col: str, config: Dict) -> Dict:
    """
    Generate insights for datetime columns

    Args:
        df: Polars DataFrame
        col: Column name
        config: Insights configuration for this column

    Returns:
        Dictionary with datetime insights
    """
    insights = {}

    # Get non-null values
    non_null_series = df[col].drop_nulls()
    if len(non_null_series) == 0:
        return insights

    # Min and Max dates
    if config.get('min_date', False):
        min_date = non_null_series.min()
        insights['min_date'] = str(min_date)

    if config.get('max_date', False):
        max_date = non_null_series.max()
        insights['max_date'] = str(max_date)

    # Date range in days
    if config.get('date_range_days', False):
        min_date = non_null_series.min()
        max_date = non_null_series.max()
        if min_date is not None and max_date is not None:
            # Calculate difference
            date_range = (max_date - min_date)
            insights['date_range_days'] = int(date_range.total_seconds() / 86400) if hasattr(date_range, 'total_seconds') else 0

    # Most common dates
    if config.get('most_common_dates', 0) > 0:
        top_n = config['most_common_dates']

        # Convert to date only (no time) for grouping
        value_counts = (
            df.select(pl.col(col))
            .filter(pl.col(col).is_not_null())
            .with_columns(pl.col(col).dt.date().alias('date_only'))
            .group_by('date_only')
            .agg(pl.count().alias('count'))
            .sort('count', descending=True)
            .head(top_n)
            .to_dicts()
        )

        total_non_null = len(non_null_series)
        insights['most_common_dates'] = [
            {
                'date': str(item['date_only']),
                'count': item['count'],
                'percentage': (item['count'] / total_non_null * 100) if total_non_null > 0 else 0
            }
            for item in value_counts
        ]

    # Most common hours
    if config.get('most_common_hours', 0) > 0:
        top_n = config['most_common_hours']

        hour_counts = (
            df.select(pl.col(col))
            .filter(pl.col(col).is_not_null())
            .with_columns(pl.col(col).dt.hour().alias('hour'))
            .group_by('hour')
            .agg(pl.count().alias('count'))
            .sort('count', descending=True)
            .head(top_n)
            .to_dicts()
        )

        total_non_null = len(non_null_series)
        insights['most_common_hours'] = [
            {
                'hour': item['hour'],
                'count': item['count'],
                'percentage': (item['count'] / total_non_null * 100) if total_non_null > 0 else 0
            }
            for item in hour_counts
        ]

    # Most common days of week
    if config.get('most_common_days', 0) > 0:
        top_n = config['most_common_days']

        # Polars weekday: Monday=1, Sunday=7
        day_names = {1: 'Monday', 2: 'Tuesday', 3: 'Wednesday', 4: 'Thursday',
                     5: 'Friday', 6: 'Saturday', 7: 'Sunday'}

        day_counts = (
            df.select(pl.col(col))
            .filter(pl.col(col).is_not_null())
            .with_columns(pl.col(col).dt.weekday().alias('weekday'))
            .group_by('weekday')
            .agg(pl.count().alias('count'))
            .sort('count', descending=True)
            .head(top_n)
            .to_dicts()
        )

        total_non_null = len(non_null_series)
        insights['most_common_days'] = [
            {
                'day': day_names.get(item['weekday'], f"Day {item['weekday']}"),
                'weekday': item['weekday'],
                'count': item['count'],
                'percentage': (item['count'] / total_non_null * 100) if total_non_null > 0 else 0
            }
            for item in day_counts
        ]

    # Most common timezones (if timezone-aware)
    if config.get('most_common_timezones', 0) > 0:
        top_n = config['most_common_timezones']

        # Check if column has timezone info
        dtype = df[col].dtype
        if hasattr(dtype, 'time_zone') and dtype.time_zone is not None:
            # For timezone-aware columns, extract timezone
            # Note: Polars stores a single timezone per column, so all values have the same timezone
            insights['timezone'] = str(dtype.time_zone)
        else:
            # For columns without timezone, we can't extract timezone info
            insights['timezone'] = 'None (timezone-naive)'

    return insights
