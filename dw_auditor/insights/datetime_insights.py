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

    return insights
