"""
String column insights generation
"""

import polars as pl
from typing import Dict, Optional


def generate_string_insights(df: pl.DataFrame, col: str, config: Dict) -> Dict:
    """
    Generate insights for string columns

    Args:
        df: Polars DataFrame
        col: Column name
        config: Insights configuration for this column

    Returns:
        Dictionary with string insights
    """
    insights = {}

    # Get non-null values
    non_null_series = df[col].drop_nulls()
    if len(non_null_series) == 0:
        return insights

    # Top N most frequent values
    if config.get('top_values', 0) > 0:
        top_n = config['top_values']
        value_counts = (
            df.select(pl.col(col))
            .filter(pl.col(col).is_not_null())
            .group_by(col)
            .agg(pl.count().alias('count'))
            .sort('count', descending=True)
            .head(top_n)
            .to_dicts()
        )

        total_non_null = len(non_null_series)
        insights['top_values'] = [
            {
                'value': item[col],
                'count': item['count'],
                'percentage': (item['count'] / total_non_null * 100) if total_non_null > 0 else 0
            }
            for item in value_counts
        ]

    # String length statistics
    if config.get('min_length', False) or config.get('max_length', False) or config.get('avg_length', False):
        lengths = non_null_series.str.len_chars()

        insights['length_stats'] = {}
        if config.get('min_length', False):
            insights['length_stats']['min'] = int(lengths.min())
        if config.get('max_length', False):
            insights['length_stats']['max'] = int(lengths.max())
        if config.get('avg_length', False):
            insights['length_stats']['avg'] = round(float(lengths.mean()), 2)

    return insights
