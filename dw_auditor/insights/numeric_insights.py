"""
Numeric column insights generation
"""

import polars as pl
from typing import Dict, List


def generate_numeric_insights(df: pl.DataFrame, col: str, config: Dict) -> Dict:
    """
    Generate insights for numeric columns (Int, Float)

    Args:
        df: Polars DataFrame
        col: Column name
        config: Insights configuration for this column

    Returns:
        Dictionary with numeric insights
    """
    insights = {}

    # Get non-null values
    non_null_series = df[col].drop_nulls()
    if len(non_null_series) == 0:
        return insights

    # Basic statistics
    if config.get('min', False):
        insights['min'] = float(non_null_series.min())
    if config.get('max', False):
        insights['max'] = float(non_null_series.max())
    if config.get('mean', False):
        insights['mean'] = round(float(non_null_series.mean()), 4)
    if config.get('median', False):
        insights['median'] = float(non_null_series.median())
    if config.get('std', False):
        insights['std'] = round(float(non_null_series.std()), 4)

    # Quantiles/Percentiles
    quantiles = config.get('quantiles', [])
    if quantiles:
        quantile_values = {}
        for q in quantiles:
            quantile_values[f'p{int(q*100)}'] = float(non_null_series.quantile(q))
        insights['quantiles'] = quantile_values

    # Top N most frequent values
    if config.get('top_values', 0) > 0:
        top_n = config['top_values']
        total_non_null = len(non_null_series)

        # Calculate value counts and percentages using Polars expressions
        value_counts = (
            df.select(pl.col(col))
            .filter(pl.col(col).is_not_null())
            .group_by(col)
            .agg(pl.count().alias('count'))
            .with_columns(
                (pl.col('count') / total_non_null * 100).alias('percentage') if total_non_null > 0
                else pl.lit(0.0).alias('percentage')
            )
            .sort('count', descending=True)
            .head(top_n)
            .to_dicts()
        )

        insights['top_values'] = [
            {
                'value': float(item[col]) if item[col] is not None else None,
                'count': item['count'],
                'percentage': item['percentage']
            }
            for item in value_counts
        ]

    return insights
