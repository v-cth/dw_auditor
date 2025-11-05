"""
Boolean column insights generation
"""

import polars as pl
from typing import Dict


def generate_boolean_insights(df: pl.DataFrame, col: str, config: Dict) -> Dict:
    """
    Generate insights for boolean columns

    Args:
        df: Polars DataFrame
        col: Column name
        config: Insights configuration for this column

    Returns:
        Dictionary with boolean insights
    """
    insights = {}

    # Top N most frequent values (typically 2-3 for boolean: true, false, null)
    if config.get('top_values', 0) > 0:
        top_n = config['top_values']
        total_rows = len(df)

        # Calculate value counts including nulls, with percentages using Polars expressions
        value_counts = (
            df.select(pl.col(col))
            .group_by(col)
            .agg(pl.count().alias('count'))
            .with_columns(
                (pl.col('count') / total_rows * 100).alias('percentage') if total_rows > 0
                else pl.lit(0.0).alias('percentage')
            )
            .sort('count', descending=True)
            .head(top_n)
            .to_dicts()
        )

        insights['boolean_distribution'] = [
            {
                'value': item[col],
                'count': item['count'],
                'percentage': item['percentage']
            }
            for item in value_counts
        ]

    return insights
