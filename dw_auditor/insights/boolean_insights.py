"""
Boolean column insights - composite insight for boolean types
"""

from typing import List
from pydantic import BaseModel
import polars as pl
from ..core.base_insight import BaseInsight, InsightResult
from ..core.insight_registry import register_insight


class BooleanInsightsParams(BaseModel):
    """Parameters for boolean insights"""
    top_values: int = 0


@register_insight("boolean_insights")
class BooleanInsights(BaseInsight):
    """Composite insight for boolean columns

    Generates distribution of boolean values including:
    - True/False/Null counts and percentages
    """

    display_name = "Boolean Column Insights"
    supported_dtypes = [pl.Boolean]

    def _validate_params(self) -> None:
        """Validate parameters using Pydantic"""
        self.config = BooleanInsightsParams(**self.params)

    def generate(self) -> List[InsightResult]:
        """Generate boolean insights

        Returns:
            List containing a single InsightResult with boolean distribution
        """
        if self.config.top_values <= 0:
            return []

        total_rows = len(self.df)

        if total_rows == 0:
            return []

        # Calculate value counts including nulls (important for boolean analysis)
        value_counts = (
            self.df.select(pl.col(self.col))
            .group_by(self.col)
            .agg(pl.count().alias('count'))
            .with_columns(
                (pl.col('count') / total_rows * 100).alias('percentage')
            )
            .sort('count', descending=True)
            .head(self.config.top_values)
            .to_dicts()
        )

        distribution = [
            {
                'value': item[self.col],
                'count': item['count'],
                'percentage': item['percentage']
            }
            for item in value_counts
        ]

        if not distribution:
            return []

        return [
            InsightResult(
                type='boolean_distribution',
                value=distribution,
                display_name='Boolean Distribution',
                metadata={
                    'includes_nulls': True,
                    'total_rows': total_rows
                }
            )
        ]
