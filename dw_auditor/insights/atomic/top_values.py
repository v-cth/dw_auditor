"""
Top Values Insight - Most frequent values (universal, works across all data types)
"""

from typing import List
from pydantic import BaseModel, Field
import polars as pl
from ...core.base_insight import BaseInsight, InsightResult
from ...core.plugin import register_plugin


class TopValuesParams(BaseModel):
    """Parameters for top values insight"""
    limit: int = Field(default=10, gt=0, description="Number of top values to return")


@register_plugin("top_values", category="insight")
class TopValuesInsight(BaseInsight):
    """Generate most frequent values with counts and percentages

    Works across all data types (string, numeric, datetime, boolean).
    Returns the N most frequently occurring values ordered by count.
    """

    display_name = "Most Frequent Values"
    supported_dtypes = []  # Empty = universal (all types)

    def _validate_params(self) -> None:
        """Validate parameters using Pydantic"""
        self.config = TopValuesParams(**self.params)

    def generate(self) -> List[InsightResult]:
        """Generate top values insight

        Returns:
            List containing a single InsightResult with top_values data
        """
        # Get value counts using helper method
        value_counts = self._calculate_value_counts(limit=self.config.limit)

        if not value_counts:
            return []

        # Format values based on column dtype
        col_dtype = str(self.df[self.col].dtype)

        for item in value_counts:
            value = item['value']

            # Format based on type
            if 'Int' in col_dtype or 'UInt' in col_dtype:
                item['value'] = int(value) if value is not None else None
            elif 'Float' in col_dtype:
                item['value'] = self._format_numeric(value) if value is not None else None
            # String, datetime, boolean stay as-is

        return [
            InsightResult(
                type='top_values',
                value=value_counts,
                display_name=f'Top {self.config.limit} Values',
                metadata={
                    'total_unique': len(self._get_non_null_series().unique()),
                    'limit': self.config.limit
                }
            )
        ]
