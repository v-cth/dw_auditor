"""
Length Stats Insight - String length statistics
"""

from typing import List
from pydantic import BaseModel
import polars as pl
from ...core.base_insight import BaseInsight, InsightResult
from ...core.plugin import register_plugin


class LengthStatsParams(BaseModel):
    """Parameters for length stats insight"""
    min_length: bool = False
    max_length: bool = False
    avg_length: bool = False


@register_plugin("length_stats", category="insight")
class LengthStatsInsight(BaseInsight):
    """Generate string length statistics

    Computes min, max, and average length of string values.
    Can be configured to compute individual stats or all together.
    """

    display_name = "Length Statistics"
    supported_dtypes = [pl.Utf8, pl.String]

    def _validate_params(self) -> None:
        """Validate parameters using Pydantic"""
        self.config = LengthStatsParams(**self.params)

    def generate(self) -> List[InsightResult]:
        """Generate length statistics insights

        Returns:
            List of InsightResults for requested stats (min, max, avg)
        """
        non_null_series = self._get_non_null_series()

        if len(non_null_series) == 0:
            return []

        # Check if any stats are requested
        if not (self.config.min_length or self.config.max_length or self.config.avg_length):
            return []

        # Calculate string lengths
        lengths = non_null_series.str.len_chars()

        results = []

        # Min length
        if self.config.min_length:
            min_len = int(lengths.min())
            results.append(
                InsightResult(
                    type='min_length',
                    value=min_len,
                    display_name='Minimum Length',
                    unit='characters'
                )
            )

        # Max length
        if self.config.max_length:
            max_len = int(lengths.max())
            results.append(
                InsightResult(
                    type='max_length',
                    value=max_len,
                    display_name='Maximum Length',
                    unit='characters'
                )
            )

        # Average length
        if self.config.avg_length:
            avg_len = self._format_numeric(float(lengths.mean()), decimals=1)
            results.append(
                InsightResult(
                    type='avg_length',
                    value=avg_len,
                    display_name='Average Length',
                    unit='characters'
                )
            )

        return results
