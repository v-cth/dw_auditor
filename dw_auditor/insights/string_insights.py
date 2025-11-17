"""
String column insights - composite insight for string types
"""

from typing import List
from pydantic import BaseModel
import polars as pl
from ..core.base_insight import BaseInsight, InsightResult
from ..core.plugin import register_plugin
from ..core.insight_runner import run_insight_sync


class StringInsightsParams(BaseModel):
    """Parameters for string insights"""
    top_values: int = 0
    min_length: bool = False
    max_length: bool = False
    avg_length: bool = False


@register_plugin("string_insights", category="insight")
class StringInsights(BaseInsight):
    """Composite insight for string columns

    Generates insights for string columns including:
    - Most frequent values
    - Length statistics (min, max, average)
    """

    display_name = "String Column Insights"
    supported_dtypes = [pl.Utf8, pl.String]

    def _validate_params(self) -> None:
        """Validate parameters using Pydantic"""
        self.config = StringInsightsParams(**self.params)

    def generate(self) -> List[InsightResult]:
        """Generate string insights

        Returns:
            List of InsightResult objects for all requested metrics
        """
        results = []
        non_null_series = self._get_non_null_series()

        if len(non_null_series) == 0:
            return results

        # Top values (delegate to atomic insight)
        if self.config.top_values > 0:
            top_values_results = run_insight_sync(
                'top_values',
                self.df,
                self.col,
                limit=self.config.top_values
            )
            results.extend(top_values_results)

        # Length statistics (delegate to atomic insight)
        if self.config.min_length or self.config.max_length or self.config.avg_length:
            length_stats_results = run_insight_sync(
                'length_stats',
                self.df,
                self.col,
                min_length=self.config.min_length,
                max_length=self.config.max_length,
                avg_length=self.config.avg_length
            )
            results.extend(length_stats_results)

        return results
