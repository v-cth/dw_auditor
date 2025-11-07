"""
Numeric column insights - composite insight for Int and Float types
"""

from typing import List, Union, Dict, Any
from pydantic import BaseModel
import polars as pl
from ..core.base_insight import BaseInsight, InsightResult
from ..core.insight_registry import register_insight
from ..core.insight_runner import run_insight_sync


class NumericInsightsParams(BaseModel):
    """Parameters for numeric insights"""
    min: bool = False
    max: bool = False
    mean: bool = False
    std: bool = False
    quantiles: bool = False
    top_values: int = 0
    histogram: Union[bool, int, Dict[str, Any]] = False


@register_insight("numeric_insights")
class NumericInsights(BaseInsight):
    """Composite insight for numeric columns

    Generates statistical insights for Int and Float columns including:
    - Basic statistics (min, max, mean, std)
    - Quantiles/percentiles
    - Most frequent values
    - Distribution histogram
    """

    display_name = "Numeric Column Insights"
    supported_dtypes = [
        pl.Int8, pl.Int16, pl.Int32, pl.Int64,
        pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64,
        pl.Float32, pl.Float64
    ]

    def _validate_params(self) -> None:
        """Validate parameters using Pydantic"""
        self.config = NumericInsightsParams(**self.params)

    def generate(self) -> List[InsightResult]:
        """Generate numeric insights

        Returns:
            List of InsightResult objects for all requested metrics
        """
        results = []
        non_null_series = self._get_non_null_series()

        if len(non_null_series) == 0:
            return results

        # Basic statistics (computed directly)
        if self.config.min:
            min_value = self._format_numeric(float(non_null_series.min()))
            results.append(
                InsightResult(
                    type='min',
                    value=min_value,
                    display_name='Minimum'
                )
            )

        if self.config.max:
            max_value = self._format_numeric(float(non_null_series.max()))
            results.append(
                InsightResult(
                    type='max',
                    value=max_value,
                    display_name='Maximum'
                )
            )

        if self.config.mean:
            mean_value = self._format_numeric(float(non_null_series.mean()))
            results.append(
                InsightResult(
                    type='mean',
                    value=mean_value,
                    display_name='Mean'
                )
            )

        if self.config.std:
            std_value = self._format_numeric(float(non_null_series.std()))
            results.append(
                InsightResult(
                    type='std',
                    value=std_value,
                    display_name='Standard Deviation'
                )
            )

        # Quantiles (delegate to atomic insight)
        if self.config.quantiles:
            quantile_results = run_insight_sync(
                'quantiles',
                self.df,
                self.col,
                quantiles=self.config.quantiles
            )
            results.extend(quantile_results)

        # Top values (delegate to atomic insight)
        if self.config.top_values > 0:
            top_values_results = run_insight_sync(
                'top_values',
                self.df,
                self.col,
                limit=self.config.top_values
            )
            results.extend(top_values_results)

        # Histogram (delegate to atomic insight)
        if self.config.histogram:
            histogram_results = run_insight_sync(
                'histogram',
                self.df,
                self.col,
                histogram=self.config.histogram
            )
            results.extend(histogram_results)

        return results
