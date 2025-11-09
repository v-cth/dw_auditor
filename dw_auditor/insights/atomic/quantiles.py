"""
Quantiles Insight - Statistical quantiles/percentiles for numeric data
"""

from typing import List, Union
from pydantic import BaseModel, Field, field_validator
import polars as pl
from ...core.base_insight import BaseInsight, InsightResult
from ...core.insight_registry import register_insight


class QuantilesParams(BaseModel):
    """Parameters for quantiles insight"""
    quantiles: Union[bool, List[float]] = Field(
        default=True,
        description="Quantiles to compute. True = [0.25, 0.5, 0.75], or provide custom list"
    )

    @field_validator('quantiles')
    @classmethod
    def validate_quantiles(cls, v):
        """Validate quantile values are between 0 and 1"""
        if isinstance(v, list):
            for q in v:
                if not (0 <= q <= 1):
                    raise ValueError(f"Quantile values must be between 0 and 1, got {q}")
        return v


@register_insight("quantiles")
class QuantilesInsight(BaseInsight):
    """Generate statistical quantiles (percentiles) for numeric columns

    Computes quantiles like Q1 (25th percentile), median (50th), Q3 (75th), etc.
    Supports custom quantile specifications.
    """

    display_name = "Quantiles"
    supported_dtypes = [
        pl.Int8, pl.Int16, pl.Int32, pl.Int64,
        pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64,
        pl.Float32, pl.Float64
    ]

    def _validate_params(self) -> None:
        """Validate parameters using Pydantic"""
        self.config = QuantilesParams(**self.params)

    def generate(self) -> List[InsightResult]:
        """Generate quantiles insight

        Returns:
            List containing two InsightResults:
            - quantiles: Dict mapping percentile names (p25, p50, p75) to values
            - median: Scalar value (p50, provided for convenience)
        """
        non_null_series = self._get_non_null_series()

        if len(non_null_series) == 0:
            return []

        # Determine which quantiles to compute
        if self.config.quantiles is False:
            return []

        if self.config.quantiles is True:
            quantiles_to_compute = [0.25, 0.5, 0.75]
        else:
            quantiles_to_compute = self.config.quantiles

        # Compute quantiles
        quantile_values = {}
        median_value = None

        for q in quantiles_to_compute:
            value = float(non_null_series.quantile(q))
            formatted_value = self._format_numeric(value)
            percentile_name = f'p{int(q * 100)}'
            quantile_values[percentile_name] = formatted_value

            # Track median (p50) separately
            if q == 0.5:
                median_value = formatted_value

        results = [
            InsightResult(
                type='quantiles',
                value=quantile_values,
                display_name='Quantiles',
                metadata={'quantiles_computed': quantiles_to_compute}
            )
        ]

        # Add median as separate result for convenience
        if median_value is not None:
            results.append(
                InsightResult(
                    type='median',
                    value=median_value,
                    display_name='Median',
                    metadata={'note': 'Same as p50 in quantiles'}
                )
            )

        return results
