"""
Histogram Insight - Distribution buckets for numeric data
"""

from typing import List, Optional, Union, Dict, Any
from pydantic import BaseModel, Field, field_validator
import polars as pl
from ...core.base_insight import BaseInsight, InsightResult
from ...core.plugin import register_plugin


class HistogramParams(BaseModel):
    """Parameters for histogram insight"""
    bins: Optional[int] = Field(default=10, gt=0, description="Number of bins (for equal_width/equal_frequency)")
    method: str = Field(default="equal_width", description="Binning method: equal_width, equal_frequency, quartiles, explicit")
    buckets: Optional[List[float]] = Field(default=None, description="Explicit bucket boundaries (only for method='explicit')")
    edge_handling: str = Field(default="include_left", description="include_left, include_right, include_both")

    @field_validator('method')
    @classmethod
    def validate_method(cls, v):
        """Validate method is one of the allowed values"""
        allowed = ['equal_width', 'equal_frequency', 'quartiles', 'explicit']
        if v not in allowed:
            raise ValueError(f"method must be one of {allowed}, got {v}")
        return v

    @field_validator('edge_handling')
    @classmethod
    def validate_edge_handling(cls, v):
        """Validate edge_handling is one of the allowed values"""
        allowed = ['include_left', 'include_right', 'include_both']
        if v not in allowed:
            raise ValueError(f"edge_handling must be one of {allowed}, got {v}")
        return v


@register_plugin("histogram", category="insight")
class HistogramInsight(BaseInsight):
    """Generate histogram distribution buckets for numeric columns

    Supports multiple binning strategies:
    - equal_width: Equal-sized ranges (default with 10 bins)
    - equal_frequency: Equal number of values per bin
    - quartiles: Use quantile boundaries (Q1, Q2, Q3)
    - explicit: User-defined bucket boundaries
    """

    display_name = "Histogram"
    supported_dtypes = [
        pl.Int8, pl.Int16, pl.Int32, pl.Int64,
        pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64,
        pl.Float32, pl.Float64
    ]

    def _validate_params(self) -> None:
        """Validate parameters using Pydantic"""
        # Extract histogram config from params dict
        # params will be like {'histogram': True} or {'histogram': {...}}
        histogram_config = self.params.get('histogram', True)

        # Handle flexible config input: bool, int, or dict
        if isinstance(histogram_config, bool):
            if not histogram_config:
                # Disabled - will return empty in generate()
                self.config = None
                return
            # Enabled with defaults
            params_dict = {}
        elif isinstance(histogram_config, int):
            # Number of bins provided
            params_dict = {'bins': histogram_config}
        elif isinstance(histogram_config, dict):
            # Full configuration provided
            params_dict = histogram_config
        else:
            raise ValueError(f"histogram config must be bool, int, or dict, got {type(histogram_config)}")

        self.config = HistogramParams(**params_dict)

        # Additional validation for explicit method
        if self.config.method == 'explicit' and not self.config.buckets:
            raise ValueError("buckets must be provided when method='explicit'")
        if self.config.method != 'explicit' and self.config.buckets:
            raise ValueError(f"buckets can only be used with method='explicit', got method='{self.config.method}'")

    def generate(self) -> List[InsightResult]:
        """Generate histogram insight

        Returns:
            List containing a single InsightResult with histogram buckets
        """
        if self.config is None:
            return []

        non_null_series = self._get_non_null_series()

        if len(non_null_series) == 0:
            return []

        # Determine bucket boundaries based on method
        if self.config.method == 'explicit':
            boundaries = sorted(self.config.buckets)
        elif self.config.method == 'quartiles':
            boundaries = self._compute_quartile_boundaries(non_null_series)
        elif self.config.method == 'equal_frequency':
            boundaries = self._compute_equal_frequency_boundaries(non_null_series, self.config.bins)
        else:  # equal_width
            boundaries = self._compute_equal_width_boundaries(non_null_series, self.config.bins)

        # Compute bucket counts
        buckets = self._compute_bucket_counts(non_null_series, boundaries)

        method_label = {
            'equal_width': 'Equal Width',
            'equal_frequency': 'Equal Frequency',
            'quartiles': 'Quartile-Based',
            'explicit': 'Custom Buckets'
        }.get(self.config.method, self.config.method)

        return [
            InsightResult(
                type='histogram',
                value=buckets,
                display_name=f'Distribution ({len(buckets)} bins, {method_label})',
                metadata={
                    'bins': len(buckets),
                    'method': self.config.method,
                    'edge_handling': self.config.edge_handling,
                    'min_value': self._format_numeric(float(non_null_series.min())),
                    'max_value': self._format_numeric(float(non_null_series.max())),
                    'boundaries': boundaries
                }
            )
        ]

    def _compute_equal_width_boundaries(self, series: pl.Series, bins: int) -> List[float]:
        """Compute boundaries for equal-width bins

        Args:
            series: Non-null numeric series
            bins: Number of bins to create

        Returns:
            List of boundary values (length = bins + 1)
        """
        min_val = float(series.min())
        max_val = float(series.max())

        # Handle edge case where all values are the same
        if min_val == max_val:
            return [min_val, max_val]

        # Compute bin width
        width = (max_val - min_val) / bins

        # Generate boundaries
        boundaries = [min_val + i * width for i in range(bins + 1)]

        return boundaries

    def _compute_equal_frequency_boundaries(self, series: pl.Series, bins: int) -> List[float]:
        """Compute boundaries for equal-frequency bins (quantile-based)

        Args:
            series: Non-null numeric series
            bins: Number of bins to create

        Returns:
            List of boundary values
        """
        # Compute quantiles for equal-frequency bins
        quantiles = [i / bins for i in range(bins + 1)]
        boundaries = [float(series.quantile(q)) for q in quantiles]

        # Remove duplicate boundaries (can happen with discrete data)
        unique_boundaries = []
        for boundary in boundaries:
            if not unique_boundaries or boundary != unique_boundaries[-1]:
                unique_boundaries.append(boundary)

        return unique_boundaries

    def _compute_quartile_boundaries(self, series: pl.Series) -> List[float]:
        """Compute boundaries based on quartiles (Q1, Q2, Q3)

        Args:
            series: Non-null numeric series

        Returns:
            List of boundary values [min, Q1, Q2, Q3, max]
        """
        min_val = float(series.min())
        q1 = float(series.quantile(0.25))
        q2 = float(series.quantile(0.50))
        q3 = float(series.quantile(0.75))
        max_val = float(series.max())

        # Build unique boundaries
        boundaries = [min_val, q1, q2, q3, max_val]

        # Remove duplicates while preserving order
        unique_boundaries = []
        for boundary in boundaries:
            if not unique_boundaries or boundary != unique_boundaries[-1]:
                unique_boundaries.append(boundary)

        return unique_boundaries

    def _compute_bucket_counts(self, series: pl.Series, boundaries: List[float]) -> List[Dict[str, Any]]:
        """Compute counts for each bucket

        Args:
            series: Non-null numeric series
            boundaries: Sorted list of boundary values

        Returns:
            List of dicts with bucket info (range, count, percentage, cumulative_pct)
        """
        total = len(series)
        buckets = []
        cumulative_count = 0

        for i in range(len(boundaries) - 1):
            lower = boundaries[i]
            upper = boundaries[i + 1]

            # Count values in this bucket based on edge handling
            if self.config.edge_handling == 'include_left':
                # [lower, upper) - standard histogram convention
                if i == 0:
                    # First bucket: include values exactly at lower bound
                    count = series.filter((series >= lower) & (series < upper)).len()
                else:
                    count = series.filter((series >= lower) & (series < upper)).len()

                # Last bucket: include upper bound
                if i == len(boundaries) - 2:
                    count = series.filter((series >= lower) & (series <= upper)).len()

            elif self.config.edge_handling == 'include_right':
                # (lower, upper] - alternative convention
                count = series.filter((series > lower) & (series <= upper)).len()

                # First bucket: include lower bound
                if i == 0:
                    count = series.filter((series >= lower) & (series <= upper)).len()

            else:  # include_both
                # [lower, upper] - useful for discrete values
                count = series.filter((series >= lower) & (series <= upper)).len()

            cumulative_count += count
            percentage = (count / total * 100) if total > 0 else 0
            cumulative_pct = (cumulative_count / total * 100) if total > 0 else 0

            # Format bucket label
            lower_fmt = self._format_numeric(lower)
            upper_fmt = self._format_numeric(upper)

            if i == 0 and self.config.edge_handling == 'include_left':
                bucket_label = f'[{lower_fmt}, {upper_fmt})'
            elif i == len(boundaries) - 2:
                if self.config.edge_handling == 'include_left':
                    bucket_label = f'[{lower_fmt}, {upper_fmt}]'
                else:
                    bucket_label = f'({lower_fmt}, {upper_fmt}]'
            else:
                if self.config.edge_handling == 'include_left':
                    bucket_label = f'[{lower_fmt}, {upper_fmt})'
                elif self.config.edge_handling == 'include_right':
                    bucket_label = f'({lower_fmt}, {upper_fmt}]'
                else:
                    bucket_label = f'[{lower_fmt}, {upper_fmt}]'

            buckets.append({
                'bucket': bucket_label,
                'lower': lower_fmt,
                'upper': upper_fmt,
                'count': count,
                'percentage': round(percentage, 2),
                'cumulative_pct': round(cumulative_pct, 2)
            })

        return buckets
