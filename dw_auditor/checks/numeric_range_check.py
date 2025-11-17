"""
Numeric range validation check
"""

from pydantic import BaseModel, validator
from typing import Optional, List
from ..core.base_check import BaseCheck, CheckResult
from ..core.plugin import register_plugin
import polars as pl


class NumericRangeParams(BaseModel):
    """Parameters for numeric range validation

    At least one boundary parameter must be provided.

    Attributes:
        greater_than: Exclusive lower bound (value > threshold)
        greater_than_or_equal: Inclusive lower bound (value >= threshold)
        less_than: Exclusive upper bound (value < threshold)
        less_than_or_equal: Inclusive upper bound (value <= threshold)
    """
    greater_than: Optional[float] = None
    greater_than_or_equal: Optional[float] = None
    less_than: Optional[float] = None
    less_than_or_equal: Optional[float] = None

    @validator('less_than', 'less_than_or_equal', 'greater_than', 'greater_than_or_equal')
    def at_least_one_param(cls, v, values):
        """Ensure at least one range parameter is provided"""
        # If this is the last field and nothing has been set yet, raise error
        all_values = list(values.values()) + ([v] if v is not None else [])
        if not any(val is not None for val in all_values):
            if len(values) == 3:  # We're on the last field
                raise ValueError("At least one range parameter must be provided")
        return v


@register_plugin("numeric_range", category="check")
class NumericRangeCheck(BaseCheck):
    """Check if numeric values fall within specified ranges

    Validates that numeric column values satisfy boundary conditions.
    Supports both inclusive and exclusive boundaries.

    Configuration example:
        {
            "numeric_range": {
                "greater_than": 0,
                "less_than_or_equal": 100
            }
        }
    """

    display_name = "Numeric Range Validation"
    supported_dtypes = [
        pl.Int8, pl.Int16, pl.Int32, pl.Int64,
        pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64,
        pl.Float32, pl.Float64
    ]

    def _validate_params(self) -> None:
        """Validate numeric range parameters"""
        self.config = NumericRangeParams(**self.params)

    def run(self) -> List[CheckResult]:
        """Execute numeric range validation

        Returns:
            List of CheckResult objects, one per violated boundary
        """
        results = []
        non_null_df = self._get_non_null_df()
        non_null_count = len(non_null_df)

        if non_null_count == 0:
            return results

        # Check greater_than (exclusive: value > threshold)
        if self.config.greater_than is not None:
            violating = non_null_df.filter(pl.col(self.col) <= self.config.greater_than)
            if len(violating) > 0:
                results.append(CheckResult(
                    type='VALUE_NOT_GREATER_THAN',
                    count=len(violating),
                    pct=self._calculate_percentage(len(violating), non_null_count),
                    threshold=self.config.greater_than,
                    operator='>',
                    suggestion=f'Values should be > {self.config.greater_than}',
                    examples=self._get_examples(violating)
                ))

        # Check greater_than_or_equal (inclusive: value >= threshold)
        if self.config.greater_than_or_equal is not None:
            violating = non_null_df.filter(pl.col(self.col) < self.config.greater_than_or_equal)
            if len(violating) > 0:
                results.append(CheckResult(
                    type='VALUE_NOT_GREATER_OR_EQUAL',
                    count=len(violating),
                    pct=self._calculate_percentage(len(violating), non_null_count),
                    threshold=self.config.greater_than_or_equal,
                    operator='>=',
                    suggestion=f'Values should be >= {self.config.greater_than_or_equal}',
                    examples=self._get_examples(violating)
                ))

        # Check less_than (exclusive: value < threshold)
        if self.config.less_than is not None:
            violating = non_null_df.filter(pl.col(self.col) >= self.config.less_than)
            if len(violating) > 0:
                results.append(CheckResult(
                    type='VALUE_NOT_LESS_THAN',
                    count=len(violating),
                    pct=self._calculate_percentage(len(violating), non_null_count),
                    threshold=self.config.less_than,
                    operator='<',
                    suggestion=f'Values should be < {self.config.less_than}',
                    examples=self._get_examples(violating)
                ))

        # Check less_than_or_equal (inclusive: value <= threshold)
        if self.config.less_than_or_equal is not None:
            violating = non_null_df.filter(pl.col(self.col) > self.config.less_than_or_equal)
            if len(violating) > 0:
                results.append(CheckResult(
                    type='VALUE_NOT_LESS_OR_EQUAL',
                    count=len(violating),
                    pct=self._calculate_percentage(len(violating), non_null_count),
                    threshold=self.config.less_than_or_equal,
                    operator='<=',
                    suggestion=f'Values should be <= {self.config.less_than_or_equal}',
                    examples=self._get_examples(violating)
                ))

        return results
