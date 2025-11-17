"""
Date range validation check
"""

from pydantic import BaseModel, validator
from typing import List, Optional
from ..core.base_check import BaseCheck, CheckResult
from ..core.plugin import register_plugin
import polars as pl
from datetime import date


class DateRangeParams(BaseModel):
    """Parameters for date range check

    Attributes:
        after: Exclusive lower bound (date > after) - ISO format string "YYYY-MM-DD"
        after_or_equal: Inclusive lower bound (date >= after_or_equal) - ISO format string "YYYY-MM-DD"
        before: Exclusive upper bound (date < before) - ISO format string "YYYY-MM-DD"
        before_or_equal: Inclusive upper bound (date <= before_or_equal) - ISO format string "YYYY-MM-DD"
    """
    after: Optional[str] = None
    after_or_equal: Optional[str] = None
    before: Optional[str] = None
    before_or_equal: Optional[str] = None

    @validator('after', 'after_or_equal', 'before', 'before_or_equal')
    def validate_date_format(cls, v):
        """Ensure date strings are valid ISO format"""
        if v is not None:
            try:
                date.fromisoformat(v)
            except ValueError as e:
                raise ValueError(f"Invalid date format (expected YYYY-MM-DD): {e}")
        return v


@register_plugin("date_range", category="check")
class DateRangeCheck(BaseCheck):
    """Check if dates/datetimes are within specified range or boundaries

    Validates dates against configurable boundaries:
    - after: Values must be strictly after this date (exclusive: >)
    - after_or_equal: Values must be on or after this date (inclusive: >=)
    - before: Values must be strictly before this date (exclusive: <)
    - before_or_equal: Values must be on or before this date (inclusive: <=)

    Configuration example:
        {"date_range": {"after_or_equal": "2020-01-01", "before": "2025-01-01"}}
    """

    display_name = "Date Range Validation"
    supported_dtypes = [pl.Datetime, pl.Date]

    def _validate_params(self) -> None:
        """Validate date range parameters"""
        self.config = DateRangeParams(**self.params)

    def run(self) -> List[CheckResult]:
        """Execute date range check

        Returns:
            List of CheckResult objects, one per boundary violation
        """
        results = []

        non_null_df = self._get_non_null_df()
        non_null_count = len(non_null_df)

        if non_null_count == 0:
            return results

        # Check after (exclusive: >)
        if self.config.after is not None:
            after_date = pl.lit(self.config.after).str.to_date()
            violating = non_null_df.filter(pl.col(self.col).cast(pl.Date) <= after_date)

            if len(violating) > 0:
                results.append(CheckResult(
                    type='DATE_NOT_AFTER',
                    count=len(violating),
                    pct=self._calculate_percentage(len(violating), non_null_count),
                    threshold=self.config.after,
                    operator='>',
                    suggestion=f'Dates should be after {self.config.after}',
                    examples=self._get_examples(violating, limit=5)
                ))

        # Check after_or_equal (inclusive: >=)
        if self.config.after_or_equal is not None:
            after_eq_date = pl.lit(self.config.after_or_equal).str.to_date()
            violating = non_null_df.filter(pl.col(self.col).cast(pl.Date) < after_eq_date)

            if len(violating) > 0:
                results.append(CheckResult(
                    type='DATE_NOT_AFTER_OR_EQUAL',
                    count=len(violating),
                    pct=self._calculate_percentage(len(violating), non_null_count),
                    threshold=self.config.after_or_equal,
                    operator='>=',
                    suggestion=f'Dates should be on or after {self.config.after_or_equal}',
                    examples=self._get_examples(violating, limit=5)
                ))

        # Check before (exclusive: <)
        if self.config.before is not None:
            before_date = pl.lit(self.config.before).str.to_date()
            violating = non_null_df.filter(pl.col(self.col).cast(pl.Date) >= before_date)

            if len(violating) > 0:
                results.append(CheckResult(
                    type='DATE_NOT_BEFORE',
                    count=len(violating),
                    pct=self._calculate_percentage(len(violating), non_null_count),
                    threshold=self.config.before,
                    operator='<',
                    suggestion=f'Dates should be before {self.config.before}',
                    examples=self._get_examples(violating, limit=5)
                ))

        # Check before_or_equal (inclusive: <=)
        if self.config.before_or_equal is not None:
            before_eq_date = pl.lit(self.config.before_or_equal).str.to_date()
            violating = non_null_df.filter(pl.col(self.col).cast(pl.Date) > before_eq_date)

            if len(violating) > 0:
                results.append(CheckResult(
                    type='DATE_NOT_BEFORE_OR_EQUAL',
                    count=len(violating),
                    pct=self._calculate_percentage(len(violating), non_null_count),
                    threshold=self.config.before_or_equal,
                    operator='<=',
                    suggestion=f'Dates should be on or before {self.config.before_or_equal}',
                    examples=self._get_examples(violating, limit=5)
                ))

        return results
