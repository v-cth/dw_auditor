"""
Date outlier detection check
"""

from pydantic import BaseModel, Field
from typing import List
from ..core.base_check import BaseCheck, CheckResult
from ..core.registry import register_check
import polars as pl


class DateOutlierParams(BaseModel):
    """Parameters for date outlier check

    Attributes:
        min_year: Minimum reasonable year (default: 1950)
        max_year: Maximum reasonable year (default: 2100)
    """
    min_year: int = Field(default=1950, ge=1000, le=9999)
    max_year: int = Field(default=2100, ge=1000, le=9999)


@register_check("date_outliers")
class DateOutlierCheck(BaseCheck):
    """Detect date/timestamp outliers (unusually old or future dates)

    Identifies dates outside reasonable year ranges:
    - Dates before min_year (default: 1950)
    - Dates after max_year (default: 2100)
    - Suspicious placeholder years: 1900, 1970, 2099, 2999, 9999

    These often indicate data errors, placeholder values, or default timestamps.

    Always reports outliers if detected (no percentage threshold).

    Configuration example:
        {"date_outliers": {"min_year": 1950, "max_year": 2100}}
    """

    display_name = "Date Outlier Detection"
    supported_dtypes = [pl.Datetime, pl.Date]

    def _validate_params(self) -> None:
        """Validate date outlier parameters"""
        self.config = DateOutlierParams(**self.params)

        # Validate that min_year < max_year
        if self.config.min_year >= self.config.max_year:
            raise ValueError(f"min_year ({self.config.min_year}) must be less than max_year ({self.config.max_year})")

    async def run(self) -> List[CheckResult]:
        """Execute date outlier check

        Returns:
            List of CheckResult objects for detected outliers
        """
        results = []

        non_null_df = self._get_non_null_df()
        non_null_count = len(non_null_df)

        if non_null_count == 0:
            return results

        # Extract year for both Date and Datetime columns
        year_col = non_null_df.select(pl.col(self.col).dt.year().alias('year'))

        # Find dates before min_year
        too_old = non_null_df.filter(pl.col(self.col).dt.year() < self.config.min_year)
        if len(too_old) > 0:
            pct_old = len(too_old) / non_null_count * 100
            # Convert dates/datetimes to formatted strings
            examples = [str(val) if val else None for val in too_old[self.col].head(5).to_list()]
            min_year_found = year_col['year'].min()

            results.append(CheckResult(
                type='DATES_TOO_OLD',
                count=len(too_old),
                pct=pct_old,
                min_year_found=int(min_year_found),
                threshold_year=self.config.min_year,
                suggestion=f'Found dates before {self.config.min_year} - check if these are valid or data errors',
                examples=examples
            ))

        # Find dates after max_year
        too_future = non_null_df.filter(pl.col(self.col).dt.year() > self.config.max_year)
        if len(too_future) > 0:
            pct_future = len(too_future) / non_null_count * 100
            # Convert dates/datetimes to formatted strings
            examples = [str(val) if val else None for val in too_future[self.col].head(5).to_list()]
            max_year_found = year_col['year'].max()

            results.append(CheckResult(
                type='DATES_TOO_FUTURE',
                count=len(too_future),
                pct=pct_future,
                max_year_found=int(max_year_found),
                threshold_year=self.config.max_year,
                suggestion=f'Found dates after {self.config.max_year} - check if these are valid or placeholder values',
                examples=examples
            ))

        # Check for specific problematic years
        problematic_years = [1900, 1970, 2099, 2999, 9999]
        year_counts = year_col.group_by('year').agg(pl.len().alias('count'))

        for problem_year in problematic_years:
            year_data = year_counts.filter(pl.col('year') == problem_year)
            if len(year_data) > 0:
                count = year_data['count'][0]
                pct = count / non_null_count * 100

                # Convert dates/datetimes to formatted strings
                examples = [str(val) if val else None for val in non_null_df.filter(
                    pl.col(self.col).dt.year() == problem_year
                )[self.col].head(5).to_list()]

                results.append(CheckResult(
                    type='SUSPICIOUS_YEAR',
                    year=problem_year,
                    count=int(count),
                    pct=pct,
                    suggestion=f'Year {problem_year} appears frequently - often used as placeholder/default value',
                    examples=examples
                ))

        return results
