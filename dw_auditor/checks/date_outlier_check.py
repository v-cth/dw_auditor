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
        problematic_years: List of years to flag as suspicious placeholders (default: [1900, 1970, 2099, 2999, 9999])
        min_suspicious_count: Minimum occurrences to report suspicious years (default: 1)
    """
    min_year: int = Field(default=1950, ge=1000, le=9999)
    max_year: int = Field(default=2100, ge=1000, le=9999)
    problematic_years: List[int] = Field(default=[1900, 1970, 2099, 2999, 9999])
    min_suspicious_count: int = Field(default=1, ge=1)


@register_check("date_outliers")
class DateOutlierCheck(BaseCheck):
    """Detect date/timestamp outliers (unusually old or future dates)

    Identifies dates outside reasonable year ranges:
    - Dates before min_year (default: 1950)
    - Dates after max_year (default: 2100)
    - Suspicious placeholder years (default: 1900, 1970, 2099, 2999, 9999)

    These often indicate data errors, placeholder values, or default timestamps.

    Configuration examples:
        # Basic range check
        {"date_outliers": {"min_year": 1950, "max_year": 2100}}

        # Custom placeholder years and threshold
        {"date_outliers": {
            "min_year": 2000,
            "max_year": 2050,
            "problematic_years": [1900, 1970, 9999],
            "min_suspicious_count": 5
        }}
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
            min_year_found = int(year_col['year'].min())
            examples = [str(val) if val else None for val in too_old[self.col].head(5).to_list()]

            # More actionable suggestion based on how old the dates are
            years_too_old = self.config.min_year - min_year_found
            if years_too_old > 100:
                suggestion = f'Dates from year {min_year_found} found ({years_too_old}y before threshold) - likely data errors or placeholder values'
            else:
                suggestion = f'Dates before {self.config.min_year} found (oldest: {min_year_found}) - verify if historical data is expected'

            results.append(CheckResult(
                type='DATES_TOO_OLD',
                count=len(too_old),
                pct=pct_old,
                min_year_found=min_year_found,
                threshold_year=self.config.min_year,
                suggestion=suggestion,
                examples=examples
            ))

        # Find dates after max_year
        too_future = non_null_df.filter(pl.col(self.col).dt.year() > self.config.max_year)
        if len(too_future) > 0:
            pct_future = len(too_future) / non_null_count * 100
            max_year_found = int(year_col['year'].max())
            examples = [str(val) if val else None for val in too_future[self.col].head(5).to_list()]

            # More actionable suggestion based on how far in the future
            years_too_future = max_year_found - self.config.max_year
            if max_year_found >= 9999:
                suggestion = f'Far-future dates (year {max_year_found}) found - likely placeholder for "never expires" or missing data'
            elif years_too_future > 100:
                suggestion = f'Dates in year {max_year_found} found ({years_too_future}y beyond threshold) - likely placeholder or data errors'
            else:
                suggestion = f'Dates after {self.config.max_year} found (latest: {max_year_found}) - verify if future dates are expected'

            results.append(CheckResult(
                type='DATES_TOO_FUTURE',
                count=len(too_future),
                pct=pct_future,
                max_year_found=max_year_found,
                threshold_year=self.config.max_year,
                suggestion=suggestion,
                examples=examples
            ))

        # Check for specific problematic years (only if within valid range to avoid duplicates)
        if self.config.problematic_years:
            year_counts = year_col.group_by('year').agg(pl.len().alias('count'))

            for problem_year in self.config.problematic_years:
                # Skip if already reported as too old/too future
                if problem_year < self.config.min_year or problem_year > self.config.max_year:
                    continue

                year_data = year_counts.filter(pl.col('year') == problem_year)
                if len(year_data) > 0:
                    count = int(year_data['count'][0])

                    # Only report if meets minimum threshold
                    if count >= self.config.min_suspicious_count:
                        pct = count / non_null_count * 100

                        # Convert dates/datetimes to formatted strings
                        examples = [str(val) if val else None for val in non_null_df.filter(
                            pl.col(self.col).dt.year() == problem_year
                        )[self.col].head(5).to_list()]

                        # Context-aware suggestions
                        if problem_year == 1900:
                            suggestion = f'Year 1900 found {count}x - common default for missing birthdates or legacy data'
                        elif problem_year == 1970:
                            suggestion = f'Year 1970 found {count}x - Unix epoch start, often indicates uninitialized timestamps'
                        elif problem_year in [2099, 2999, 9999]:
                            suggestion = f'Year {problem_year} found {count}x - common placeholder for "no expiration" or far-future dates'
                        else:
                            suggestion = f'Year {problem_year} found {count}x - often used as placeholder/default value'

                        results.append(CheckResult(
                            type='SUSPICIOUS_YEAR',
                            year=problem_year,
                            count=count,
                            pct=pct,
                            suggestion=suggestion,
                            examples=examples
                        ))

        return results
