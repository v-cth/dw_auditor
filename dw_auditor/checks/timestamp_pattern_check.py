"""
Timestamp pattern detection check
"""

from pydantic import BaseModel, Field
from typing import List
from ..core.base_check import BaseCheck, CheckResult
from ..core.plugin import register_plugin
import polars as pl


class TimestampPatternParams(BaseModel):
    """Parameters for timestamp pattern check

    No parameters required - always reports patterns if detected.
    """
    pass


@register_plugin("timestamp_patterns", category="check")
class TimestampPatternCheck(BaseCheck):
    """Detect timestamp patterns (same hour, effectively dates)

    Identifies timestamps that have suspicious patterns:
    - Constant hour: Timestamps share the same hour (suggests date-only data)
    - Always midnight: Timestamps are at 00:00:00 (suggests DATE type is more appropriate)

    Only checks Datetime columns - skips Date columns automatically.

    Always reports patterns if detected (no threshold).
    """

    display_name = "Timestamp Pattern Detection"
    supported_dtypes = [pl.Datetime, pl.Date]

    def _validate_params(self) -> None:
        """Validate timestamp pattern parameters"""
        self.config = TimestampPatternParams(**self.params)

    def run(self) -> List[CheckResult]:
        """Execute timestamp pattern check

        Returns:
            List of CheckResult objects for detected patterns
        """
        results = []

        non_null_df = self._get_non_null_df()
        non_null_count = len(non_null_df)

        if non_null_count == 0:
            return results

        # For Date columns, skip time checks
        if self.df[self.col].dtype == pl.Date:
            return results

        # Extract time components for Datetime
        time_analysis = non_null_df.select([
            pl.col(self.col).dt.hour().alias('hour'),
            pl.col(self.col).dt.minute().alias('minute'),
            pl.col(self.col).dt.second().alias('second'),
        ])

        # Check unique hours - report if most timestamps share same hour
        unique_hours = time_analysis['hour'].n_unique()

        if unique_hours <= 3:
            hour_counts = time_analysis.group_by('hour').agg(
                pl.len().alias('count')
            ).sort('count', descending=True)
            most_common = hour_counts.row(0, named=True)
            pct_same_hour = most_common['count'] / non_null_count

            # Report constant hour pattern
            # Convert datetimes to formatted strings
            examples = [str(val) if val else None for val in non_null_df[self.col].head(3).to_list()]
            results.append(CheckResult(
                type='CONSTANT_HOUR',
                hour=most_common['hour'],
                count=most_common['count'],
                pct=pct_same_hour * 100,
                suggestion='Timestamp appears to be date-only, consider using DATE type',
                examples=examples
            ))

        # Check if times are midnight - report if found
        midnight_count = time_analysis.filter(
            (pl.col('hour') == 0) &
            (pl.col('minute') == 0) &
            (pl.col('second') == 0)
        ).height

        if midnight_count > 0:
            midnight_pct = midnight_count / non_null_count
            # Convert datetimes to formatted strings
            examples = [str(val) if val else None for val in non_null_df[self.col].head(3).to_list()]
            results.append(CheckResult(
                type='ALWAYS_MIDNIGHT',
                count=midnight_count,
                pct=midnight_pct * 100,
                suggestion='All timestamps at midnight - use DATE type instead',
                examples=examples
            ))

        return results
