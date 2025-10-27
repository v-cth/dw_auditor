"""
Future date detection check
"""

from pydantic import BaseModel, Field
from typing import List
from ..core.base_check import BaseCheck, CheckResult
from ..core.registry import register_check
import polars as pl
from datetime import datetime, date, timezone


class FutureDateParams(BaseModel):
    """Parameters for future date check

    Attributes:
        threshold_pct: Minimum percentage to report as issue (default: 0.0 = report all)
    """
    threshold_pct: float = Field(default=0.0, ge=0.0, le=100.0)


@register_check("future_dates")
class FutureDateCheck(BaseCheck):
    """Detect dates/datetimes that are in the future relative to current time

    Identifies dates after the current date/time, which may indicate:
    - Data entry errors
    - Scheduled future events (may be valid)
    - Clock/timezone issues

    Handles:
    - Date columns: compared against today's date
    - Datetime columns: compared against current datetime
    - Timezone-aware datetimes: compared against UTC now
    - Timezone-naive datetimes: compared against local now

    Configuration example:
        {"future_dates": {"threshold_pct": 0.0}}
    """

    display_name = "Future Date Detection"

    def _validate_params(self) -> None:
        """Validate future date parameters"""
        self.config = FutureDateParams(**self.params)

    async def run(self) -> List[CheckResult]:
        """Execute future date check

        Returns:
            List with single CheckResult if future dates found above threshold
        """
        results = []

        non_null_df = self._get_non_null_df()
        non_null_count = len(non_null_df)

        if non_null_count == 0:
            return results

        # Get current date/datetime based on column type
        col_dtype = self.df[self.col].dtype

        if col_dtype == pl.Date:
            # For Date columns, compare against today's date
            current_ref = date.today()
            future_rows = non_null_df.filter(pl.col(self.col) > current_ref)
        else:
            # For Datetime columns, compare against current datetime
            # Handle timezone-aware vs timezone-naive
            if hasattr(col_dtype, 'time_zone') and col_dtype.time_zone is not None:
                # Timezone-aware: use current UTC time with timezone
                current_ref = datetime.now(timezone.utc)
            else:
                # Timezone-naive: use current local time without timezone
                current_ref = datetime.now()

            future_rows = non_null_df.filter(pl.col(self.col) > current_ref)

        if len(future_rows) > 0:
            pct_future = len(future_rows) / non_null_count * 100

            # Only report if above threshold
            if pct_future >= self.config.threshold_pct:
                examples = future_rows[self.col].head(5).to_list()

                # Calculate how far into the future
                if col_dtype == pl.Date:
                    max_future_date = future_rows[self.col].max()
                    days_in_future = (max_future_date - current_ref).days if max_future_date else 0
                else:
                    max_future_date = future_rows[self.col].max()
                    if max_future_date:
                        if hasattr(col_dtype, 'time_zone') and col_dtype.time_zone is not None:
                            # For timezone-aware, both should be timezone-aware
                            time_diff = max_future_date - current_ref
                        else:
                            # For timezone-naive
                            time_diff = max_future_date - current_ref
                        days_in_future = time_diff.days
                    else:
                        days_in_future = 0

                results.append(CheckResult(
                    type='FUTURE_DATES',
                    count=len(future_rows),
                    pct=pct_future,
                    max_days_future=days_in_future,
                    current_reference=str(current_ref),
                    max_future_value=str(max_future_date) if max_future_date else None,
                    suggestion=f'Found {len(future_rows)} dates in the future - check if these are valid or data entry errors',
                    examples=examples
                ))

        return results
