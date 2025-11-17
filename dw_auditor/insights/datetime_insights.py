"""
DateTime column insights - composite insight for date and datetime types
"""

from typing import List
from pydantic import BaseModel
import polars as pl
from ..core.base_insight import BaseInsight, InsightResult
from ..core.plugin import register_plugin


class DatetimeInsightsParams(BaseModel):
    """Parameters for datetime insights"""
    min_date: bool = False
    max_date: bool = False
    date_range_days: bool = False
    most_common_dates: int = 0
    most_common_hours: int = 0
    most_common_days: int = 0
    most_common_timezones: int = 0


@register_plugin("datetime_insights", category="insight")
class DatetimeInsights(BaseInsight):
    """Composite insight for datetime and date columns

    Generates insights for datetime columns including:
    - Date range (min, max, span in days)
    - Most common dates
    - Most common hours (datetime only, not date)
    - Most common days of week
    - Timezone information
    """

    display_name = "Datetime Column Insights"
    supported_dtypes = [pl.Date, pl.Datetime, pl.Time, pl.Duration]

    def _validate_params(self) -> None:
        """Validate parameters using Pydantic"""
        self.config = DatetimeInsightsParams(**self.params)

    def generate(self) -> List[InsightResult]:
        """Generate datetime insights

        Returns:
            List of InsightResult objects for all requested metrics
        """
        results = []
        non_null_series = self._get_non_null_series()

        if len(non_null_series) == 0:
            return results

        # Check if this is a date-only column (vs datetime/timestamp)
        dtype = self.df[self.col].dtype
        is_date_only = dtype == pl.Date

        # Min date
        if self.config.min_date:
            min_date = non_null_series.min()
            results.append(
                InsightResult(
                    type='min_date',
                    value=str(min_date),
                    display_name='Earliest Date'
                )
            )

        # Max date
        if self.config.max_date:
            max_date = non_null_series.max()
            results.append(
                InsightResult(
                    type='max_date',
                    value=str(max_date),
                    display_name='Latest Date'
                )
            )

        # Date range in days
        if self.config.date_range_days:
            min_date = non_null_series.min()
            max_date = non_null_series.max()
            if min_date is not None and max_date is not None:
                date_range = max_date - min_date
                days = int(date_range.total_seconds() / 86400) if hasattr(date_range, 'total_seconds') else 0
                results.append(
                    InsightResult(
                        type='date_range_days',
                        value=days,
                        display_name='Date Range',
                        unit='days'
                    )
                )

        # Most common dates
        if self.config.most_common_dates > 0:
            total_non_null = len(non_null_series)

            value_counts = (
                self.df.select(pl.col(self.col))
                .filter(pl.col(self.col).is_not_null())
                .with_columns(pl.col(self.col).dt.date().alias('date_only'))
                .group_by('date_only')
                .agg(pl.count().alias('count'))
                .with_columns(
                    (pl.col('count') / total_non_null * 100).alias('percentage')
                )
                .sort('count', descending=True)
                .head(self.config.most_common_dates)
                .to_dicts()
            )

            common_dates = [
                {
                    'date': str(item['date_only']),
                    'count': item['count'],
                    'percentage': item['percentage']
                }
                for item in value_counts
            ]

            if common_dates:
                results.append(
                    InsightResult(
                        type='most_common_dates',
                        value=common_dates,
                        display_name=f'Top {self.config.most_common_dates} Dates'
                    )
                )

        # Most common hours (only for datetime/timestamp, not date)
        if self.config.most_common_hours > 0 and not is_date_only:
            total_non_null = len(non_null_series)

            hour_counts = (
                self.df.select(pl.col(self.col))
                .filter(pl.col(self.col).is_not_null())
                .with_columns(pl.col(self.col).dt.hour().alias('hour'))
                .group_by('hour')
                .agg(pl.count().alias('count'))
                .with_columns(
                    (pl.col('count') / total_non_null * 100).alias('percentage')
                )
                .sort('count', descending=True)
                .head(self.config.most_common_hours)
                .to_dicts()
            )

            common_hours = [
                {
                    'hour': item['hour'],
                    'count': item['count'],
                    'percentage': item['percentage']
                }
                for item in hour_counts
            ]

            if common_hours:
                results.append(
                    InsightResult(
                        type='most_common_hours',
                        value=common_hours,
                        display_name=f'Top {self.config.most_common_hours} Hours'
                    )
                )

        # Most common days of week
        if self.config.most_common_days > 0:
            total_non_null = len(non_null_series)

            # Polars weekday: Monday=1, Sunday=7
            day_names = {
                1: 'Monday', 2: 'Tuesday', 3: 'Wednesday', 4: 'Thursday',
                5: 'Friday', 6: 'Saturday', 7: 'Sunday'
            }

            day_counts = (
                self.df.select(pl.col(self.col))
                .filter(pl.col(self.col).is_not_null())
                .with_columns(pl.col(self.col).dt.weekday().alias('weekday'))
                .group_by('weekday')
                .agg(pl.count().alias('count'))
                .with_columns(
                    (pl.col('count') / total_non_null * 100).alias('percentage')
                )
                .sort('count', descending=True)
                .head(self.config.most_common_days)
                .to_dicts()
            )

            common_days = [
                {
                    'day': day_names.get(item['weekday'], f"Day {item['weekday']}"),
                    'weekday': item['weekday'],
                    'count': item['count'],
                    'percentage': item['percentage']
                }
                for item in day_counts
            ]

            if common_days:
                results.append(
                    InsightResult(
                        type='most_common_days',
                        value=common_days,
                        display_name=f'Top {self.config.most_common_days} Days of Week'
                    )
                )

        # Timezone information
        if self.config.most_common_timezones > 0:
            if hasattr(dtype, 'time_zone') and dtype.time_zone is not None:
                timezone_info = str(dtype.time_zone)
            else:
                timezone_info = 'None (timezone-naive)'

            results.append(
                InsightResult(
                    type='timezone',
                    value=timezone_info,
                    display_name='Timezone'
                )
            )

        return results
