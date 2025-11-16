"""
Tests for future dates check
"""

import pytest
import polars as pl
from datetime import datetime, date, timedelta
from dw_auditor.checks.date_future_check import FutureDateCheck
from tests.conftest import assert_no_results, assert_has_result_type, get_result_by_type


class TestFutureDateCheck:
    """Test suite for FutureDateCheck"""

    def test_detects_future_dates(self):
        """Test detection of dates in the future"""
        future_date = date.today() + timedelta(days=365)
        df = pl.DataFrame({
            'date': [
                date.today() - timedelta(days=10),
                future_date,
                date.today() - timedelta(days=5),
                future_date + timedelta(days=30),
                date.today()
            ]
        })

        check = FutureDateCheck(
            df=df,
            col='date'
        )
        results = pytest.helpers.run_check_sync(check)

        # Should find 2 future dates
        assert_has_result_type(results, 'FUTURE_DATES')
        result = get_result_by_type(results, 'FUTURE_DATES')
        assert result.count == 2

    def test_detects_future_datetimes(self):
        """Test detection of datetimes in the future"""
        future_dt = datetime.now() + timedelta(days=10)
        df = pl.DataFrame({
            'timestamp': [
                datetime.now() - timedelta(hours=5),
                future_dt,
                datetime.now() - timedelta(days=1),
                future_dt + timedelta(days=5)
            ]
        })

        check = FutureDateCheck(
            df=df,
            col='timestamp'
        )
        results = pytest.helpers.run_check_sync(check)

        # Should find 2 future timestamps
        assert_has_result_type(results, 'FUTURE_DATES')
        result = get_result_by_type(results, 'FUTURE_DATES')
        assert result.count == 2

    def test_no_future_dates(self):
        """Test that past dates return no results"""
        df = pl.DataFrame({
            'date': [
                date.today() - timedelta(days=365),
                date.today() - timedelta(days=100),
                date.today() - timedelta(days=10),
                date.today()
            ]
        })

        check = FutureDateCheck(
            df=df,
            col='date'
        )
        results = pytest.helpers.run_check_sync(check)

        assert_no_results(results)

    def test_max_days_future_calculation_dates(self):
        """Test calculation of max days in future for Date type"""
        future_date = date.today() + timedelta(days=100)
        df = pl.DataFrame({
            'date': [
                date.today(),
                future_date,
                date.today() + timedelta(days=10)
            ]
        })

        check = FutureDateCheck(
            df=df,
            col='date'
        )
        results = pytest.helpers.run_check_sync(check)

        result = get_result_by_type(results, 'FUTURE_DATES')
        # Max days should be around 100
        assert result.max_days_future >= 99
        assert result.max_days_future <= 101

    def test_max_days_future_calculation_datetimes(self):
        """Test calculation of max days in future for Datetime type"""
        future_dt = datetime.now() + timedelta(days=50)
        df = pl.DataFrame({
            'timestamp': [
                datetime.now(),
                future_dt,
                datetime.now() + timedelta(days=5)
            ]
        })

        check = FutureDateCheck(
            df=df,
            col='timestamp'
        )
        results = pytest.helpers.run_check_sync(check)

        result = get_result_by_type(results, 'FUTURE_DATES')
        # Max days should be around 50
        assert result.max_days_future >= 49
        assert result.max_days_future <= 51

    def test_with_nulls(self):
        """Test handling of null values"""
        future_date = date.today() + timedelta(days=10)
        df = pl.DataFrame({
            'date': [
                date.today(),
                None,
                future_date,
                None,
                date.today() - timedelta(days=5)
            ]
        })

        check = FutureDateCheck(
            df=df,
            col='date'
        )
        results = pytest.helpers.run_check_sync(check)

        # Should ignore nulls and find 1 future date
        result = get_result_by_type(results, 'FUTURE_DATES')
        assert result.count == 1

    def test_empty_dataframe(self, empty_df):
        """Test handling of empty DataFrame"""
        check = FutureDateCheck(
            df=empty_df,
            col='value'
        )
        results = pytest.helpers.run_check_sync(check)

        assert_no_results(results)

    def test_all_null_column(self, all_null_df):
        """Test handling of all-null column"""
        check = FutureDateCheck(
            df=all_null_df,
            col='value'
        )
        results = pytest.helpers.run_check_sync(check)

        assert_no_results(results)

    def test_percentage_calculation(self):
        """Test that percentage is calculated correctly"""
        future_date = date.today() + timedelta(days=10)
        df = pl.DataFrame({
            'date': [future_date, future_date] + [date.today() - timedelta(days=i) for i in range(1, 9)]
        })

        check = FutureDateCheck(
            df=df,
            col='date'
        )
        results = pytest.helpers.run_check_sync(check)

        result = get_result_by_type(results, 'FUTURE_DATES')
        assert result.count == 2
        assert result.pct == pytest.approx(20.0, rel=0.01)

    def test_current_reference_included(self):
        """Test that current reference is included in result"""
        future_date = date.today() + timedelta(days=10)
        df = pl.DataFrame({
            'date': [future_date]
        })

        check = FutureDateCheck(
            df=df,
            col='date'
        )
        results = pytest.helpers.run_check_sync(check)

        result = get_result_by_type(results, 'FUTURE_DATES')
        assert result.current_reference is not None
        # Should contain today's date in some form
        assert str(date.today().year) in result.current_reference

    def test_max_future_value_included(self):
        """Test that max future value is included in result"""
        future_date = date.today() + timedelta(days=100)
        df = pl.DataFrame({
            'date': [
                date.today() + timedelta(days=10),
                future_date,
                date.today() + timedelta(days=50)
            ]
        })

        check = FutureDateCheck(
            df=df,
            col='date'
        )
        results = pytest.helpers.run_check_sync(check)

        result = get_result_by_type(results, 'FUTURE_DATES')
        assert result.max_future_value is not None
        # Should contain the furthest future date
        assert str(future_date) in result.max_future_value

    def test_suggestion_text(self):
        """Test that suggestion is generated"""
        future_date = date.today() + timedelta(days=10)
        df = pl.DataFrame({
            'date': [future_date]
        })

        check = FutureDateCheck(
            df=df,
            col='date'
        )
        results = pytest.helpers.run_check_sync(check)

        result = get_result_by_type(results, 'FUTURE_DATES')
        assert result.suggestion is not None
        assert 'future' in result.suggestion.lower()

    def test_examples_included(self):
        """Test that examples are included in results"""
        future_date = date.today() + timedelta(days=10)
        df = pl.DataFrame({
            'date': [future_date] * 10
        })

        check = FutureDateCheck(
            df=df,
            col='date'
        )
        results = pytest.helpers.run_check_sync(check)

        result = get_result_by_type(results, 'FUTURE_DATES')
        assert len(result.examples) > 0
        # Should show at most 5 examples
        assert len(result.examples) <= 5

    def test_today_not_flagged(self):
        """Test that today's date is not flagged as future"""
        df = pl.DataFrame({
            'date': [
                date.today(),
                date.today(),
                date.today()
            ]
        })

        check = FutureDateCheck(
            df=df,
            col='date'
        )
        results = pytest.helpers.run_check_sync(check)

        # Today should not be considered future
        assert_no_results(results)

    def test_now_not_flagged(self):
        """Test that current datetime is not flagged as future"""
        # Create timestamps very close to now
        now = datetime.now()
        df = pl.DataFrame({
            'timestamp': [
                now - timedelta(seconds=1),
                now - timedelta(seconds=2),
                now - timedelta(seconds=3)
            ]
        })

        check = FutureDateCheck(
            df=df,
            col='timestamp'
        )
        results = pytest.helpers.run_check_sync(check)

        # Past timestamps should not be flagged
        assert_no_results(results)
