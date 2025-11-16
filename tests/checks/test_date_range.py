"""
Tests for date range check
"""

import pytest
import polars as pl
from datetime import datetime, date
from pydantic import ValidationError
from dw_auditor.checks.date_range_check import DateRangeCheck
from tests.conftest import assert_no_results, assert_has_result_type, get_result_by_type


class TestDateRangeCheck:
    """Test suite for DateRangeCheck"""

    def test_after_exclusive(self, sample_date_df):
        """Test exclusive lower bound (date > after)"""
        check = DateRangeCheck(
            df=sample_date_df,
            col='normal_dates',
            after='2020-06-01'
        )
        results = pytest.helpers.run_check_sync(check)

        # Should find dates <= 2020-06-01
        assert_has_result_type(results, 'DATE_NOT_AFTER')
        result = get_result_by_type(results, 'DATE_NOT_AFTER')
        assert result.count >= 1
        assert result.operator == '>'

    def test_after_or_equal_inclusive(self, sample_date_df):
        """Test inclusive lower bound (date >= after_or_equal)"""
        check = DateRangeCheck(
            df=sample_date_df,
            col='normal_dates',
            after_or_equal='2020-06-01'
        )
        results = pytest.helpers.run_check_sync(check)

        # Should find dates < 2020-06-01
        assert_has_result_type(results, 'DATE_NOT_AFTER_OR_EQUAL')
        result = get_result_by_type(results, 'DATE_NOT_AFTER_OR_EQUAL')
        assert result.operator == '>='

    def test_before_exclusive(self, sample_date_df):
        """Test exclusive upper bound (date < before)"""
        check = DateRangeCheck(
            df=sample_date_df,
            col='normal_dates',
            before='2021-01-01'
        )
        results = pytest.helpers.run_check_sync(check)

        # Should find dates >= 2021-01-01
        assert_has_result_type(results, 'DATE_NOT_BEFORE')
        result = get_result_by_type(results, 'DATE_NOT_BEFORE')
        assert result.operator == '<'

    def test_before_or_equal_inclusive(self, sample_date_df):
        """Test inclusive upper bound (date <= before_or_equal)"""
        check = DateRangeCheck(
            df=sample_date_df,
            col='normal_dates',
            before_or_equal='2021-12-31'
        )
        results = pytest.helpers.run_check_sync(check)

        # Should find dates > 2021-12-31
        assert_has_result_type(results, 'DATE_NOT_BEFORE_OR_EQUAL')
        result = get_result_by_type(results, 'DATE_NOT_BEFORE_OR_EQUAL')
        assert result.operator == '<='

    def test_both_bounds_range(self):
        """Test both lower and upper bounds together"""
        df = pl.DataFrame({
            'date': [
                datetime(2020, 1, 1),
                datetime(2020, 6, 15),
                datetime(2021, 3, 20),
                datetime(2022, 12, 31),
                datetime(2023, 7, 4)
            ]
        })

        check = DateRangeCheck(
            df=df,
            col='date',
            after_or_equal='2020-06-01',
            before_or_equal='2022-12-31'
        )
        results = pytest.helpers.run_check_sync(check)

        # Should find violations on both ends
        assert len(results) >= 1
        # Either 2020-01-01 is too early or 2023-07-04 is too late

    def test_all_dates_valid(self):
        """Test that valid dates return no results"""
        df = pl.DataFrame({
            'date': [
                datetime(2021, 1, 1),
                datetime(2021, 6, 15),
                datetime(2021, 12, 31)
            ]
        })

        check = DateRangeCheck(
            df=df,
            col='date',
            after='2020-12-31',
            before='2022-01-01'
        )
        results = pytest.helpers.run_check_sync(check)

        assert_no_results(results)

    def test_with_date_type(self, sample_date_df):
        """Test with Date type (not Datetime)"""
        check = DateRangeCheck(
            df=sample_date_df,
            col='date_only',
            after_or_equal='2020-01-01',
            before_or_equal='2023-12-31'
        )
        results = pytest.helpers.run_check_sync(check)

        # Should work with Date type
        assert isinstance(results, list)

    def test_with_nulls(self, sample_date_df):
        """Test handling of null values"""
        check = DateRangeCheck(
            df=sample_date_df,
            col='with_nulls',
            after='2020-01-01'
        )
        results = pytest.helpers.run_check_sync(check)

        # Should ignore nulls
        assert isinstance(results, list)

    def test_empty_dataframe(self, empty_df):
        """Test handling of empty DataFrame"""
        check = DateRangeCheck(
            df=empty_df,
            col='value',
            after='2020-01-01'
        )
        results = pytest.helpers.run_check_sync(check)

        assert_no_results(results)

    def test_all_null_column(self, all_null_df):
        """Test handling of all-null column"""
        check = DateRangeCheck(
            df=all_null_df,
            col='value',
            after='2020-01-01'
        )
        results = pytest.helpers.run_check_sync(check)

        assert_no_results(results)

    def test_percentage_calculation(self):
        """Test that percentage is calculated correctly"""
        df = pl.DataFrame({
            'date': [
                datetime(2020, 1, 1),
                datetime(2020, 2, 1),
                datetime(2021, 1, 1),
                datetime(2021, 2, 1),
                datetime(2021, 3, 1),
                datetime(2021, 4, 1),
                datetime(2021, 5, 1),
                datetime(2021, 6, 1),
                datetime(2021, 7, 1),
                datetime(2021, 8, 1)
            ]
        })

        check = DateRangeCheck(
            df=df,
            col='date',
            before='2021-01-01'
        )
        results = pytest.helpers.run_check_sync(check)

        result = get_result_by_type(results, 'DATE_NOT_BEFORE')
        # 8 dates >= 2021-01-01
        assert result.count == 8
        assert result.pct == pytest.approx(80.0, rel=0.01)

    def test_suggestion_text(self):
        """Test that suggestions are generated"""
        df = pl.DataFrame({
            'date': [datetime(2019, 1, 1)]
        })

        check = DateRangeCheck(
            df=df,
            col='date',
            after_or_equal='2020-01-01'
        )
        results = pytest.helpers.run_check_sync(check)

        result = get_result_by_type(results, 'DATE_NOT_AFTER_OR_EQUAL')
        assert result.suggestion is not None
        assert '2020-01-01' in result.suggestion

    def test_invalid_date_format_raises_error(self):
        """Test that invalid date format raises error"""
        df = pl.DataFrame({'date': [datetime(2020, 1, 1)]})

        with pytest.raises(ValidationError):
            DateRangeCheck(
                df=df,
                col='date',
                after='not-a-date'
            )

    def test_boundary_exact_date_after(self):
        """Test exact boundary with 'after' (exclusive)"""
        df = pl.DataFrame({
            'date': [datetime(2020, 6, 1)]
        })

        check = DateRangeCheck(
            df=df,
            col='date',
            after='2020-06-01'
        )
        results = pytest.helpers.run_check_sync(check)

        # Should flag because date == after (not > after)
        assert_has_result_type(results, 'DATE_NOT_AFTER')

    def test_boundary_exact_date_after_or_equal(self):
        """Test exact boundary with 'after_or_equal' (inclusive)"""
        df = pl.DataFrame({
            'date': [datetime(2020, 6, 1)]
        })

        check = DateRangeCheck(
            df=df,
            col='date',
            after_or_equal='2020-06-01'
        )
        results = pytest.helpers.run_check_sync(check)

        # Should not flag because date == after_or_equal (valid for >=)
        assert_no_results(results)

    def test_threshold_in_result(self):
        """Test that threshold is included in result"""
        df = pl.DataFrame({
            'date': [datetime(2019, 1, 1)]
        })

        check = DateRangeCheck(
            df=df,
            col='date',
            after='2020-01-01'
        )
        results = pytest.helpers.run_check_sync(check)

        result = get_result_by_type(results, 'DATE_NOT_AFTER')
        assert result.threshold == '2020-01-01'
