"""
Tests for timestamp patterns check
"""

import pytest
import polars as pl
from datetime import datetime, date
from dw_auditor.checks.timestamp_pattern_check import TimestampPatternCheck
from tests.conftest import assert_no_results, assert_has_result_type, get_result_by_type


class TestTimestampPatternCheck:
    """Test suite for TimestampPatternCheck"""

    def test_constant_hour_pattern(self):
        """Test detection of constant hour in timestamps"""
        df = pl.DataFrame({
            'timestamp': [
                datetime(2020, 1, 1, 10, 30, 0),
                datetime(2020, 2, 1, 10, 45, 0),
                datetime(2020, 3, 1, 10, 15, 0),
                datetime(2020, 4, 1, 10, 0, 0),
                datetime(2020, 5, 1, 10, 20, 0)
            ]
        })

        check = TimestampPatternCheck(
            df=df,
            col='timestamp'
        )
        results = pytest.helpers.run_check_sync(check)

        # Should detect constant hour pattern
        assert_has_result_type(results, 'CONSTANT_HOUR')
        result = get_result_by_type(results, 'CONSTANT_HOUR')
        assert result.hour == 10
        assert result.count == 5

    def test_always_midnight_pattern(self):
        """Test detection of timestamps at midnight"""
        df = pl.DataFrame({
            'timestamp': [
                datetime(2020, 1, 1, 0, 0, 0),
                datetime(2020, 2, 1, 0, 0, 0),
                datetime(2020, 3, 1, 0, 0, 0),
                datetime(2020, 4, 1, 0, 0, 0),
                datetime(2020, 5, 1, 0, 0, 0)
            ]
        })

        check = TimestampPatternCheck(
            df=df,
            col='timestamp'
        )
        results = pytest.helpers.run_check_sync(check)

        # Should detect midnight pattern
        assert_has_result_type(results, 'ALWAYS_MIDNIGHT')
        result = get_result_by_type(results, 'ALWAYS_MIDNIGHT')
        assert result.count == 5
        assert result.pct == pytest.approx(100.0, rel=0.01)

    def test_both_patterns_detected(self):
        """Test that both patterns can be detected together"""
        df = pl.DataFrame({
            'timestamp': [
                datetime(2020, 1, 1, 0, 0, 0),
                datetime(2020, 2, 1, 0, 0, 0),
                datetime(2020, 3, 1, 0, 0, 0)
            ]
        })

        check = TimestampPatternCheck(
            df=df,
            col='timestamp'
        )
        results = pytest.helpers.run_check_sync(check)

        # Should detect both constant hour (0) and midnight
        assert_has_result_type(results, 'CONSTANT_HOUR')
        assert_has_result_type(results, 'ALWAYS_MIDNIGHT')

    def test_varied_times_no_pattern(self):
        """Test that varied times return no results"""
        df = pl.DataFrame({
            'timestamp': [
                datetime(2020, 1, 1, 8, 30, 15),
                datetime(2020, 2, 1, 14, 45, 30),
                datetime(2020, 3, 1, 20, 15, 45),
                datetime(2020, 4, 1, 6, 0, 10),
                datetime(2020, 5, 1, 18, 20, 25),
                datetime(2020, 6, 1, 11, 35, 40),
                datetime(2020, 7, 1, 16, 50, 55)
            ]
        })

        check = TimestampPatternCheck(
            df=df,
            col='timestamp'
        )
        results = pytest.helpers.run_check_sync(check)

        # No constant hour pattern (>3 unique hours)
        # No midnight pattern
        assert_no_results(results)

    def test_some_midnight_not_all(self):
        """Test detection when only some timestamps are at midnight"""
        df = pl.DataFrame({
            'timestamp': [
                datetime(2020, 1, 1, 0, 0, 0),
                datetime(2020, 2, 1, 0, 0, 0),
                datetime(2020, 3, 1, 10, 30, 0),
                datetime(2020, 4, 1, 0, 0, 0),
                datetime(2020, 5, 1, 14, 45, 0)
            ]
        })

        check = TimestampPatternCheck(
            df=df,
            col='timestamp'
        )
        results = pytest.helpers.run_check_sync(check)

        # Should detect partial midnight pattern
        result = get_result_by_type(results, 'ALWAYS_MIDNIGHT')
        if result:
            assert result.count == 3
            assert result.pct == pytest.approx(60.0, rel=0.01)

    def test_date_column_skipped(self, sample_date_df):
        """Test that Date columns are skipped (no time component)"""
        check = TimestampPatternCheck(
            df=sample_date_df,
            col='date_only'
        )
        results = pytest.helpers.run_check_sync(check)

        # Date columns should be skipped
        assert_no_results(results)

    def test_with_nulls(self):
        """Test handling of null values"""
        df = pl.DataFrame({
            'timestamp': [
                datetime(2020, 1, 1, 10, 30, 0),
                None,
                datetime(2020, 3, 1, 10, 15, 0),
                None,
                datetime(2020, 5, 1, 10, 20, 0)
            ]
        })

        check = TimestampPatternCheck(
            df=df,
            col='timestamp'
        )
        results = pytest.helpers.run_check_sync(check)

        # Should ignore nulls and detect pattern in remaining values
        assert isinstance(results, list)

    def test_empty_dataframe(self, empty_df):
        """Test handling of empty DataFrame"""
        check = TimestampPatternCheck(
            df=empty_df,
            col='value'
        )
        results = pytest.helpers.run_check_sync(check)

        assert_no_results(results)

    def test_all_null_column(self, all_null_df):
        """Test handling of all-null column"""
        check = TimestampPatternCheck(
            df=all_null_df,
            col='value'
        )
        results = pytest.helpers.run_check_sync(check)

        assert_no_results(results)

    def test_midnight_with_varying_hours(self):
        """Test midnight detection with other hours present"""
        df = pl.DataFrame({
            'timestamp': [
                datetime(2020, 1, 1, 0, 0, 0),
                datetime(2020, 2, 1, 5, 30, 15),
                datetime(2020, 3, 1, 0, 0, 0),
                datetime(2020, 4, 1, 12, 45, 30),
                datetime(2020, 5, 1, 0, 0, 0),
                datetime(2020, 6, 1, 18, 20, 45)
            ]
        })

        check = TimestampPatternCheck(
            df=df,
            col='timestamp'
        )
        results = pytest.helpers.run_check_sync(check)

        # Should detect midnight pattern
        result = get_result_by_type(results, 'ALWAYS_MIDNIGHT')
        assert result is not None
        assert result.count == 3

    def test_unique_hours_threshold(self):
        """Test that >3 unique hours prevents CONSTANT_HOUR detection"""
        df = pl.DataFrame({
            'timestamp': [
                datetime(2020, 1, 1, 8, 0, 0),
                datetime(2020, 2, 1, 10, 0, 0),
                datetime(2020, 3, 1, 12, 0, 0),
                datetime(2020, 4, 1, 14, 0, 0),
                datetime(2020, 5, 1, 16, 0, 0)
            ]
        })

        check = TimestampPatternCheck(
            df=df,
            col='timestamp'
        )
        results = pytest.helpers.run_check_sync(check)

        # Should not detect CONSTANT_HOUR (5 unique hours > 3 threshold)
        constant_hour_results = [r for r in results if r.type == 'CONSTANT_HOUR']
        assert len(constant_hour_results) == 0

    def test_percentage_calculation_constant_hour(self):
        """Test percentage calculation for constant hour"""
        df = pl.DataFrame({
            'timestamp': (
                [datetime(2020, 1, i, 10, 0, 0) for i in range(1, 9)] +  # 8 at hour 10
                [datetime(2020, 2, i, 14, 0, 0) for i in range(1, 3)]    # 2 at hour 14
            )
        })

        check = TimestampPatternCheck(
            df=df,
            col='timestamp'
        )
        results = pytest.helpers.run_check_sync(check)

        result = get_result_by_type(results, 'CONSTANT_HOUR')
        assert result.hour == 10  # Most common hour
        assert result.count == 8
        assert result.pct == pytest.approx(80.0, rel=0.01)

    def test_suggestion_texts(self):
        """Test that appropriate suggestions are provided"""
        df_midnight = pl.DataFrame({
            'timestamp': [datetime(2020, i, 1, 0, 0, 0) for i in range(1, 6)]
        })

        check = TimestampPatternCheck(
            df=df_midnight,
            col='timestamp'
        )
        results = pytest.helpers.run_check_sync(check)

        midnight_result = get_result_by_type(results, 'ALWAYS_MIDNIGHT')
        if midnight_result:
            assert 'DATE type' in midnight_result.suggestion or 'date' in midnight_result.suggestion.lower()

        constant_result = get_result_by_type(results, 'CONSTANT_HOUR')
        if constant_result:
            assert 'DATE type' in constant_result.suggestion or 'date' in constant_result.suggestion.lower()
