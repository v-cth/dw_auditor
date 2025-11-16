"""
Tests for numeric range check
"""

import pytest
import polars as pl
from pydantic import ValidationError
from dw_auditor.checks.numeric_range_check import NumericRangeCheck
from tests.conftest import assert_no_results, assert_has_result_type, get_result_by_type


class TestNumericRangeCheck:
    """Test suite for NumericRangeCheck"""

    def test_greater_than_exclusive(self, sample_numeric_df):
        """Test exclusive lower bound (value > threshold)"""
        check = NumericRangeCheck(
            df=sample_numeric_df,
            col='integers',
            greater_than=50
        )
        results = pytest.helpers.run_check_sync(check)

        # Should find values <= 50
        assert_has_result_type(results, 'VALUE_NOT_GREATER_THAN')
        result = get_result_by_type(results, 'VALUE_NOT_GREATER_THAN')
        assert result.count == 5  # 10, 20, 30, 40, 50
        assert result.threshold == 50
        assert result.operator == '>'

    def test_greater_than_or_equal_inclusive(self, sample_numeric_df):
        """Test inclusive lower bound (value >= threshold)"""
        check = NumericRangeCheck(
            df=sample_numeric_df,
            col='integers',
            greater_than_or_equal=50
        )
        results = pytest.helpers.run_check_sync(check)

        # Should find values < 50
        assert_has_result_type(results, 'VALUE_NOT_GREATER_OR_EQUAL')
        result = get_result_by_type(results, 'VALUE_NOT_GREATER_OR_EQUAL')
        assert result.count == 4  # 10, 20, 30, 40
        assert result.threshold == 50
        assert result.operator == '>='

    def test_less_than_exclusive(self, sample_numeric_df):
        """Test exclusive upper bound (value < threshold)"""
        check = NumericRangeCheck(
            df=sample_numeric_df,
            col='integers',
            less_than=50
        )
        results = pytest.helpers.run_check_sync(check)

        # Should find values >= 50
        assert_has_result_type(results, 'VALUE_NOT_LESS_THAN')
        result = get_result_by_type(results, 'VALUE_NOT_LESS_THAN')
        assert result.count == 6  # 50, 60, 70, 80, 90, 100
        assert result.threshold == 50
        assert result.operator == '<'

    def test_less_than_or_equal_inclusive(self, sample_numeric_df):
        """Test inclusive upper bound (value <= threshold)"""
        check = NumericRangeCheck(
            df=sample_numeric_df,
            col='integers',
            less_than_or_equal=50
        )
        results = pytest.helpers.run_check_sync(check)

        # Should find values > 50
        assert_has_result_type(results, 'VALUE_NOT_LESS_OR_EQUAL')
        result = get_result_by_type(results, 'VALUE_NOT_LESS_OR_EQUAL')
        assert result.count == 5  # 60, 70, 80, 90, 100
        assert result.threshold == 50
        assert result.operator == '<='

    def test_both_bounds_range(self):
        """Test both lower and upper bounds together"""
        df = pl.DataFrame({
            'value': [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110]
        })

        check = NumericRangeCheck(
            df=df,
            col='value',
            greater_than_or_equal=20,
            less_than_or_equal=80
        )
        results = pytest.helpers.run_check_sync(check)

        # Should find violations on both ends
        assert len(results) >= 2
        assert_has_result_type(results, 'VALUE_NOT_GREATER_OR_EQUAL')
        assert_has_result_type(results, 'VALUE_NOT_LESS_OR_EQUAL')

        lower_result = get_result_by_type(results, 'VALUE_NOT_GREATER_OR_EQUAL')
        upper_result = get_result_by_type(results, 'VALUE_NOT_LESS_OR_EQUAL')

        assert lower_result.count == 2  # 0, 10
        assert upper_result.count == 3  # 90, 100, 110

    def test_floats(self, sample_numeric_df):
        """Test with float values"""
        check = NumericRangeCheck(
            df=sample_numeric_df,
            col='floats',
            greater_than=5.0,
            less_than=8.0
        )
        results = pytest.helpers.run_check_sync(check)

        # Should find violations
        assert len(results) >= 1

    def test_negative_numbers(self, sample_numeric_df):
        """Test with negative numbers"""
        check = NumericRangeCheck(
            df=sample_numeric_df,
            col='with_negatives',
            greater_than_or_equal=0
        )
        results = pytest.helpers.run_check_sync(check)

        # Should find negative values
        assert_has_result_type(results, 'VALUE_NOT_GREATER_OR_EQUAL')
        result = get_result_by_type(results, 'VALUE_NOT_GREATER_OR_EQUAL')
        assert result.count == 2  # -10, -5

    def test_all_values_valid(self):
        """Test that valid data returns no results"""
        df = pl.DataFrame({
            'value': [50, 60, 70, 80, 90]
        })

        check = NumericRangeCheck(
            df=df,
            col='value',
            greater_than=40,
            less_than=100
        )
        results = pytest.helpers.run_check_sync(check)

        assert_no_results(results)

    def test_with_nulls(self, sample_numeric_df):
        """Test handling of null values"""
        check = NumericRangeCheck(
            df=sample_numeric_df,
            col='with_nulls',
            greater_than=20
        )
        results = pytest.helpers.run_check_sync(check)

        # Should ignore nulls
        assert isinstance(results, list)

    def test_empty_dataframe(self, empty_df):
        """Test handling of empty DataFrame"""
        check = NumericRangeCheck(
            df=empty_df,
            col='value',
            greater_than=0
        )
        results = pytest.helpers.run_check_sync(check)

        assert_no_results(results)

    def test_all_null_column(self, all_null_df):
        """Test handling of all-null column"""
        check = NumericRangeCheck(
            df=all_null_df,
            col='value',
            greater_than=0
        )
        results = pytest.helpers.run_check_sync(check)

        assert_no_results(results)

    def test_percentage_calculation(self):
        """Test that percentage is calculated correctly"""
        df = pl.DataFrame({
            'value': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        })

        check = NumericRangeCheck(
            df=df,
            col='value',
            less_than=5
        )
        results = pytest.helpers.run_check_sync(check)

        result = get_result_by_type(results, 'VALUE_NOT_LESS_THAN')
        assert result.count == 6  # 5, 6, 7, 8, 9, 10
        assert result.pct == pytest.approx(60.0, rel=0.01)

    def test_suggestion_text(self):
        """Test that suggestions are generated"""
        df = pl.DataFrame({
            'value': [1, 2, 3, 4, 5]
        })

        check = NumericRangeCheck(
            df=df,
            col='value',
            greater_than=10
        )
        results = pytest.helpers.run_check_sync(check)

        result = get_result_by_type(results, 'VALUE_NOT_GREATER_THAN')
        assert result.suggestion is not None
        assert '> 10' in result.suggestion

    def test_no_parameters_raises_error(self):
        """Test that at least one parameter is required"""
        df = pl.DataFrame({'value': [1, 2, 3]})

        with pytest.raises(ValidationError):
            NumericRangeCheck(
                df=df,
                col='value'
                # No parameters provided
            )
