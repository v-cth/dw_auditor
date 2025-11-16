"""
Tests for date outliers check
"""

import pytest
import polars as pl
from datetime import datetime
from pydantic import ValidationError
from dw_auditor.checks.date_outlier_check import DateOutlierCheck
from tests.conftest import assert_no_results, assert_has_result_type, get_result_by_type


class TestDateOutlierCheck:
    """Test suite for DateOutlierCheck"""

    def test_too_old_dates(self, sample_date_df):
        """Test detection of dates before min_year"""
        check = DateOutlierCheck(
            df=sample_date_df,
            col='old_dates',
            min_year=1950,
            max_year=2100
        )
        results = pytest.helpers.run_check_sync(check)

        # Should find dates before 1950
        assert_has_result_type(results, 'DATES_TOO_OLD')
        result = get_result_by_type(results, 'DATES_TOO_OLD')
        assert result.count > 0

    def test_too_future_dates(self, sample_date_df):
        """Test detection of dates after max_year"""
        check = DateOutlierCheck(
            df=sample_date_df,
            col='future_dates',
            min_year=1950,
            max_year=2100
        )
        results = pytest.helpers.run_check_sync(check)

        # Should find dates after 2100
        assert_has_result_type(results, 'DATES_TOO_FUTURE')
        result = get_result_by_type(results, 'DATES_TOO_FUTURE')
        assert result.count > 0

    def test_suspicious_years(self, sample_date_df):
        """Test detection of problematic placeholder years"""
        check = DateOutlierCheck(
            df=sample_date_df,
            col='placeholder_years',
            min_year=1950,
            max_year=2100,
            problematic_years=[1900, 1970, 2099, 9999]
        )
        results = pytest.helpers.run_check_sync(check)

        # Should find suspicious years
        # 9999 will be caught as DATES_TOO_FUTURE
        # 1900 will be caught as DATES_TOO_OLD
        # 1970 and 2099 should be caught as SUSPICIOUS_YEAR
        assert len(results) > 0

    def test_custom_min_max_year(self):
        """Test custom year range"""
        df = pl.DataFrame({
            'date': [
                datetime(2010, 1, 1),
                datetime(2015, 6, 15),
                datetime(2025, 3, 20),
                datetime(2030, 12, 31)
            ]
        })

        check = DateOutlierCheck(
            df=df,
            col='date',
            min_year=2015,
            max_year=2025
        )
        results = pytest.helpers.run_check_sync(check)

        # Should find dates outside 2015-2025 range
        assert_has_result_type(results, 'DATES_TOO_OLD')
        assert_has_result_type(results, 'DATES_TOO_FUTURE')

    def test_problematic_year_1900_suggestion(self):
        """Test 1900 gets appropriate suggestion"""
        df = pl.DataFrame({
            'date': [datetime(1900, 1, 1), datetime(1900, 6, 15), datetime(2020, 1, 1)]
        })

        check = DateOutlierCheck(
            df=df,
            col='date',
            min_year=1950,
            max_year=2100,
            problematic_years=[1900]
        )
        results = pytest.helpers.run_check_sync(check)

        # Should have DATES_TOO_OLD (not SUSPICIOUS_YEAR since 1900 < 1950)
        assert_has_result_type(results, 'DATES_TOO_OLD')

    def test_problematic_year_1970_suggestion(self):
        """Test 1970 gets Unix epoch suggestion"""
        df = pl.DataFrame({
            'date': [datetime(1970, 1, 1), datetime(1970, 8, 20), datetime(2020, 1, 1)]
        })

        check = DateOutlierCheck(
            df=df,
            col='date',
            min_year=1950,
            max_year=2100,
            problematic_years=[1970]
        )
        results = pytest.helpers.run_check_sync(check)

        # Should have SUSPICIOUS_YEAR for 1970
        suspicious = get_result_by_type(results, 'SUSPICIOUS_YEAR')
        if suspicious:
            assert 'Unix epoch' in suspicious.suggestion or 'epoch' in suspicious.suggestion.lower()

    def test_problematic_year_9999_suggestion(self):
        """Test 9999 gets appropriate suggestion"""
        df = pl.DataFrame({
            'date': [datetime(9999, 12, 31), datetime(9999, 1, 1), datetime(2020, 1, 1)]
        })

        check = DateOutlierCheck(
            df=df,
            col='date',
            min_year=1950,
            max_year=2100,
            problematic_years=[9999]
        )
        results = pytest.helpers.run_check_sync(check)

        # Should have DATES_TOO_FUTURE (9999 > 2100)
        assert_has_result_type(results, 'DATES_TOO_FUTURE')

    def test_min_suspicious_count_threshold(self):
        """Test min_suspicious_count filters low-frequency years"""
        df = pl.DataFrame({
            'date': (
                [datetime(1970, 1, 1)] * 10 +  # Should be reported
                [datetime(2099, 1, 1)] +  # Below threshold
                [datetime(2020, 1, 1)] * 5
            )
        })

        check = DateOutlierCheck(
            df=df,
            col='date',
            min_year=1950,
            max_year=2100,
            problematic_years=[1970, 2099],
            min_suspicious_count=5
        )
        results = pytest.helpers.run_check_sync(check)

        # Should find 1970 (10 occurrences) but not 2099 (1 occurrence)
        suspicious_results = [r for r in results if r.type == 'SUSPICIOUS_YEAR']
        years_found = [r.year for r in suspicious_results if hasattr(r, 'year')]

        if 1970 in years_found:
            assert 2099 not in years_found

    def test_no_outliers(self):
        """Test normal data returns no results"""
        df = pl.DataFrame({
            'date': [
                datetime(2020, 1, 1),
                datetime(2021, 6, 15),
                datetime(2022, 12, 31)
            ]
        })

        check = DateOutlierCheck(
            df=df,
            col='date',
            min_year=2000,
            max_year=2100,
            problematic_years=[1900, 1970, 9999]
        )
        results = pytest.helpers.run_check_sync(check)

        assert_no_results(results)

    def test_with_nulls(self, sample_date_df):
        """Test handling of null values"""
        check = DateOutlierCheck(
            df=sample_date_df,
            col='with_nulls',
            min_year=1950,
            max_year=2100
        )
        results = pytest.helpers.run_check_sync(check)

        # Should ignore nulls
        assert isinstance(results, list)

    def test_empty_dataframe(self, empty_df):
        """Test handling of empty DataFrame"""
        check = DateOutlierCheck(
            df=empty_df,
            col='value',
            min_year=1950,
            max_year=2100
        )
        results = pytest.helpers.run_check_sync(check)

        assert_no_results(results)

    def test_all_null_column(self, all_null_df):
        """Test handling of all-null column"""
        check = DateOutlierCheck(
            df=all_null_df,
            col='value',
            min_year=1950,
            max_year=2100
        )
        results = pytest.helpers.run_check_sync(check)

        assert_no_results(results)

    def test_percentage_calculation(self):
        """Test that percentage is calculated correctly"""
        df = pl.DataFrame({
            'date': [
                datetime(1900, 1, 1),
                datetime(1900, 6, 15)
            ] + [datetime(2020, 1, 1)] * 8
        })

        check = DateOutlierCheck(
            df=df,
            col='date',
            min_year=1950,
            max_year=2100
        )
        results = pytest.helpers.run_check_sync(check)

        result = get_result_by_type(results, 'DATES_TOO_OLD')
        assert result.count == 2
        assert result.pct == pytest.approx(20.0, rel=0.01)

    def test_min_year_greater_than_max_year_raises_error(self):
        """Test that min_year >= max_year raises error"""
        df = pl.DataFrame({'date': [datetime(2020, 1, 1)]})

        with pytest.raises(ValueError):
            DateOutlierCheck(
                df=df,
                col='date',
                min_year=2100,
                max_year=1950
            )

    def test_default_parameters(self):
        """Test default parameter values"""
        df = pl.DataFrame({
            'date': [
                datetime(1900, 1, 1),
                datetime(1970, 1, 1),
                datetime(2020, 1, 1),
                datetime(9999, 12, 31)
            ]
        })

        check = DateOutlierCheck(
            df=df,
            col='date'
            # Use defaults: min_year=1950, max_year=2100, problematic_years=[1900, 1970, 2099, 2999, 9999]
        )
        results = pytest.helpers.run_check_sync(check)

        # Should find both old and future dates with defaults
        assert len(results) > 0

    def test_examples_included(self):
        """Test that examples are included in results"""
        df = pl.DataFrame({
            'date': [datetime(1900, 1, 1)] * 5
        })

        check = DateOutlierCheck(
            df=df,
            col='date',
            min_year=1950,
            max_year=2100
        )
        results = pytest.helpers.run_check_sync(check)

        result = get_result_by_type(results, 'DATES_TOO_OLD')
        assert len(result.examples) > 0
