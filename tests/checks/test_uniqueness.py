"""
Tests for uniqueness check
"""

import pytest
import polars as pl
from dw_auditor.checks.uniqueness_check import UniquenessCheck
from tests.conftest import assert_no_results, assert_has_result_type, get_result_by_type


class TestUniquenessCheck:
    """Test suite for UniquenessCheck"""

    def test_simple_duplicates(self):
        """Test detection of simple duplicate values"""
        df = pl.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'value': ['A', 'B', 'A', 'C', 'B']
        })

        check = UniquenessCheck(
            df=df,
            col='value'
        )
        results = pytest.helpers.run_check_sync(check)

        assert_has_result_type(results, 'DUPLICATE_VALUES')
        result = get_result_by_type(results, 'DUPLICATE_VALUES')
        # Total duplicate rows: (2 - 1) + (2 - 1) = 2 duplicates
        assert result.count == 2
        assert result.distinct_duplicates == 2  # A and B are duplicated

    def test_multiple_duplicates_same_value(self):
        """Test value appearing multiple times"""
        df = pl.DataFrame({
            'id': [1, 2, 3, 4, 5, 6],
            'value': ['A', 'A', 'A', 'B', 'C', 'C']
        })

        check = UniquenessCheck(
            df=df,
            col='value'
        )
        results = pytest.helpers.run_check_sync(check)

        result = get_result_by_type(results, 'DUPLICATE_VALUES')
        # A appears 3 times (2 duplicates) + C appears 2 times (1 duplicate) = 3 total duplicates
        assert result.count == 3
        assert result.distinct_duplicates == 2  # A and C

    def test_all_unique_values(self):
        """Test that unique values return no results"""
        df = pl.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'value': ['A', 'B', 'C', 'D', 'E']
        })

        check = UniquenessCheck(
            df=df,
            col='value'
        )
        results = pytest.helpers.run_check_sync(check)

        assert_no_results(results)

    def test_numeric_duplicates(self):
        """Test with numeric values"""
        df = pl.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'value': [100, 200, 100, 300, 200]
        })

        check = UniquenessCheck(
            df=df,
            col='value'
        )
        results = pytest.helpers.run_check_sync(check)

        result = get_result_by_type(results, 'DUPLICATE_VALUES')
        assert result.count == 2  # 2 duplicate rows
        assert result.distinct_duplicates == 2  # 100 and 200

    def test_with_nulls(self):
        """Test that nulls are ignored"""
        df = pl.DataFrame({
            'id': [1, 2, 3, 4, 5, 6],
            'value': ['A', 'A', None, 'B', None, 'B']
        })

        check = UniquenessCheck(
            df=df,
            col='value'
        )
        results = pytest.helpers.run_check_sync(check)

        result = get_result_by_type(results, 'DUPLICATE_VALUES')
        # A and B both appear twice (ignore nulls)
        assert result.count == 2
        assert result.distinct_duplicates == 2

    def test_with_primary_keys(self):
        """Test examples include primary key context"""
        df = pl.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'category': ['A', 'B', 'A', 'C', 'B']
        })

        check = UniquenessCheck(
            df=df,
            col='category',
            primary_key_columns=['id']
        )
        results = pytest.helpers.run_check_sync(check)

        result = get_result_by_type(results, 'DUPLICATE_VALUES')
        # Examples should contain count and primary key info
        assert len(result.examples) > 0
        assert 'count=' in result.examples[0]
        assert 'id=' in result.examples[0]

    def test_examples_format(self):
        """Test that examples show counts"""
        df = pl.DataFrame({
            'value': ['A', 'A', 'A', 'B', 'B', 'C']
        })

        check = UniquenessCheck(
            df=df,
            col='value'
        )
        results = pytest.helpers.run_check_sync(check)

        result = get_result_by_type(results, 'DUPLICATE_VALUES')
        # Examples should show value with count
        assert len(result.examples) > 0
        assert 'count=' in result.examples[0]
        # First example should be A with count=3
        assert 'count=3' in result.examples[0]

    def test_top_5_most_frequent(self):
        """Test that top 5 duplicates are shown"""
        df = pl.DataFrame({
            'value': (
                ['A'] * 10 +  # Most frequent
                ['B'] * 8 +
                ['C'] * 6 +
                ['D'] * 4 +
                ['E'] * 3 +
                ['F'] * 2 +  # Should not appear in top 5
                ['G'] * 2
            )
        })

        check = UniquenessCheck(
            df=df,
            col='value'
        )
        results = pytest.helpers.run_check_sync(check)

        result = get_result_by_type(results, 'DUPLICATE_VALUES')
        # Should show at most 5 examples
        assert len(result.examples) <= 5
        # First should be A with highest count
        assert 'A' in result.examples[0]
        assert 'count=10' in result.examples[0]

    def test_empty_dataframe(self, empty_df):
        """Test handling of empty DataFrame"""
        check = UniquenessCheck(
            df=empty_df,
            col='value'
        )
        results = pytest.helpers.run_check_sync(check)

        assert_no_results(results)

    def test_all_null_column(self, all_null_df):
        """Test handling of all-null column"""
        check = UniquenessCheck(
            df=all_null_df,
            col='value'
        )
        results = pytest.helpers.run_check_sync(check)

        assert_no_results(results)

    def test_percentage_calculation(self):
        """Test that percentage is calculated correctly"""
        df = pl.DataFrame({
            'value': ['A', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I']
        })

        check = UniquenessCheck(
            df=df,
            col='value'
        )
        results = pytest.helpers.run_check_sync(check)

        result = get_result_by_type(results, 'DUPLICATE_VALUES')
        # 1 duplicate (A appears twice, so 1 extra row)
        assert result.count == 1
        # 1/10 = 10%
        assert result.pct == pytest.approx(10.0, rel=0.01)

    def test_suggestion_text(self):
        """Test that suggestion is generated"""
        df = pl.DataFrame({
            'value': ['A', 'A', 'B', 'B']
        })

        check = UniquenessCheck(
            df=df,
            col='value'
        )
        results = pytest.helpers.run_check_sync(check)

        result = get_result_by_type(results, 'DUPLICATE_VALUES')
        assert result.suggestion is not None
        assert 'unique values' in result.suggestion.lower()
        assert '2 distinct value(s)' in result.suggestion

    def test_date_duplicates(self):
        """Test with date values"""
        from datetime import date

        df = pl.DataFrame({
            'date': [
                date(2020, 1, 1),
                date(2020, 1, 1),
                date(2020, 6, 15),
                date(2020, 6, 15),
                date(2021, 1, 1)
            ]
        })

        check = UniquenessCheck(
            df=df,
            col='date'
        )
        results = pytest.helpers.run_check_sync(check)

        result = get_result_by_type(results, 'DUPLICATE_VALUES')
        assert result.count == 2  # 2 duplicate rows
        assert result.distinct_duplicates == 2  # 2 distinct dates duplicated
