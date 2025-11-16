"""
Tests for trailing/leading characters check
"""

import pytest
import polars as pl
from dw_auditor.checks.string_trailing_check import TrailingCharactersCheck
from tests.conftest import assert_check_result, assert_no_results, assert_has_result_type


class TestTrailingCharactersCheck:
    """Test suite for TrailingCharactersCheck"""

    def test_default_patterns_trailing_space(self, sample_string_df):
        """Test detection of trailing spaces with default patterns"""
        check = TrailingCharactersCheck(
            df=sample_string_df,
            col='trailing_space',
            primary_key_columns=['id']
        )
        results = pytest.helpers.run_check_sync(check)

        # Should find trailing spaces
        assert_has_result_type(results, 'TRAILING_CHARACTERS')
        trailing_result = [r for r in results if r.type == 'TRAILING_CHARACTERS' and r.pattern == ' '][0]
        assert trailing_result.count == 6

    def test_default_patterns_leading_space(self, sample_string_df):
        """Test detection of leading spaces with default patterns"""
        check = TrailingCharactersCheck(
            df=sample_string_df,
            col='leading_space',
            primary_key_columns=['id']
        )
        results = pytest.helpers.run_check_sync(check)

        # Should find leading spaces
        assert_has_result_type(results, 'LEADING_CHARACTERS')
        leading_result = [r for r in results if r.type == 'LEADING_CHARACTERS' and r.pattern == ' '][0]
        assert leading_result.count == 6

    def test_both_leading_and_trailing(self, sample_string_df):
        """Test detection of both leading and trailing spaces"""
        check = TrailingCharactersCheck(
            df=sample_string_df,
            col='both_space',
            primary_key_columns=['id']
        )
        results = pytest.helpers.run_check_sync(check)

        # Should find both leading and trailing
        assert len(results) >= 2
        assert_has_result_type(results, 'LEADING_CHARACTERS')
        assert_has_result_type(results, 'TRAILING_CHARACTERS')

    def test_custom_pattern_underscore(self, sample_string_df):
        """Test custom pattern (underscore)"""
        check = TrailingCharactersCheck(
            df=sample_string_df,
            col='trailing_underscore',
            primary_key_columns=['id'],
            patterns=['_']
        )
        results = pytest.helpers.run_check_sync(check)

        # Should find trailing underscores
        assert_has_result_type(results, 'TRAILING_CHARACTERS')
        result = [r for r in results if r.type == 'TRAILING_CHARACTERS'][0]
        assert result.count == 6

    def test_multiple_patterns(self):
        """Test multiple custom patterns"""
        df = pl.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'value': ['hello_', 'world ', 'test#', 'data_', 'clean']
        })

        check = TrailingCharactersCheck(
            df=df,
            col='value',
            primary_key_columns=['id'],
            patterns=['_', ' ', '#']
        )
        results = pytest.helpers.run_check_sync(check)

        # Should find all three patterns
        trailing_types = [r for r in results if r.type == 'TRAILING_CHARACTERS']
        patterns_found = {r.pattern for r in trailing_types}
        assert '_' in patterns_found
        assert ' ' in patterns_found
        assert '#' in patterns_found

    def test_clean_data_no_issues(self, sample_string_df):
        """Test that clean data returns no results"""
        check = TrailingCharactersCheck(
            df=sample_string_df,
            col='clean',
            primary_key_columns=['id']
        )
        results = pytest.helpers.run_check_sync(check)

        assert_no_results(results)

    def test_with_nulls(self, sample_string_df):
        """Test handling of null values"""
        check = TrailingCharactersCheck(
            df=sample_string_df,
            col='with_nulls',
            primary_key_columns=['id']
        )
        results = pytest.helpers.run_check_sync(check)

        # Should not crash on nulls
        # Nulls should be ignored in the check
        assert isinstance(results, list)

    def test_empty_dataframe(self, empty_df):
        """Test handling of empty DataFrame"""
        check = TrailingCharactersCheck(
            df=empty_df,
            col='value'
        )
        results = pytest.helpers.run_check_sync(check)

        assert_no_results(results)

    def test_all_null_column(self, all_null_df):
        """Test handling of all-null column"""
        check = TrailingCharactersCheck(
            df=all_null_df,
            col='value'
        )
        results = pytest.helpers.run_check_sync(check)

        assert_no_results(results)

    def test_string_pattern_converts_to_list(self):
        """Test that string patterns are converted to character list"""
        df = pl.DataFrame({
            'id': [1, 2, 3],
            'value': ['hello ', 'world\t', 'test\n']
        })

        check = TrailingCharactersCheck(
            df=df,
            col='value',
            patterns=' \t\n'  # String, should convert to list
        )
        results = pytest.helpers.run_check_sync(check)

        # Should find all three character types
        assert len(results) > 0

    def test_percentage_calculation(self):
        """Test that percentage is calculated correctly"""
        df = pl.DataFrame({
            'id': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            'value': ['a ', 'b ', 'c ', 'd ', 'e ', 'f', 'g', 'h', 'i', 'j']
        })

        check = TrailingCharactersCheck(
            df=df,
            col='value',
            patterns=[' ']
        )
        results = pytest.helpers.run_check_sync(check)

        result = [r for r in results if r.type == 'TRAILING_CHARACTERS'][0]
        assert result.count == 5
        assert result.pct == pytest.approx(50.0, rel=0.01)

    def test_examples_with_primary_keys(self):
        """Test that examples include primary key context"""
        df = pl.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'value': ['hello ', 'world ', 'test ', 'data ', 'clean']
        })

        check = TrailingCharactersCheck(
            df=df,
            col='value',
            primary_key_columns=['id'],
            patterns=[' ']
        )
        results = pytest.helpers.run_check_sync(check)

        result = [r for r in results if r.type == 'TRAILING_CHARACTERS'][0]
        assert len(result.examples) > 0
        # Examples should be quoted due to quote=True parameter
        assert all("'" in ex for ex in result.examples)


# Pytest helper plugin to run async checks synchronously
@pytest.fixture(scope='session', autouse=True)
def pytest_helpers():
    """Add helper methods to pytest"""
    import asyncio

    class Helpers:
        @staticmethod
        def run_check_sync(check):
            """Run async check synchronously"""
            return asyncio.run(check.run())

    pytest.helpers = Helpers()
