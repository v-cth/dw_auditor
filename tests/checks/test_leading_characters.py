"""
Tests for leading characters check
"""

import pytest
import polars as pl
from dw_auditor.checks.string_leading_check import LeadingCharactersCheck
from tests.conftest import assert_check_result, assert_no_results, assert_has_result_type


class TestLeadingCharactersCheck:
    """Test suite for LeadingCharactersCheck"""

    def test_default_patterns_punctuation(self):
        """Test detection of leading punctuation with default patterns"""
        df = pl.DataFrame({
            'id': [1, 2, 3, 4, 5, 6, 7, 8],
            'value': ['.hello', ',world', ';test', ':data', '!clean', '?text', ' sample', 'normal']
        })

        check = LeadingCharactersCheck(
            df=df,
            col='value',
            primary_key_columns=['id']
        )
        results = pytest.helpers.run_check_sync(check)

        # Should find multiple patterns
        assert len(results) > 0
        assert_has_result_type(results, 'LEADING_CHARACTERS')

    def test_custom_pattern_dot(self):
        """Test custom pattern (dot)"""
        df = pl.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'value': ['.hidden', '.config', 'normal', '.gitignore', 'file']
        })

        check = LeadingCharactersCheck(
            df=df,
            col='value',
            primary_key_columns=['id'],
            patterns=['.']
        )
        results = pytest.helpers.run_check_sync(check)

        assert_has_result_type(results, 'LEADING_CHARACTERS')
        result = [r for r in results if r.pattern == '.'][0]
        assert result.count == 3

    def test_multiple_patterns(self):
        """Test multiple custom patterns"""
        df = pl.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'value': ['_private', '#comment', '@mention', 'normal', '_another']
        })

        check = LeadingCharactersCheck(
            df=df,
            col='value',
            patterns=['_', '#', '@']
        )
        results = pytest.helpers.run_check_sync(check)

        patterns_found = {r.pattern for r in results}
        assert '_' in patterns_found
        assert '#' in patterns_found
        assert '@' in patterns_found

    def test_clean_data_no_issues(self, sample_string_df):
        """Test that clean data returns no results"""
        check = LeadingCharactersCheck(
            df=sample_string_df,
            col='clean',
            patterns=['.', ',', '#']
        )
        results = pytest.helpers.run_check_sync(check)

        assert_no_results(results)

    def test_with_nulls(self, sample_string_df):
        """Test handling of null values"""
        check = LeadingCharactersCheck(
            df=sample_string_df,
            col='with_nulls',
            patterns=[' ', '.']
        )
        results = pytest.helpers.run_check_sync(check)

        # Should not crash on nulls
        assert isinstance(results, list)

    def test_empty_dataframe(self, empty_df):
        """Test handling of empty DataFrame"""
        check = LeadingCharactersCheck(
            df=empty_df,
            col='value',
            patterns=['.']
        )
        results = pytest.helpers.run_check_sync(check)

        assert_no_results(results)

    def test_percentage_calculation(self):
        """Test that percentage is calculated correctly"""
        df = pl.DataFrame({
            'id': list(range(1, 21)),
            'value': ['.a', '.b', '.c', '.d', '.e'] + ['f'] * 15
        })

        check = LeadingCharactersCheck(
            df=df,
            col='value',
            patterns=['.']
        )
        results = pytest.helpers.run_check_sync(check)

        result = [r for r in results if r.pattern == '.'][0]
        assert result.count == 5
        assert result.pct == pytest.approx(25.0, rel=0.01)

    def test_string_pattern_converts_to_list(self):
        """Test that string patterns are converted to character list"""
        df = pl.DataFrame({
            'id': [1, 2, 3],
            'value': ['.hidden', ',comma', ';semicolon']
        })

        check = LeadingCharactersCheck(
            df=df,
            col='value',
            patterns='.,;'  # String, should convert to list
        )
        results = pytest.helpers.run_check_sync(check)

        # Should find all three patterns
        patterns_found = {r.pattern for r in results}
        assert '.' in patterns_found
        assert ',' in patterns_found
        assert ';' in patterns_found
