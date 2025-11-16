"""
Tests for case duplicates check
"""

import pytest
import polars as pl
from dw_auditor.checks.string_case_check import CaseDuplicatesCheck
from tests.conftest import assert_no_results, assert_has_result_type


class TestCaseDuplicatesCheck:
    """Test suite for CaseDuplicatesCheck"""

    def test_simple_case_duplicates(self):
        """Test detection of simple case variations"""
        df = pl.DataFrame({
            'id': [1, 2, 3, 4, 5, 6],
            'value': ['Paris', 'paris', 'PARIS', 'London', 'london', 'Berlin']
        })

        check = CaseDuplicatesCheck(
            df=df,
            col='value'
        )
        results = pytest.helpers.run_check_sync(check)

        assert_has_result_type(results, 'CASE_DUPLICATES')
        # Should find 2 groups: Paris (3 variations) and London (2 variations)
        assert results[0].count == 2

    def test_multiple_variations(self):
        """Test detection with many case variations"""
        df = pl.DataFrame({
            'id': [1, 2, 3, 4, 5, 6],
            'value': ['Product', 'product', 'PRODUCT', 'ProDuCt', 'pRoDuCt', 'unique']
        })

        check = CaseDuplicatesCheck(
            df=df,
            col='value'
        )
        results = pytest.helpers.run_check_sync(check)

        assert_has_result_type(results, 'CASE_DUPLICATES')
        # Should find 1 group: product with 5 variations
        assert results[0].count == 1

    def test_no_duplicates(self):
        """Test that unique values return no results"""
        df = pl.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'value': ['apple', 'banana', 'cherry', 'date', 'elderberry']
        })

        check = CaseDuplicatesCheck(
            df=df,
            col='value'
        )
        results = pytest.helpers.run_check_sync(check)

        assert_no_results(results)

    def test_all_same_case(self):
        """Test data with all same case (no duplicates)"""
        df = pl.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'value': ['hello', 'world', 'test', 'data', 'value']
        })

        check = CaseDuplicatesCheck(
            df=df,
            col='value'
        )
        results = pytest.helpers.run_check_sync(check)

        assert_no_results(results)

    def test_with_nulls(self):
        """Test handling of null values"""
        df = pl.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'value': ['Hello', 'hello', None, 'HELLO', None]
        })

        check = CaseDuplicatesCheck(
            df=df,
            col='value'
        )
        results = pytest.helpers.run_check_sync(check)

        # Should find Hello group ignoring nulls
        assert_has_result_type(results, 'CASE_DUPLICATES')

    def test_empty_dataframe(self, empty_df):
        """Test handling of empty DataFrame"""
        check = CaseDuplicatesCheck(
            df=empty_df,
            col='value'
        )
        results = pytest.helpers.run_check_sync(check)

        assert_no_results(results)

    def test_all_null_column(self, all_null_df):
        """Test handling of all-null column"""
        check = CaseDuplicatesCheck(
            df=all_null_df,
            col='value'
        )
        results = pytest.helpers.run_check_sync(check)

        assert_no_results(results)

    def test_examples_format(self):
        """Test that examples are formatted correctly"""
        df = pl.DataFrame({
            'id': [1, 2, 3],
            'value': ['Test', 'test', 'TEST']
        })

        check = CaseDuplicatesCheck(
            df=df,
            col='value'
        )
        results = pytest.helpers.run_check_sync(check)

        assert len(results[0].examples) > 0
        # Examples should be formatted as 'lowercase' → ['Variation1', 'Variation2']
        example = results[0].examples[0]
        assert '→' in example
        assert "'" in example

    def test_mixed_case_sample_data(self, sample_string_df):
        """Test with fixture data containing mixed case"""
        check = CaseDuplicatesCheck(
            df=sample_string_df,
            col='mixed_case'
        )
        results = pytest.helpers.run_check_sync(check)

        # Should find case duplicates in mixed_case column
        assert_has_result_type(results, 'CASE_DUPLICATES')
