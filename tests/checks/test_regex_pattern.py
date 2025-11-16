"""
Tests for regex pattern check
"""

import pytest
import polars as pl
from pydantic import ValidationError
from dw_auditor.checks.string_regex_check import RegexPatternCheck
from tests.conftest import assert_no_results, assert_has_result_type, get_result_by_type


class TestRegexPatternCheck:
    """Test suite for RegexPatternCheck"""

    def test_contains_mode_finds_pattern(self):
        """Test contains mode finds rows with pattern"""
        df = pl.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'value': ['hello!', 'world?', 'clean', 'test#', 'data']
        })

        check = RegexPatternCheck(
            df=df,
            col='value',
            pattern=r'[!?#]',  # Special characters
            mode='contains'
        )
        results = pytest.helpers.run_check_sync(check)

        assert_has_result_type(results, 'REGEX_PATTERN')
        result = get_result_by_type(results, 'REGEX_PATTERN')
        assert result.count == 3  # hello!, world?, test#
        assert result.mode == 'contains'

    def test_match_mode_finds_invalid_format(self):
        """Test match mode finds rows NOT matching pattern"""
        df = pl.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'email': [
                'valid@example.com',
                'invalid',
                'also@valid.org',
                'not-email',
                'test@site.net'
            ]
        })

        check = RegexPatternCheck(
            df=df,
            col='email',
            pattern=r'[\w.-]+@[\w.-]+\.\w+',  # Simple email pattern
            mode='match'
        )
        results = pytest.helpers.run_check_sync(check)

        assert_has_result_type(results, 'REGEX_PATTERN')
        result = get_result_by_type(results, 'REGEX_PATTERN')
        assert result.count == 2  # invalid, not-email
        assert result.mode == 'match'

    def test_contains_special_characters(self):
        """Test finding special characters in strings"""
        df = pl.DataFrame({
            'value': ['clean_text', 'has space', 'has\ttab', 'normal', 'has\nnewline']
        })

        check = RegexPatternCheck(
            df=df,
            col='value',
            pattern=r'[\s\t\n]',  # Whitespace characters
            mode='contains'
        )
        results = pytest.helpers.run_check_sync(check)

        result = get_result_by_type(results, 'REGEX_PATTERN')
        assert result.count == 3  # has space, has\ttab, has\nnewline

    def test_match_phone_number_format(self):
        """Test validating phone number format"""
        df = pl.DataFrame({
            'phone': ['555-1234', '555-5678', '5551234', 'invalid', '555-9999']
        })

        check = RegexPatternCheck(
            df=df,
            col='phone',
            pattern=r'\d{3}-\d{4}',  # XXX-XXXX format
            mode='match'
        )
        results = pytest.helpers.run_check_sync(check)

        result = get_result_by_type(results, 'REGEX_PATTERN')
        assert result.count == 2  # 5551234, invalid

    def test_custom_description(self):
        """Test custom description is used"""
        df = pl.DataFrame({
            'value': ['hello!', 'world?', 'clean']
        })

        check = RegexPatternCheck(
            df=df,
            col='value',
            pattern=r'[!?]',
            mode='contains',
            description='Found unwanted punctuation'
        )
        results = pytest.helpers.run_check_sync(check)

        result = get_result_by_type(results, 'REGEX_PATTERN')
        assert result.description == 'Found unwanted punctuation'

    def test_default_description_contains(self):
        """Test default description for contains mode"""
        df = pl.DataFrame({
            'value': ['hello!', 'clean']
        })

        check = RegexPatternCheck(
            df=df,
            col='value',
            pattern=r'[!]',
            mode='contains'
        )
        results = pytest.helpers.run_check_sync(check)

        result = get_result_by_type(results, 'REGEX_PATTERN')
        assert 'containing pattern' in result.description.lower()

    def test_default_description_match(self):
        """Test default description for match mode"""
        df = pl.DataFrame({
            'value': ['invalid', 'also_invalid']
        })

        check = RegexPatternCheck(
            df=df,
            col='value',
            pattern=r'\d+',
            mode='match'
        )
        results = pytest.helpers.run_check_sync(check)

        result = get_result_by_type(results, 'REGEX_PATTERN')
        assert 'not matching pattern' in result.description.lower()

    def test_no_violations(self):
        """Test clean data returns no results"""
        df = pl.DataFrame({
            'value': ['clean', 'text', 'data', 'values']
        })

        check = RegexPatternCheck(
            df=df,
            col='value',
            pattern=r'[!@#$%]',
            mode='contains'
        )
        results = pytest.helpers.run_check_sync(check)

        assert_no_results(results)

    def test_with_nulls(self):
        """Test handling of null values"""
        df = pl.DataFrame({
            'value': ['hello!', None, 'world?', None, 'clean']
        })

        check = RegexPatternCheck(
            df=df,
            col='value',
            pattern=r'[!?]',
            mode='contains'
        )
        results = pytest.helpers.run_check_sync(check)

        # Should ignore nulls
        result = get_result_by_type(results, 'REGEX_PATTERN')
        assert result.count == 2  # hello!, world?

    def test_empty_dataframe(self, empty_df):
        """Test handling of empty DataFrame"""
        check = RegexPatternCheck(
            df=empty_df,
            col='value',
            pattern=r'\d+',
            mode='contains'
        )
        results = pytest.helpers.run_check_sync(check)

        assert_no_results(results)

    def test_all_null_column(self, all_null_df):
        """Test handling of all-null column"""
        check = RegexPatternCheck(
            df=all_null_df,
            col='value',
            pattern=r'\d+',
            mode='contains'
        )
        results = pytest.helpers.run_check_sync(check)

        assert_no_results(results)

    def test_percentage_calculation(self):
        """Test that percentage is calculated correctly"""
        df = pl.DataFrame({
            'value': ['a!', 'b!', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j']
        })

        check = RegexPatternCheck(
            df=df,
            col='value',
            pattern=r'!',
            mode='contains'
        )
        results = pytest.helpers.run_check_sync(check)

        result = get_result_by_type(results, 'REGEX_PATTERN')
        assert result.count == 2
        assert result.pct == pytest.approx(20.0, rel=0.01)

    def test_invalid_regex_raises_error(self):
        """Test that invalid regex pattern raises error"""
        df = pl.DataFrame({'value': ['test']})

        with pytest.raises(ValidationError):
            RegexPatternCheck(
                df=df,
                col='value',
                pattern=r'[invalid(',  # Invalid regex
                mode='contains'
            )

    def test_empty_pattern_raises_error(self):
        """Test that empty pattern raises error"""
        df = pl.DataFrame({'value': ['test']})

        with pytest.raises(ValidationError):
            RegexPatternCheck(
                df=df,
                col='value',
                pattern='',  # Empty pattern
                mode='contains'
            )

    def test_invalid_mode_raises_error(self):
        """Test that invalid mode raises error"""
        df = pl.DataFrame({'value': ['test']})

        with pytest.raises(ValidationError):
            RegexPatternCheck(
                df=df,
                col='value',
                pattern=r'\d+',
                mode='invalid_mode'
            )

    def test_email_pattern_sample_data(self, sample_string_df):
        """Test with email pattern on sample data"""
        check = RegexPatternCheck(
            df=sample_string_df,
            col='email_pattern',
            pattern=r'[\w.-]+@[\w.-]+\.\w+',
            mode='match'
        )
        results = pytest.helpers.run_check_sync(check)

        # Should find non-email values
        assert_has_result_type(results, 'REGEX_PATTERN')
