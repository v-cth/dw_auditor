"""
Shared pytest fixtures and utilities for testing
"""

import pytest
import polars as pl
from datetime import datetime, date, timedelta


@pytest.fixture
def sample_string_df():
    """Sample DataFrame with various string test cases"""
    return pl.DataFrame({
        'id': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        'clean': ['hello', 'world', 'test', 'data', 'clean', 'text', 'sample', 'value', 'item', 'entry'],
        'trailing_space': ['hello ', 'world ', 'test', 'data ', 'clean', 'text ', 'sample', 'value ', 'item', 'entry'],
        'leading_space': [' hello', ' world', 'test', ' data', 'clean', ' text', 'sample', ' value', 'item', 'entry'],
        'both_space': [' hello ', ' world ', 'test', ' data ', 'clean', ' text ', 'sample', ' value ', 'item', 'entry'],
        'trailing_underscore': ['hello_', 'world_', 'test', 'data_', 'clean', 'text_', 'sample', 'value_', 'item', 'entry'],
        'mixed_case': ['Hello', 'HELLO', 'hello', 'World', 'WORLD', 'world', 'Test', 'TEST', 'test', 'Data'],
        'email_pattern': ['test@example.com', 'user@test.org', 'invalid', 'admin@domain.net', 'not-email',
                         'hello@world.com', 'bad.format', 'user2@site.io', 'text', 'name@company.com'],
        'numeric_strings': ['123', '456', '789', '101', 'abc', '999', '555', 'xyz', '888', '777'],
        'with_nulls': ['hello', None, 'world', None, 'test', 'data', None, 'value', 'item', None],
    })


@pytest.fixture
def sample_numeric_df():
    """Sample DataFrame with numeric test cases"""
    return pl.DataFrame({
        'id': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        'integers': [10, 20, 30, 40, 50, 60, 70, 80, 90, 100],
        'floats': [1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5, 10.5],
        'with_negatives': [-10, -5, 0, 5, 10, 15, 20, 25, 30, 35],
        'outliers': [1, 2, 3, 4, 5, 6, 7, 8, 9, 1000],
        'mixed_range': [0, 50, 100, 150, 200, -10, 25, 75, 125, 175],
        'with_nulls': [10, None, 30, None, 50, 60, None, 80, 90, None],
    })


@pytest.fixture
def sample_date_df():
    """Sample DataFrame with date/datetime test cases"""
    base_date = datetime(2020, 1, 1)

    return pl.DataFrame({
        'id': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        'normal_dates': [
            datetime(2020, 1, 1),
            datetime(2020, 6, 15),
            datetime(2021, 3, 20),
            datetime(2022, 12, 31),
            datetime(2023, 7, 4),
            datetime(2019, 2, 14),
            datetime(2020, 10, 31),
            datetime(2021, 5, 1),
            datetime(2022, 8, 15),
            datetime(2023, 11, 11),
        ],
        'old_dates': [
            datetime(1900, 1, 1),
            datetime(1970, 1, 1),
            datetime(2020, 1, 1),
            datetime(1900, 12, 31),
            datetime(2021, 5, 15),
            datetime(1930, 6, 1),
            datetime(2022, 3, 20),
            datetime(1940, 8, 10),
            datetime(2023, 7, 4),
            datetime(1920, 2, 29),
        ],
        'future_dates': [
            datetime(2150, 1, 1),
            datetime(2020, 1, 1),
            datetime(2999, 12, 31),
            datetime(2021, 5, 15),
            datetime(2200, 6, 1),
            datetime(2022, 3, 20),
            datetime(9999, 12, 31),
            datetime(2023, 7, 4),
            datetime(2100, 8, 15),
            datetime(2024, 1, 1),
        ],
        'placeholder_years': [
            datetime(1900, 1, 1),
            datetime(1900, 6, 15),
            datetime(1970, 1, 1),
            datetime(1970, 8, 20),
            datetime(2099, 12, 31),
            datetime(2099, 6, 1),
            datetime(9999, 12, 31),
            datetime(9999, 1, 1),
            datetime(2020, 5, 15),
            datetime(2021, 3, 10),
        ],
        'date_only': [
            date(2020, 1, 1),
            date(2020, 6, 15),
            date(2021, 3, 20),
            date(2022, 12, 31),
            date(2023, 7, 4),
            date(2019, 2, 14),
            date(2020, 10, 31),
            date(2021, 5, 1),
            date(2022, 8, 15),
            date(2023, 11, 11),
        ],
        'with_nulls': [
            datetime(2020, 1, 1),
            None,
            datetime(2021, 3, 20),
            None,
            datetime(2023, 7, 4),
            datetime(2019, 2, 14),
            None,
            datetime(2021, 5, 1),
            datetime(2022, 8, 15),
            None,
        ],
    })


@pytest.fixture
def sample_convertible_df():
    """Sample DataFrame with string columns that can be converted to various types"""
    return pl.DataFrame({
        'id': ['1', '2', '3', '4', '5'],
        'int_strings': ['10', '20', '30', '40', '50'],
        'float_strings': ['1.5', '2.5', '3.5', '4.5', '5.5'],
        'date_strings': ['2020-01-01', '2020-06-15', '2021-03-20', '2022-12-31', '2023-07-04'],
        'datetime_strings': [
            '2020-01-01 10:30:00',
            '2020-06-15 14:45:00',
            '2021-03-20 08:15:00',
            '2022-12-31 23:59:59',
            '2023-07-04 12:00:00',
        ],
        'mixed_ints': ['10', '20', 'abc', '40', '50'],
        'mixed_floats': ['1.5', '2.5', 'xyz', '4.5', '5.5'],
        'not_convertible': ['abc', 'def', 'ghi', 'jkl', 'mno'],
        'mostly_ints': ['1', '2', '3', '4', None],
        'sparse_ints': ['1', 'a', 'b', 'c', 'd'],
    })


@pytest.fixture
def empty_df():
    """Empty DataFrame for edge case testing"""
    return pl.DataFrame({
        'id': [],
        'value': [],
    })


@pytest.fixture
def all_null_df():
    """DataFrame with all null values"""
    return pl.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'value': [None, None, None, None, None],
    })


# Helper functions for tests

def assert_check_result(result, expected_type, expected_count=None, min_count=None):
    """
    Assert that a CheckResult matches expectations

    Args:
        result: CheckResult object
        expected_type: Expected result type
        expected_count: Exact expected count (optional)
        min_count: Minimum expected count (optional)
    """
    assert result.type == expected_type, f"Expected type {expected_type}, got {result.type}"

    if expected_count is not None:
        assert result.count == expected_count, f"Expected count {expected_count}, got {result.count}"

    if min_count is not None:
        assert result.count >= min_count, f"Expected count >= {min_count}, got {result.count}"


def assert_no_results(results):
    """Assert that check returned no results"""
    assert len(results) == 0, f"Expected no results, got {len(results)}: {results}"


def assert_has_result_type(results, result_type):
    """Assert that results contain a specific type"""
    types = [r.type for r in results]
    assert result_type in types, f"Expected {result_type} in results, got {types}"


def get_result_by_type(results, result_type):
    """Get a specific result by type"""
    for result in results:
        if result.type == result_type:
            return result
    return None
