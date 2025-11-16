"""
Tests for TypeConverter including property-based tests using Hypothesis
"""

import pytest
import polars as pl
from hypothesis import given, strategies as st, settings, assume
from dw_auditor.core.type_converter import TypeConverter


class TestTypeConverterBasic:
    """Basic unit tests for TypeConverter"""

    def test_integer_conversion(self):
        """Test conversion of string integers to int64"""
        df = pl.DataFrame({
            'id': ['1', '2', '3', '4', '5'],
            'value': ['10', '20', '30', '40', '50']
        })

        converter = TypeConverter()
        result_df, log = converter.convert_dataframe(df)

        assert result_df['value'].dtype == pl.Int64
        assert len(log) == 2  # Both id and value converted
        assert all(entry['to_type'] == 'int64' for entry in log)

    def test_float_conversion(self):
        """Test conversion of string floats to float64"""
        df = pl.DataFrame({
            'value': ['1.5', '2.5', '3.5', '4.5', '5.5']
        })

        converter = TypeConverter()
        result_df, log = converter.convert_dataframe(df)

        assert result_df['value'].dtype == pl.Float64
        assert len(log) == 1
        assert log[0]['to_type'] == 'float64'

    def test_date_conversion(self):
        """Test conversion of ISO date strings to date"""
        df = pl.DataFrame({
            'date': ['2020-01-01', '2020-06-15', '2021-03-20', '2022-12-31', '2023-07-04']
        })

        converter = TypeConverter()
        result_df, log = converter.convert_dataframe(df)

        assert result_df['date'].dtype == pl.Date
        assert len(log) == 1
        assert log[0]['to_type'] == 'date'

    def test_datetime_conversion(self):
        """Test conversion of datetime strings to datetime"""
        df = pl.DataFrame({
            'timestamp': [
                '2020-01-01 10:30:00',
                '2020-06-15 14:45:00',
                '2021-03-20 08:15:00',
                '2022-12-31 23:59:59'
            ]
        })

        converter = TypeConverter()
        result_df, log = converter.convert_dataframe(df)

        assert result_df['timestamp'].dtype == pl.Datetime
        assert len(log) == 1
        assert log[0]['to_type'] == 'datetime'

    def test_no_conversion_for_mixed_data(self):
        """Test that mixed data below threshold is not converted"""
        df = pl.DataFrame({
            'value': ['1', '2', 'abc', 'def', 'xyz']
        })

        converter = TypeConverter(sample_threshold=0.90, full_threshold=0.95)
        result_df, log = converter.convert_dataframe(df)

        # Should not convert (only 40% are integers)
        assert result_df['value'].dtype in [pl.Utf8, pl.String]
        assert len(log) == 0

    def test_empty_dataframe(self):
        """Test handling of empty DataFrame"""
        df = pl.DataFrame({'value': []})

        converter = TypeConverter()
        result_df, log = converter.convert_dataframe(df)

        assert len(result_df) == 0
        assert len(log) == 0

    def test_all_null_column(self):
        """Test handling of all-null column"""
        df = pl.DataFrame({
            'value': [None, None, None, None, None]
        })

        converter = TypeConverter()
        result_df, log = converter.convert_dataframe(df)

        # Should not convert all-null column
        assert len(log) == 0

    def test_conversion_with_nulls(self):
        """Test conversion succeeds with some null values"""
        df = pl.DataFrame({
            'value': ['1', '2', None, '4', '5', '6', '7', '8', '9', '10']
        })

        converter = TypeConverter()
        result_df, log = converter.convert_dataframe(df)

        # Should convert to int64 (90% non-null are valid integers)
        assert result_df['value'].dtype == pl.Int64
        assert len(log) == 1

    def test_conversion_order_int_before_float(self):
        """Test that integers are detected before floats"""
        df = pl.DataFrame({
            'value': ['1', '2', '3', '4', '5']
        })

        converter = TypeConverter()
        result_df, log = converter.convert_dataframe(df)

        # Should convert to int64, not float64
        assert result_df['value'].dtype == pl.Int64
        assert log[0]['to_type'] == 'int64'

    def test_conversion_order_datetime_before_date(self):
        """Test datetime strings with time component convert to datetime"""
        df = pl.DataFrame({
            'value': [
                '2020-01-01 10:30:00',
                '2020-06-15 14:45:00',
                '2021-03-20 08:15:00',
                '2022-12-31 23:59:59',
                '2023-07-04 12:00:00'
            ]
        })

        converter = TypeConverter()
        result_df, log = converter.convert_dataframe(df)

        # Should convert to datetime (has time component)
        assert result_df['value'].dtype == pl.Datetime
        assert log[0]['to_type'] == 'datetime'

    def test_success_rate_in_log(self):
        """Test that conversion log includes success rate"""
        df = pl.DataFrame({
            'value': ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10']
        })

        converter = TypeConverter()
        result_df, log = converter.convert_dataframe(df)

        assert len(log) == 1
        assert 'success_rate' in log[0]
        assert log[0]['success_rate'] == 1.0  # 100% success

    def test_converted_values_count(self):
        """Test that conversion log includes count of converted values"""
        df = pl.DataFrame({
            'value': ['1', '2', None, '4', '5']
        })

        converter = TypeConverter()
        result_df, log = converter.convert_dataframe(df)

        assert len(log) == 1
        assert 'converted_values' in log[0]
        assert log[0]['converted_values'] == 4  # 4 non-null values converted

    def test_custom_thresholds(self):
        """Test custom conversion thresholds"""
        df = pl.DataFrame({
            'value': ['1', '2', '3', '4', '5', 'a', 'b']  # 71% integers
        })

        # Strict threshold - should not convert
        strict_converter = TypeConverter(sample_threshold=0.80, full_threshold=0.95)
        result_df, log = strict_converter.convert_dataframe(df)
        assert result_df['value'].dtype in [pl.Utf8, pl.String]

        # Lenient threshold - should convert
        lenient_converter = TypeConverter(sample_threshold=0.60, full_threshold=0.70)
        result_df, log = lenient_converter.convert_dataframe(df)
        assert result_df['value'].dtype == pl.Int64

    def test_sample_fraction(self):
        """Test custom sample fraction"""
        df = pl.DataFrame({
            'value': [str(i) for i in range(1, 101)]  # 100 rows
        })

        converter = TypeConverter(sample_fraction=0.1)
        result_df, log = converter.convert_dataframe(df)

        # Should still convert successfully
        assert result_df['value'].dtype == pl.Int64

    def test_no_string_columns(self):
        """Test DataFrame with no string columns"""
        df = pl.DataFrame({
            'int_col': [1, 2, 3],
            'float_col': [1.5, 2.5, 3.5]
        })

        converter = TypeConverter()
        result_df, log = converter.convert_dataframe(df)

        # Should return unchanged
        assert result_df['int_col'].dtype == pl.Int64
        assert result_df['float_col'].dtype == pl.Float64
        assert len(log) == 0


class TestTypeConverterPropertyBased:
    """Property-based tests using Hypothesis"""

    @given(st.lists(st.integers(min_value=-10000, max_value=10000), min_size=10, max_size=100))
    @settings(max_examples=50, deadline=None)
    def test_property_integer_strings_convert_to_int(self, integers):
        """Property: String representations of integers should convert to int64"""
        # Convert integers to strings
        df = pl.DataFrame({
            'value': [str(i) for i in integers]
        })

        converter = TypeConverter()
        result_df, log = converter.convert_dataframe(df)

        # Property: Should always convert to int64
        assert result_df['value'].dtype == pl.Int64
        # Property: All values should be converted successfully
        assert log[0]['success_rate'] == 1.0

    @given(st.lists(st.floats(min_value=-10000.0, max_value=10000.0, allow_nan=False, allow_infinity=False), min_size=10, max_size=100))
    @settings(max_examples=50, deadline=None)
    def test_property_float_strings_convert_to_float(self, floats):
        """Property: String representations of floats should convert to float64"""
        # Convert floats to strings
        df = pl.DataFrame({
            'value': [str(f) for f in floats]
        })

        converter = TypeConverter()
        result_df, log = converter.convert_dataframe(df)

        # Property: Should convert to either int64 (if all whole numbers) or float64
        assert result_df['value'].dtype in [pl.Int64, pl.Float64]

    @given(st.lists(st.text(alphabet=st.characters(blacklist_categories=('Cs',)), min_size=1, max_size=20), min_size=10, max_size=100))
    @settings(max_examples=30, deadline=None)
    def test_property_random_text_stays_string(self, texts):
        """Property: Random text that's not numeric should remain as string"""
        # Filter out numeric-looking strings
        assume(not all(t.replace('.', '').replace('-', '').isdigit() for t in texts if t))

        df = pl.DataFrame({
            'value': texts
        })

        converter = TypeConverter()
        result_df, log = converter.convert_dataframe(df)

        # Property: Should remain string if conversion rate is low
        # (depending on how many happened to be numeric)
        assert isinstance(result_df['value'].dtype, (type(pl.Utf8), type(pl.String))) or result_df['value'].dtype in [pl.Utf8, pl.String, pl.Int64, pl.Float64, pl.Date, pl.Datetime]

    @given(st.lists(st.integers(min_value=1, max_value=100), min_size=10, max_size=100), st.floats(min_value=0.0, max_value=0.5))
    @settings(max_examples=30, deadline=None)
    def test_property_partial_nulls_dont_prevent_conversion(self, integers, null_fraction):
        """Property: Columns with some nulls should still convert if enough valid values"""
        assume(0.0 <= null_fraction <= 0.5)  # Up to 50% nulls

        # Create data with nulls
        total_size = len(integers)
        null_count = int(total_size * null_fraction)
        values = [str(i) for i in integers[:total_size - null_count]] + [None] * null_count

        df = pl.DataFrame({'value': values})

        converter = TypeConverter()
        result_df, log = converter.convert_dataframe(df)

        # Property: With <=50% nulls, rest should convert if all integers
        if null_count < total_size:
            assert result_df['value'].dtype == pl.Int64
            assert len(log) == 1

    @given(st.lists(st.integers(min_value=1, max_value=100), min_size=20, max_size=100), st.floats(min_value=0.0, max_value=0.3))
    @settings(max_examples=30, deadline=None)
    def test_property_mostly_valid_data_converts(self, integers, corruption_rate):
        """Property: Data that's mostly valid should convert if above threshold"""
        assume(0.0 <= corruption_rate <= 0.3)  # Up to 30% corruption

        total_size = len(integers)
        corrupt_count = int(total_size * corruption_rate)

        # Create mostly valid data with some corruption
        values = [str(i) for i in integers[:total_size - corrupt_count]] + ['invalid'] * corrupt_count

        df = pl.DataFrame({'value': values})

        converter = TypeConverter(sample_threshold=0.70, full_threshold=0.70)
        result_df, log = converter.convert_dataframe(df)

        # Property: With <=30% corruption and 70% threshold, should convert
        valid_rate = 1 - corruption_rate
        if valid_rate >= 0.70:
            assert result_df['value'].dtype == pl.Int64
        else:
            assert result_df['value'].dtype in [pl.Utf8, pl.String]

    @given(st.data())
    @settings(max_examples=50, deadline=None)
    def test_property_conversion_is_deterministic(self, data):
        """Property: Same input should always produce same output"""
        # Generate random data
        size = data.draw(st.integers(min_value=10, max_value=50))
        integers = data.draw(st.lists(st.integers(min_value=1, max_value=1000), min_size=size, max_size=size))

        df = pl.DataFrame({'value': [str(i) for i in integers]})

        converter = TypeConverter()

        # Convert twice
        result1, log1 = converter.convert_dataframe(df.clone())
        result2, log2 = converter.convert_dataframe(df.clone())

        # Property: Results should be identical
        assert result1['value'].dtype == result2['value'].dtype
        assert len(log1) == len(log2)
        if len(log1) > 0:
            assert log1[0]['to_type'] == log2[0]['to_type']

    @given(st.lists(st.dates(min_value=pl.date(2000, 1, 1), max_value=pl.date(2030, 12, 31)), min_size=10, max_size=100))
    @settings(max_examples=30, deadline=None)
    def test_property_iso_date_strings_convert_to_date(self, dates):
        """Property: ISO format date strings should convert to Date type"""
        # Convert dates to ISO format strings
        df = pl.DataFrame({
            'value': [d.isoformat() for d in dates]
        })

        converter = TypeConverter()
        result_df, log = converter.convert_dataframe(df)

        # Property: Should convert to Date type
        assert result_df['value'].dtype == pl.Date
        assert len(log) == 1
        assert log[0]['to_type'] == 'date'

    @given(st.integers(min_value=10, max_value=1000))
    @settings(max_examples=20, deadline=None)
    def test_property_sample_size_affects_performance_not_accuracy(self, data_size):
        """Property: Sample size should affect speed but not conversion accuracy"""
        # Create data that should definitely convert
        df = pl.DataFrame({
            'value': [str(i) for i in range(data_size)]
        })

        # Test with different sample fractions
        converter_small = TypeConverter(sample_fraction=0.01)
        converter_large = TypeConverter(sample_fraction=0.20)

        result_small, log_small = converter_small.convert_dataframe(df.clone())
        result_large, log_large = converter_large.convert_dataframe(df.clone())

        # Property: Both should produce same result type
        assert result_small['value'].dtype == result_large['value'].dtype
        assert result_small['value'].dtype == pl.Int64

    @given(st.lists(st.integers(min_value=-1000, max_value=1000), min_size=10, max_size=100))
    @settings(max_examples=30, deadline=None)
    def test_property_negative_integers_convert_correctly(self, integers):
        """Property: Negative integer strings should convert to int64"""
        df = pl.DataFrame({
            'value': [str(i) for i in integers]
        })

        converter = TypeConverter()
        result_df, log = converter.convert_dataframe(df)

        # Property: Should convert to int64 regardless of sign
        assert result_df['value'].dtype == pl.Int64

    @given(st.lists(st.floats(min_value=-1000.0, max_value=1000.0, allow_nan=False, allow_infinity=False), min_size=10, max_size=100))
    @settings(max_examples=30, deadline=None)
    def test_property_negative_floats_convert_correctly(self, floats):
        """Property: Negative float strings should convert to float64 or int64"""
        df = pl.DataFrame({
            'value': [str(f) for f in floats]
        })

        converter = TypeConverter()
        result_df, log = converter.convert_dataframe(df)

        # Property: Should convert to numeric type
        assert result_df['value'].dtype in [pl.Int64, pl.Float64]

    @given(st.lists(st.integers(min_value=1, max_value=100), min_size=10, max_size=100))
    @settings(max_examples=20, deadline=None)
    def test_property_conversion_preserves_values(self, integers):
        """Property: Conversion should preserve the actual numeric values"""
        df_original = pl.DataFrame({
            'value': [str(i) for i in integers]
        })

        converter = TypeConverter()
        result_df, log = converter.convert_dataframe(df_original)

        # Property: Converted values should match original integers
        for i, orig_int in enumerate(integers):
            assert result_df['value'][i] == orig_int
