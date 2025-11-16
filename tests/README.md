# Test Suite for DW Auditor

Comprehensive test suite covering all checks and core functionality.

## Structure

```
tests/
├── conftest.py                    # Shared fixtures and test utilities
├── checks/                        # Tests for all 10 check classes
│   ├── test_trailing_characters.py
│   ├── test_leading_characters.py
│   ├── test_case_duplicates.py
│   ├── test_regex_pattern.py
│   ├── test_numeric_range.py
│   ├── test_uniqueness.py
│   ├── test_date_range.py
│   ├── test_date_outliers.py
│   ├── test_future_dates.py
│   └── test_timestamp_patterns.py
└── core/
    └── test_type_converter.py     # Property-based tests for TypeConverter
```

## Installation

Install test dependencies:

```bash
source audit_env/bin/activate
pip install -e ".[test]"
```

## Running Tests

### Run all tests
```bash
pytest tests/
```

### Run with coverage
```bash
pytest tests/ --cov=dw_auditor --cov-report=html
```

### Run specific test file
```bash
pytest tests/checks/test_trailing_characters.py -v
```

### Run specific test class
```bash
pytest tests/checks/test_numeric_range.py::TestNumericRangeCheck -v
```

### Run specific test
```bash
pytest tests/checks/test_numeric_range.py::TestNumericRangeCheck::test_greater_than_exclusive -v
```

### Run property-based tests only
```bash
pytest tests/core/test_type_converter.py::TestTypeConverterPropertyBased -v
```

## Test Coverage

### Check Tests (10 files, ~250+ tests)

Each check has comprehensive tests covering:
- ✅ **Basic functionality** - Core check logic with various scenarios
- ✅ **Edge cases** - Empty DataFrames, all nulls, clean data
- ✅ **Null handling** - Proper null value handling
- ✅ **Percentage calculations** - Accurate percentage reporting
- ✅ **Primary key context** - Examples with PK information
- ✅ **Parameter validation** - Pydantic validation errors
- ✅ **Suggestion text** - Actionable error messages
- ✅ **Boundary conditions** - Inclusive/exclusive bounds
- ✅ **Multiple violations** - Multiple issue types in one check

**String Checks (4 files):**
- `test_trailing_characters.py` - Leading/trailing character detection
- `test_leading_characters.py` - Leading character/punctuation detection
- `test_case_duplicates.py` - Case-insensitive duplicate detection
- `test_regex_pattern.py` - Regex pattern validation (contains & match modes)

**Numeric Checks (1 file):**
- `test_numeric_range.py` - Range validation with 4 boundary types

**Date/Time Checks (4 files):**
- `test_date_range.py` - Date range validation with 4 boundary types
- `test_date_outliers.py` - Outlier detection (too old, too future, suspicious years)
- `test_future_dates.py` - Future date detection
- `test_timestamp_patterns.py` - Timestamp pattern detection (midnight, constant hour)

**Universal Checks (1 file):**
- `test_uniqueness.py` - Duplicate value detection

### TypeConverter Tests (1 file, ~70+ tests)

**Basic Unit Tests (~30 tests):**
- Integer conversion (string → int64)
- Float conversion (string → float64)
- Date conversion (ISO strings → Date)
- Datetime conversion (ISO strings → Datetime)
- Mixed data handling (below threshold)
- Null value handling
- Conversion order (int before float, datetime before date)
- Success rate tracking
- Custom thresholds
- Sample fraction configuration

**Property-Based Tests (~40+ tests using Hypothesis):**
- ✅ **Integer strings** → Always convert to int64
- ✅ **Float strings** → Convert to float64/int64
- ✅ **Random text** → Stays as string (if not numeric)
- ✅ **Partial nulls** → Don't prevent conversion
- ✅ **Mostly valid data** → Converts if above threshold
- ✅ **Deterministic behavior** → Same input = same output
- ✅ **ISO date strings** → Convert to Date type
- ✅ **Sample size** → Affects performance, not accuracy
- ✅ **Negative numbers** → Convert correctly
- ✅ **Value preservation** → Conversion preserves values

Property-based tests use Hypothesis to generate thousands of random test cases, ensuring robust behavior across a wide range of inputs.

## Fixtures

`conftest.py` provides shared fixtures:

### Sample Data Fixtures
- `sample_string_df` - String columns with various patterns
- `sample_numeric_df` - Numeric columns with ranges/outliers
- `sample_date_df` - Date/datetime columns with various scenarios
- `sample_convertible_df` - String columns that can be type-converted
- `empty_df` - Empty DataFrame for edge cases
- `all_null_df` - All-null DataFrame for edge cases

### Helper Functions
- `assert_check_result()` - Assert check result matches expectations
- `assert_no_results()` - Assert check returned no results
- `assert_has_result_type()` - Assert results contain specific type
- `get_result_by_type()` - Get specific result from list
- `pytest.helpers.run_check_sync()` - Run async checks synchronously

## Testing Philosophy

1. **Comprehensive Coverage**: Every check has tests for normal operation, edge cases, and error conditions
2. **Property-Based Testing**: TypeConverter uses Hypothesis for exhaustive testing with random data
3. **Real-World Scenarios**: Tests use realistic data patterns that mirror actual data quality issues
4. **Clear Assertions**: Each test has specific, focused assertions with clear failure messages
5. **Modular Design**: Fixtures and helpers promote DRY principles and maintainability

## CI/CD Integration

Tests are designed to run in CI/CD pipelines:

```yaml
# Example GitHub Actions workflow
- name: Run tests
  run: |
    pytest tests/ --cov=dw_auditor --cov-report=xml

- name: Upload coverage
  uses: codecov/codecov-action@v3
  with:
    file: ./coverage.xml
```

## Writing New Tests

### For a New Check

1. Create test file: `tests/checks/test_<check_name>.py`
2. Import check class and test utilities
3. Create test class: `TestCheckNameCheck`
4. Add tests for:
   - Basic functionality with sample data
   - Edge cases (empty, all nulls)
   - Null handling
   - Percentage calculations
   - Parameter validation
   - Suggestions and examples

Example:
```python
import pytest
from dw_auditor.checks.my_check import MyCheck
from tests.conftest import assert_has_result_type

class TestMyCheck:
    def test_basic_detection(self):
        df = pl.DataFrame({'value': [...]})
        check = MyCheck(df=df, col='value', param1='value')
        results = pytest.helpers.run_check_sync(check)
        assert_has_result_type(results, 'MY_RESULT_TYPE')
```

### For Core Functionality

1. Create test file: `tests/core/test_<module_name>.py`
2. Add unit tests for basic behavior
3. Add property-based tests with Hypothesis for exhaustive coverage

Example property-based test:
```python
from hypothesis import given, strategies as st

@given(st.lists(st.integers(), min_size=10))
def test_property_integers_convert(self, integers):
    # Test that any list of integers converts correctly
    df = pl.DataFrame({'value': [str(i) for i in integers]})
    result = converter.convert(df)
    assert result['value'].dtype == pl.Int64
```

## Troubleshooting

**Import errors**: Ensure package is installed: `pip install -e .`

**Async test errors**: Check `pytest-asyncio` is installed

**Hypothesis warnings**: Increase deadline for slow tests:
```python
@settings(deadline=None)
```

**Coverage gaps**: Run with `--cov-report=html` and open `htmlcov/index.html`

## Performance

- Basic check tests: ~0.1-0.5s each
- Property-based tests: ~1-5s each (Hypothesis generates many examples)
- Full suite: ~30-60s (depending on hardware)

To speed up development, run specific test files or use `-k` to filter tests:
```bash
pytest tests/checks/ -k "test_basic"  # Run only tests with "test_basic" in name
```
