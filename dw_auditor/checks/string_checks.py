"""
String-based data quality checks
"""

import polars as pl
from typing import List, Dict, Optional
import re


def _format_example_with_pk(row_data: Dict, col: str, primary_key_columns: Optional[List[str]] = None) -> str:
    """Format an example with primary key context"""
    if primary_key_columns and len(primary_key_columns) > 0:
        pk_values = []
        for pk_col in primary_key_columns:
            if pk_col in row_data:
                pk_values.append(f"{pk_col}={row_data[pk_col]}")
        if pk_values:
            return f"{row_data[col]} [{', '.join(pk_values)}]"
    return str(row_data[col])


def check_trailing_spaces(df: pl.DataFrame, col: str, primary_key_columns: Optional[List[str]] = None) -> List[Dict]:
    """Detect trailing or leading spaces"""
    issues = []

    # Check for leading spaces
    leading = df.filter(
        pl.col(col).is_not_null() &
        pl.col(col).str.starts_with(" ")
    )

    if len(leading) > 0:
        non_null_count = df[col].drop_nulls().len()
        examples = leading[col].head(3).to_list()
        issues.append({
            'type': 'LEADING_SPACES',
            'count': len(leading),
            'pct': len(leading) / non_null_count * 100,
            'examples': examples
        })

    # Check for trailing spaces
    trailing = df.filter(
        pl.col(col).is_not_null() &
        pl.col(col).str.ends_with(" ")
    )

    if len(trailing) > 0:
        non_null_count = df[col].drop_nulls().len()

        # Format examples with primary key context if available
        examples = []
        select_cols = [col] + (primary_key_columns if primary_key_columns else [])
        for row in trailing.select(select_cols).head(3).iter_rows(named=True):
            examples.append(f"'{_format_example_with_pk(row, col, primary_key_columns)}'")

        issues.append({
            'type': 'TRAILING_SPACES',
            'count': len(trailing),
            'pct': len(trailing) / non_null_count * 100,
            'examples': examples
        })

    return issues


def check_case_duplicates(df: pl.DataFrame, col: str, primary_key_columns: Optional[List[str]] = None) -> List[Dict]:
    """Detect values that differ only in case"""
    issues = []

    # Get non-null values and group by lowercase
    case_analysis = (
        df.select(pl.col(col))
        .filter(pl.col(col).is_not_null())
        .with_columns(pl.col(col).str.to_lowercase().alias('lower'))
        .group_by('lower')
        .agg(pl.col(col).unique().alias('variations'))
        .filter(pl.col('variations').list.len() > 1)
    )

    if len(case_analysis) > 0:
        examples = []
        for row in case_analysis.head(3).iter_rows(named=True):
            examples.append((row['lower'], row['variations']))

        issues.append({
            'type': 'CASE_DUPLICATES',
            'count': len(case_analysis),
            'examples': examples
        })

    return issues


def check_special_chars(df: pl.DataFrame, col: str, primary_key_columns: Optional[List[str]] = None, pattern: str = r'[^a-zA-Z0-9\s\.,\-_@]') -> List[Dict]:
    """Detect strings with special characters"""
    issues = []

    with_special = df.filter(
        pl.col(col).is_not_null() &
        pl.col(col).str.contains(pattern)
    )

    if len(with_special) > 0:
        non_null_count = df[col].drop_nulls().len()

        # Format examples with primary key context if available
        examples = []
        select_cols = [col] + (primary_key_columns if primary_key_columns else [])
        for row in with_special.select(select_cols).head(2).iter_rows(named=True):
            examples.append(_format_example_with_pk(row, col, primary_key_columns))

        # Extract unique special characters from examples
        special_chars = set()
        for val in with_special[col].head(100).to_list():
            if val:
                special_chars.update(re.findall(pattern, str(val)))

        issues.append({
            'type': 'SPECIAL_CHARACTERS',
            'count': len(with_special),
            'pct': len(with_special) / non_null_count * 100,
            'special_chars': list(special_chars)[:10],
            'examples': examples
        })

    return issues


def check_numeric_strings(df: pl.DataFrame, col: str, primary_key_columns: Optional[List[str]] = None, threshold: float = 0.8) -> List[Dict]:
    """Detect string columns that contain only numbers"""
    issues = []

    # Pattern for numeric strings (including decimals and negatives)
    numeric_pattern = r'^-?\d+\.?\d*$'

    non_null_df = df.filter(pl.col(col).is_not_null())
    non_null_count = len(non_null_df)

    if non_null_count == 0:
        return issues

    numeric_strings = non_null_df.filter(
        pl.col(col).str.contains(f'^{numeric_pattern}$')
    )

    # Only flag if a high percentage are numeric
    pct_numeric = len(numeric_strings) / non_null_count
    if pct_numeric > threshold:
        examples = numeric_strings[col].head(3).to_list()
        issues.append({
            'type': 'NUMERIC_STRINGS',
            'count': len(numeric_strings),
            'pct': pct_numeric * 100,
            'suggestion': 'Consider converting to numeric type',
            'examples': examples
        })

    return issues
