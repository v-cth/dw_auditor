"""
String-based data quality checks
"""

import polars as pl
from typing import List, Dict, Optional, Union
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


def check_trailing_characters(df: pl.DataFrame, col: str, primary_key_columns: Optional[List[str]] = None, patterns: Union[str, List[str]] = None) -> List[Dict]:
    """Detect trailing or leading characters/strings

    Args:
        df: DataFrame to check
        col: Column name to check
        primary_key_columns: Optional primary key columns for context
        patterns: Characters or strings to check for. Can be:
                 - Single string of characters: " \t" (checks each character individually)
                 - List of strings: [" ", "_dim", "_tmp"] (checks for each pattern)
                 Default: [" ", "\t", "\n", "\r"] (whitespace characters)

    Returns:
        List of issue dictionaries
    """
    issues = []
    non_null_count = df[col].drop_nulls().len()

    if non_null_count == 0:
        return issues

    # Default patterns if none provided
    if patterns is None:
        patterns = [" ", "\t", "\n", "\r"]
    elif isinstance(patterns, str):
        # Convert string to list of individual characters
        patterns = list(patterns)

    # Check for leading patterns
    for pattern in patterns:
        escaped_pattern = re.escape(pattern)
        leading_regex = f"^{escaped_pattern}"

        leading = df.filter(
            pl.col(col).is_not_null() &
            pl.col(col).str.contains(leading_regex)
        )

        if len(leading) > 0:
            # Format examples with primary key context if available
            examples = []
            select_cols = [col] + (primary_key_columns if primary_key_columns else [])
            for row in leading.select(select_cols).head(3).iter_rows(named=True):
                examples.append(f"'{_format_example_with_pk(row, col, primary_key_columns)}'")

            issues.append({
                'type': 'LEADING_CHARACTERS',
                'pattern': pattern,
                'count': len(leading),
                'pct': len(leading) / non_null_count * 100,
                'examples': examples
            })

    # Check for trailing patterns
    for pattern in patterns:
        escaped_pattern = re.escape(pattern)
        trailing_regex = f"{escaped_pattern}$"

        trailing = df.filter(
            pl.col(col).is_not_null() &
            pl.col(col).str.contains(trailing_regex)
        )

        if len(trailing) > 0:
            # Format examples with primary key context if available
            examples = []
            select_cols = [col] + (primary_key_columns if primary_key_columns else [])
            for row in trailing.select(select_cols).head(3).iter_rows(named=True):
                examples.append(f"'{_format_example_with_pk(row, col, primary_key_columns)}'")

            issues.append({
                'type': 'TRAILING_CHARACTERS',
                'pattern': pattern,
                'count': len(trailing),
                'pct': len(trailing) / non_null_count * 100,
                'examples': examples
            })

    return issues


def check_ending_characters(df: pl.DataFrame, col: str, primary_key_columns: Optional[List[str]] = None, patterns: Union[str, List[str]] = None) -> List[Dict]:
    """Detect strings that end with specific characters or patterns

    Args:
        df: DataFrame to check
        col: Column name to check
        primary_key_columns: Optional primary key columns for context
        patterns: Characters or strings to check for at the end. Can be:
                 - Single string of characters: ".,;" (checks each character individually)
                 - List of strings: [".", "_tmp", "!"] (checks for each pattern)
                 Default: [".", ",", ";", ":", "!", "?"] (punctuation)

    Returns:
        List of issue dictionaries with pattern-specific counts
    """
    issues = []
    non_null_count = df[col].drop_nulls().len()

    if non_null_count == 0:
        return issues

    # Default patterns if none provided
    if patterns is None:
        patterns = [".", ",", ";", ":", "!", "?"]
    elif isinstance(patterns, str):
        # Convert string to list of individual characters
        patterns = list(patterns)

    # Check for each ending pattern separately
    for pattern in patterns:
        escaped_pattern = re.escape(pattern)
        ending_regex = f"{escaped_pattern}$"

        ending_with_pattern = df.filter(
            pl.col(col).is_not_null() &
            pl.col(col).str.contains(ending_regex)
        )

        if len(ending_with_pattern) > 0:
            # Format examples with primary key context if available
            examples = []
            select_cols = [col] + (primary_key_columns if primary_key_columns else [])
            for row in ending_with_pattern.select(select_cols).head(3).iter_rows(named=True):
                examples.append(_format_example_with_pk(row, col, primary_key_columns))

            issues.append({
                'type': 'ENDING_CHARACTERS',
                'pattern': pattern,
                'count': len(ending_with_pattern),
                'pct': len(ending_with_pattern) / non_null_count * 100,
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


def check_regex_patterns(df: pl.DataFrame, col: str, primary_key_columns: Optional[List[str]] = None, pattern: str = None, mode: str = "contains", description: str = None) -> List[Dict]:
    """Check strings against regex patterns with flexible validation modes

    Args:
        df: DataFrame to check
        col: Column name to check
        primary_key_columns: Optional primary key columns for context
        pattern: Regex pattern to check against
        mode: Validation mode - "contains" (flag if pattern found) or "match" (flag if pattern not matched)
        description: Optional human-readable description for error messages

    Returns:
        List of issue dictionaries with examples
    """
    issues = []

    if pattern is None:
        return issues

    non_null_count = df[col].drop_nulls().len()
    if non_null_count == 0:
        return issues

    if mode == "contains":
        # Flag rows where pattern IS found (negative validation)
        violating = df.filter(
            pl.col(col).is_not_null() &
            pl.col(col).str.contains(pattern)
        )
        issue_description = description or f"Rows containing pattern: {pattern}"
    elif mode == "match":
        # Flag rows where pattern is NOT matched (positive validation)
        violating = df.filter(
            pl.col(col).is_not_null() &
            ~pl.col(col).str.contains(f"^{pattern}$")
        )
        issue_description = description or f"Rows not matching pattern: {pattern}"
    else:
        raise ValueError(f"Invalid mode: {mode}. Must be 'contains' or 'match'")

    if len(violating) > 0:
        # Format examples with primary key context if available
        examples = []
        select_cols = [col] + (primary_key_columns if primary_key_columns else [])
        for row in violating.select(select_cols).head(3).iter_rows(named=True):
            examples.append(_format_example_with_pk(row, col, primary_key_columns))

        issues.append({
            'type': 'REGEX_PATTERN',
            'pattern': pattern,
            'mode': mode,
            'description': issue_description,
            'count': len(violating),
            'pct': len(violating) / non_null_count * 100,
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
