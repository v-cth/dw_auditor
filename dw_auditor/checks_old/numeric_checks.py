"""
Numeric data quality checks
"""

import polars as pl
from typing import List, Dict, Optional


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


def check_numeric_range(
    df: pl.DataFrame,
    col: str,
    primary_key_columns: Optional[List[str]] = None,
    greater_than: Optional[float] = None,
    greater_than_or_equal: Optional[float] = None,
    less_than: Optional[float] = None,
    less_than_or_equal: Optional[float] = None
) -> List[Dict]:
    """
    Check if numeric values are within specified range or boundaries

    Args:
        df: DataFrame to check
        col: Column name
        primary_key_columns: Optional list of primary key column names for context
        greater_than: Exclusive lower bound (value > greater_than)
        greater_than_or_equal: Inclusive lower bound (value >= greater_than_or_equal)
        less_than: Exclusive upper bound (value < less_than)
        less_than_or_equal: Inclusive upper bound (value <= less_than_or_equal)

    Returns:
        List of issues found
    """
    issues = []

    non_null_df = df.filter(pl.col(col).is_not_null())
    non_null_count = len(non_null_df)

    if non_null_count == 0:
        return issues

    # Check greater_than (exclusive: >)
    if greater_than is not None:
        not_greater = non_null_df.filter(pl.col(col) <= greater_than)
        not_greater_count = len(not_greater)

        if not_greater_count > 0:
            pct = (not_greater_count / non_null_count) * 100

            # Format examples with primary key context if available
            examples = []
            select_cols = [col] + (primary_key_columns if primary_key_columns else [])
            for row in not_greater.select(select_cols).head(5).iter_rows(named=True):
                examples.append(_format_example_with_pk(row, col, primary_key_columns))

            issues.append({
                'type': 'VALUE_NOT_GREATER_THAN',
                'count': not_greater_count,
                'pct': pct,
                'threshold': greater_than,
                'operator': '>',
                'suggestion': f'Values should be > {greater_than}',
                'examples': examples
            })

    # Check greater_than_or_equal (inclusive: >=)
    if greater_than_or_equal is not None:
        not_gte = non_null_df.filter(pl.col(col) < greater_than_or_equal)
        not_gte_count = len(not_gte)

        if not_gte_count > 0:
            pct = (not_gte_count / non_null_count) * 100

            # Format examples with primary key context if available
            examples = []
            select_cols = [col] + (primary_key_columns if primary_key_columns else [])
            for row in not_gte.select(select_cols).head(5).iter_rows(named=True):
                examples.append(_format_example_with_pk(row, col, primary_key_columns))

            issues.append({
                'type': 'VALUE_NOT_GREATER_OR_EQUAL',
                'count': not_gte_count,
                'pct': pct,
                'threshold': greater_than_or_equal,
                'operator': '>=',
                'suggestion': f'Values should be >= {greater_than_or_equal}',
                'examples': examples
            })

    # Check less_than (exclusive: <)
    if less_than is not None:
        not_less = non_null_df.filter(pl.col(col) >= less_than)
        not_less_count = len(not_less)

        if not_less_count > 0:
            pct = (not_less_count / non_null_count) * 100

            # Format examples with primary key context if available
            examples = []
            select_cols = [col] + (primary_key_columns if primary_key_columns else [])
            for row in not_less.select(select_cols).head(5).iter_rows(named=True):
                examples.append(_format_example_with_pk(row, col, primary_key_columns))

            issues.append({
                'type': 'VALUE_NOT_LESS_THAN',
                'count': not_less_count,
                'pct': pct,
                'threshold': less_than,
                'operator': '<',
                'suggestion': f'Values should be < {less_than}',
                'examples': examples
            })

    # Check less_than_or_equal (inclusive: <=)
    if less_than_or_equal is not None:
        not_lte = non_null_df.filter(pl.col(col) > less_than_or_equal)
        not_lte_count = len(not_lte)

        if not_lte_count > 0:
            pct = (not_lte_count / non_null_count) * 100

            # Format examples with primary key context if available
            examples = []
            select_cols = [col] + (primary_key_columns if primary_key_columns else [])
            for row in not_lte.select(select_cols).head(5).iter_rows(named=True):
                examples.append(_format_example_with_pk(row, col, primary_key_columns))

            issues.append({
                'type': 'VALUE_NOT_LESS_OR_EQUAL',
                'count': not_lte_count,
                'pct': pct,
                'threshold': less_than_or_equal,
                'operator': '<=',
                'suggestion': f'Values should be <= {less_than_or_equal}',
                'examples': examples
            })

    return issues
