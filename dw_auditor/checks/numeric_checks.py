"""
Numeric data quality checks
"""

import polars as pl
from typing import List, Dict, Optional


def check_numeric_range(
    df: pl.DataFrame,
    col: str,
    min_val: Optional[float] = None,
    max_val: Optional[float] = None,
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
        min_val: Minimum value (inclusive: value >= min_val)
        max_val: Maximum value (inclusive: value <= max_val)
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

    # Check minimum value (inclusive: >=)
    if min_val is not None:
        below_min = non_null_df.filter(pl.col(col) < min_val)
        below_min_count = len(below_min)

        if below_min_count > 0:
            pct = (below_min_count / non_null_count) * 100
            examples = below_min[col].head(5).to_list()

            issues.append({
                'type': 'VALUE_BELOW_MIN',
                'count': below_min_count,
                'pct': pct,
                'threshold': min_val,
                'operator': '>=',
                'suggestion': f'Values should be >= {min_val}',
                'examples': examples
            })

    # Check maximum value (inclusive: <=)
    if max_val is not None:
        above_max = non_null_df.filter(pl.col(col) > max_val)
        above_max_count = len(above_max)

        if above_max_count > 0:
            pct = (above_max_count / non_null_count) * 100
            examples = above_max[col].head(5).to_list()

            issues.append({
                'type': 'VALUE_ABOVE_MAX',
                'count': above_max_count,
                'pct': pct,
                'threshold': max_val,
                'operator': '<=',
                'suggestion': f'Values should be <= {max_val}',
                'examples': examples
            })

    # Check greater_than (exclusive: >)
    if greater_than is not None:
        not_greater = non_null_df.filter(pl.col(col) <= greater_than)
        not_greater_count = len(not_greater)

        if not_greater_count > 0:
            pct = (not_greater_count / non_null_count) * 100
            examples = not_greater[col].head(5).to_list()

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
            examples = not_gte[col].head(5).to_list()

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
            examples = not_less[col].head(5).to_list()

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
            examples = not_lte[col].head(5).to_list()

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
