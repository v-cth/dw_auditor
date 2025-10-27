"""
Uniqueness checks for all column types
"""

import polars as pl
from typing import List, Dict, Optional


def check_uniqueness(
    df: pl.DataFrame,
    col: str,
    primary_key_columns: Optional[List[str]] = None
) -> List[Dict]:
    """
    Check if all values in a column are unique (excluding nulls)

    Args:
        df: DataFrame to check
        col: Column name
        primary_key_columns: Optional list of primary key column names for context

    Returns:
        List of issues found (empty if all values are unique)
    """
    issues = []

    # Filter out nulls
    non_null_df = df.filter(pl.col(col).is_not_null())
    non_null_count = len(non_null_df)

    if non_null_count == 0:
        return issues

    # Count occurrences of each value
    value_counts = non_null_df.group_by(col).agg(
        pl.count().alias('count')
    ).filter(
        pl.col('count') > 1  # Only duplicates
    ).sort('count', descending=True)

    duplicate_values_count = len(value_counts)

    if duplicate_values_count > 0:
        # Calculate total number of duplicate rows (sum of counts - number of unique duplicate values)
        total_duplicate_rows = value_counts['count'].sum() - duplicate_values_count
        pct = (total_duplicate_rows / non_null_count) * 100

        # Get top 5 most frequent duplicates with their counts
        examples = []
        for row in value_counts.head(5).iter_rows(named=True):
            value = row[col]
            count = row['count']

            # Try to get primary key context for first occurrence
            if primary_key_columns and len(primary_key_columns) > 0:
                # Get one example row with this value
                select_cols = [col] + primary_key_columns
                example_row = non_null_df.filter(pl.col(col) == value).select(select_cols).head(1)

                if len(example_row) > 0:
                    row_data = example_row.row(0, named=True)
                    pk_values = [f"{pk_col}={row_data[pk_col]}" for pk_col in primary_key_columns if pk_col in row_data]
                    if pk_values:
                        examples.append(f"{value} [count={count}, {', '.join(pk_values)}]")
                    else:
                        examples.append(f"{value} [count={count}]")
                else:
                    examples.append(f"{value} [count={count}]")
            else:
                examples.append(f"{value} [count={count}]")

        issues.append({
            'type': 'DUPLICATE_VALUES',
            'count': total_duplicate_rows,
            'pct': pct,
            'distinct_duplicates': duplicate_values_count,
            'suggestion': f'Column should contain only unique values. Found {duplicate_values_count} distinct value(s) with duplicates.',
            'examples': examples
        })

    return issues
