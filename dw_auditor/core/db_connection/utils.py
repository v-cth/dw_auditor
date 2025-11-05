"""
Shared utility functions for database adapters
"""

import re
import ibis
from typing import Optional


def qualify_query_tables(
    query: str,
    table_name: str,
    dataset: str,
    project: Optional[str] = None
) -> str:
    """
    Rewrite SQL query to use fully-qualified table names

    Args:
        query: SQL query string
        table_name: Unqualified table name to replace
        dataset: Dataset/schema name
        project: Optional project name (for BigQuery cross-project)

    Returns:
        Modified query with qualified table names
    """
    if project:
        full_table_name = f"`{project}.{dataset}.{table_name}`"
    else:
        full_table_name = f"`{dataset}.{table_name}`"

    # Replace in FROM clauses
    pattern = r'\bFROM\s+' + re.escape(table_name) + r'\b'
    query = re.sub(pattern, f'FROM {full_table_name}', query, flags=re.IGNORECASE)

    # Replace in JOIN clauses
    pattern = r'\bJOIN\s+' + re.escape(table_name) + r'\b'
    query = re.sub(pattern, f'JOIN {full_table_name}', query, flags=re.IGNORECASE)

    return query


def apply_sampling(
    table: 'ibis.expr.types.Table',
    sample_size: int,
    method: str = 'random',
    key_column: Optional[str] = None
) -> 'ibis.expr.types.Table':
    """
    Apply sampling strategy to table

    Args:
        table: Ibis table expression
        sample_size: Number of rows to sample
        method: Sampling method ('random', 'recent', 'top', 'systematic')
        key_column: Column to use for ordering/filtering (required for non-random methods)

    Returns:
        Ibis table expression with sampling applied
    """
    if method == 'random':
        return table.order_by(ibis.random()).limit(sample_size)

    elif method == 'recent':
        if not key_column:
            raise ValueError("'recent' sampling method requires a key_column")
        return table.order_by(ibis.desc(key_column)).limit(sample_size)

    elif method == 'top':
        if not key_column:
            raise ValueError("'top' sampling method requires a key_column")
        return table.order_by(key_column).limit(sample_size)

    elif method == 'systematic':
        if not key_column:
            raise ValueError("'systematic' sampling method requires a key_column")

        try:
            row_count = table.count().execute()
            if row_count and row_count > sample_size:
                stride = max(1, row_count // sample_size)
                return table.filter(table[key_column] % stride == 0).limit(sample_size)
            else:
                return table.limit(sample_size)
        except Exception:
            stride = 10
            return table.filter(table[key_column] % stride == 0).limit(sample_size)

    else:
        raise ValueError(f"Unknown sampling method: {method}. Use 'random', 'recent', 'top', or 'systematic'")
