"""
Export column summary to CSV/DataFrame
"""

import polars as pl
from typing import Dict


def export_column_summary_to_dataframe(results: Dict) -> pl.DataFrame:
    """
    Export column summary to a Polars DataFrame

    Args:
        results: Audit results dictionary

    Returns:
        DataFrame with one row per column with basic metrics
    """
    if 'column_summary' not in results or not results['column_summary']:
        return pl.DataFrame({
            'table_name': [],
            'column_name': [],
            'data_type': [],
            'status': [],
            'null_count': [],
            'null_pct': [],
            'distinct_count': []
        })

    rows = []
    for col_name, col_data in results['column_summary'].items():
        row = {
            'table_name': results['table_name'],
            'column_name': col_name,
            'data_type': col_data['dtype'],
            'status': col_data.get('status', 'UNKNOWN'),
            'null_count': col_data['null_count'],
            'null_pct': col_data['null_pct'],
            'distinct_count': col_data['distinct_count']
        }
        rows.append(row)

    return pl.DataFrame(rows)
