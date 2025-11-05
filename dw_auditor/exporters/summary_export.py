"""
Export column summary to CSV/DataFrame
"""

import polars as pl
from typing import Dict, List


def export_combined_column_summary_to_dataframe(all_results: List[Dict]) -> pl.DataFrame:
    """
    Export combined column summary for all tables to a single Polars DataFrame

    Args:
        all_results: List of audit results dictionaries (one per table)

    Returns:
        DataFrame with one row per column across all tables
    """
    all_rows = []

    for results in all_results:
        if 'column_summary' not in results or not results['column_summary']:
            continue

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
            # Add source_dtype if this column was type-converted
            if 'source_dtype' in col_data:
                row['source_dtype'] = col_data['source_dtype']
            else:
                row['source_dtype'] = None
            all_rows.append(row)

    if not all_rows:
        return pl.DataFrame({
            'table_name': [],
            'column_name': [],
            'data_type': [],
            'source_dtype': [],
            'status': [],
            'null_count': [],
            'null_pct': [],
            'distinct_count': []
        })

    return pl.DataFrame(all_rows)


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
            'source_dtype': [],
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
        # Add source_dtype if this column was type-converted
        if 'source_dtype' in col_data:
            row['source_dtype'] = col_data['source_dtype']
        else:
            row['source_dtype'] = None
        rows.append(row)

    return pl.DataFrame(rows)
