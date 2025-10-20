"""
Export audit results to Polars DataFrame
"""

import polars as pl
from typing import Dict


def export_to_dataframe(results: Dict) -> pl.DataFrame:
    """
    Export audit results to a Polars DataFrame for easy analysis

    Args:
        results: Audit results dictionary

    Returns:
        DataFrame with one row per issue found
    """
    rows = []

    for col_name, col_data in results['columns'].items():
        for issue in col_data['issues']:
            row = {
                'table_name': results['table_name'],
                'total_rows': results['total_rows'],
                'analyzed_rows': results['analyzed_rows'],
                'sampled': results['sampled'],
                'column_name': col_name,
                'column_dtype': col_data['dtype'],
                'null_count': col_data['null_count'],
                'null_pct': col_data['null_pct'],
                'distinct_count': col_data.get('distinct_count', None),
                'issue_type': issue['type'],
                'issue_count': issue.get('count', 0),
                'issue_pct': issue.get('pct', 0),
                'suggestion': issue.get('suggestion', ''),
                'examples': str(issue.get('examples', [])[:3]),
                'audit_timestamp': results.get('timestamp', '')
            }
            rows.append(row)

    if not rows:
        # Return empty dataframe with schema if no issues
        return pl.DataFrame({
            'table_name': [],
            'total_rows': [],
            'analyzed_rows': [],
            'sampled': [],
            'column_name': [],
            'column_dtype': [],
            'null_count': [],
            'null_pct': [],
            'distinct_count': [],
            'issue_type': [],
            'issue_count': [],
            'issue_pct': [],
            'suggestion': [],
            'examples': [],
            'audit_timestamp': []
        })

    return pl.DataFrame(rows)
