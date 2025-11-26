"""
Shared utility functions for database adapters
"""

import sqlglot
import ibis
from typing import Optional


def qualify_query_tables(
    query: str,
    table_name: str,
    dataset: str,
    project: Optional[str] = None,
    dialect: str = 'bigquery'
) -> str:
    """
    Rewrite SQL query to use fully-qualified table names using proper SQL parsing
    
    Args:
        query: SQL query string
        table_name: Unqualified table name to replace
        dataset: Dataset/schema name
        project: Optional project/catalog name (for cross-project/catalog queries)
        dialect: SQL dialect ('bigquery' or 'databricks')
    
    Returns:
        Modified query with qualified table names
    """
    try:
        # Parse SQL using sqlglot
        parsed = sqlglot.parse_one(query, dialect=dialect)
        
        # Collect CTE names to avoid qualifying them
        cte_names = set()
        for cte in parsed.find_all(sqlglot.exp.CTE):
            if cte.alias:
                cte_names.add(cte.alias)
        
        # Walk the AST and qualify only actual table references (not CTEs)
        for table in parsed.find_all(sqlglot.exp.Table):
            # Only qualify if:
            # 1. It matches our table name
            # 2. It's not already qualified (no catalog)
            # 3. It's not a CTE reference
            if (table.name == table_name and 
                not table.catalog and 
                table.name not in cte_names):
                if project:
                    # Use quoted=True for BigQuery to ensure backticks around identifiers
                    table.set('catalog', sqlglot.exp.Identifier(this=project, quoted=True))
                table.set('db', sqlglot.exp.Identifier(this=dataset, quoted=True))
        
        return parsed.sql(dialect=dialect)
    except Exception as e:
        # Fallback to original query if parsing fails
        import logging
        logger = logging.getLogger(__name__)
        logger.warning(f"Failed to parse SQL with sqlglot: {e}. Using original query.")
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
            count_result = table.count().to_polars()
            row_count = int(count_result[0, 0]) if count_result is not None else None
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
