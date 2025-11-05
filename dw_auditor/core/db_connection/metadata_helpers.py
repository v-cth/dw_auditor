"""
Shared helpers for metadata fetching and caching across database backends
"""

import logging
import polars as pl
from typing import Optional, List, Set, FrozenSet, Callable

logger = logging.getLogger(__name__)


def should_skip_query(
    schema: str,
    table_names: Optional[List[str]],
    cache_registry: dict,
    query_type: str
) -> bool:
    """
    Check if a metadata query can be skipped based on cache

    Args:
        schema: Schema/dataset name
        table_names: List of table names being requested (None = all tables)
        cache_registry: Dictionary tracking what's been fetched (schema -> frozenset)
        query_type: Name of query type for logging (e.g., "TABLES", "COLUMNS")

    Returns:
        True if query can be skipped, False if it needs to be executed
    """
    tables_sig = None if not table_names else frozenset(table_names)

    if schema not in cache_registry:
        return False

    prev_sig = cache_registry[schema]

    # Exact match
    if prev_sig == tables_sig:
        logger.debug(f"[metadata] {query_type} schema={schema} skipped (exact match)")
        return True

    # Requested is subset of previous fetch
    if tables_sig is not None and prev_sig is not None and tables_sig.issubset(prev_sig):
        logger.debug(f"[metadata] {query_type} schema={schema} skipped (subset cached)")
        return True

    return False


def execute_metadata_query(
    schema: str,
    table_names: Optional[List[str]],
    cache_registry: dict,
    query_type: str,
    query_builder: Callable[[], str],
    query_executor: Callable[[str], pl.DataFrame]
) -> Optional[pl.DataFrame]:
    """
    Execute a metadata query with caching and logging

    Args:
        schema: Schema/dataset name
        table_names: List of table names (None = all tables)
        cache_registry: Dictionary to track fetched signatures
        query_type: Query type for logging (e.g., "TABLES", "COLUMNS+PK")
        query_builder: Function that builds the SQL query
        query_executor: Function that executes the query and returns DataFrame

    Returns:
        DataFrame with results, or None if skipped (already cached)
    """
    tables_sig = None if not table_names else frozenset(table_names)

    logger.debug(f"[metadata] {query_type} schema={schema} filter={table_names if table_names else 'ALL'}")

    # Check if we can skip
    if should_skip_query(schema, table_names, cache_registry, query_type):
        return None

    # Build and log query
    query = query_builder()
    logger.debug(f"[query] Metadata {query_type} query:\n{query}")

    # Execute query
    result = query_executor(query)

    # Update cache registry
    cache_registry[schema] = tables_sig

    return result


def split_columns_pk_dataframe(
    combined_df: pl.DataFrame,
    is_pk_column: str = "is_pk",
    pk_ordinal_column: str = "pk_ordinal_position"
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Split a combined columns+PK DataFrame into separate columns and PK DataFrames

    Args:
        combined_df: Combined DataFrame with columns and PK indicator
        is_pk_column: Name of boolean column indicating primary key
        pk_ordinal_column: Name of ordinal position column for PK

    Returns:
        Tuple of (columns_df, pk_df)
    """
    # Build columns dataframe (exclude PK-specific columns)
    exclude_cols = [is_pk_column, pk_ordinal_column]
    column_cols = [col for col in combined_df.columns if col not in exclude_cols]
    columns_df = combined_df.select(column_cols)

    # Build primary keys dataframe
    if combined_df.height > 0:
        # Include schema_name if it exists
        select_cols = []
        if "schema_name" in combined_df.columns:
            select_cols.append(pl.col("schema_name"))
        select_cols.extend([
            pl.col("table_name"),
            pl.col("column_name"),
            pl.col(pk_ordinal_column).alias("ordinal_position"),
        ])

        pk_df = (
            combined_df
            .filter(pl.col(is_pk_column) == True)  # noqa: E712
            .select(select_cols)
            .sort(["table_name", "ordinal_position"])
        )
    else:
        pk_df = pl.DataFrame()

    return columns_df, pk_df


def normalize_snowflake_columns(df: pl.DataFrame, column_mapping: dict) -> pl.DataFrame:
    """
    Normalize Snowflake uppercase column names to lowercase

    Args:
        df: DataFrame with uppercase column names
        column_mapping: Dictionary mapping uppercase -> lowercase names

    Returns:
        DataFrame with normalized column names
    """
    # Only rename columns that exist in the DataFrame
    rename_map = {k: v for k, v in column_mapping.items() if k in df.columns}
    return df.rename(rename_map)
