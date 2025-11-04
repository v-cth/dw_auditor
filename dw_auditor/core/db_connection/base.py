"""
Base adapter class defining interface for database backends
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any
import ibis
import polars as pl
import logging


class BaseAdapter(ABC):
    """Abstract base class for database adapters"""

    def __init__(self, **connection_params):
        self.connection_params = connection_params
        self.conn: Optional[ibis.BaseBackend] = None

        # Schema-wide metadata cache (Polars DataFrames)
        self._tables_df: Optional[pl.DataFrame] = None
        self._columns_df: Optional[pl.DataFrame] = None
        self._pk_df: Optional[pl.DataFrame] = None
        self._rowcount_df: Optional[pl.DataFrame] = None
        self._cached_schema: Optional[str] = None
        # Track which tables have been prefetched for the cached schema
        # None => all tables fetched; set(str) => subset fetched
        self._fetched_tables: Optional[set[str]] = None

    @abstractmethod
    def connect(self) -> ibis.BaseBackend:
        """Establish database connection"""
        pass

    @abstractmethod
    def _fetch_all_metadata(self, schema: str, table_names: Optional[List[str]] = None):
        """
        Fetch metadata for tables in schema (3-4 queries total)

        Args:
            schema: Schema/dataset name
            table_names: Optional list of specific table names to fetch (if None, fetch all)

        Stores results in _tables_df, _columns_df, _pk_df, _rowcount_df
        """
        pass

    def _ensure_metadata(self, schema: str, table_names: Optional[List[str]] = None):
        """Fetch metadata if not cached or schema changed; avoid unnecessary refetches."""
        logger = logging.getLogger(__name__)
        # Initial or schema change: fetch exactly what's requested (or all if None)
        if self._tables_df is None or self._cached_schema != schema:
            logger.debug(f"[metadata] fetch INIT schema={schema} tables={'ALL' if table_names is None else ','.join(table_names)}")
            self._fetch_all_metadata(schema, table_names)
            self._fetched_tables = None if table_names is None else set(table_names)
            return

        # Same schema and we have some cache
        if table_names is None:
            # Caller wants full coverage. If we don't already have all, upgrade to all.
            if self._fetched_tables is not None:
                logger.debug(f"[metadata] fetch UPGRADE schema={schema} tables=ALL (from subset of {len(self._fetched_tables)})")
                self._fetch_all_metadata(schema, None)
                self._fetched_tables = None
            return

        # Caller wants a subset of tables
        requested = set(table_names)
        if self._fetched_tables is None:
            # Already have full coverage
            return

        if requested.issubset(self._fetched_tables):
            # Already covered
            return

        # Need to extend cache to cover union of requested and existing subset
        union_tables = self._fetched_tables | requested
        logger.debug(f"[metadata] fetch EXTEND schema={schema} tables={','.join(sorted(list(union_tables)))}")
        self._fetch_all_metadata(schema, sorted(list(union_tables)))
        self._fetched_tables = set(union_tables)

    def prefetch_metadata(self, schema: str, table_names: List[str]):
        """
        Pre-fetch metadata for specific tables (recommended for multi-table audits)

        Args:
            schema: Schema/dataset name
            table_names: List of table names to fetch metadata for
        """
        # Deduplicate prefetches within the same schema
        requested = set(table_names) if table_names else set()

        # If switching schema, just fetch and reset tracking
        if self._cached_schema != schema:
            self._fetch_all_metadata(schema, table_names)
            self._fetched_tables = None if not table_names else set(table_names)
            return

        # Same schema
        if self._fetched_tables is None:
            # Already fetched all tables; no need to fetch subset again
            return

        if not table_names:
            # Requesting all tables now; upgrade cache to all
            self._fetch_all_metadata(schema, None)
            self._fetched_tables = None
            return

        # If requested subset is already covered, skip
        if self._fetched_tables is not None and requested.issubset(self._fetched_tables):
            return

        # Need to extend cached subset to cover the union
        union_tables = requested if self._fetched_tables is None else (self._fetched_tables | requested)
        self._fetch_all_metadata(schema, sorted(list(union_tables)))
        self._fetched_tables = set(union_tables)

    @abstractmethod
    def get_table(self, table_name: str, schema: Optional[str] = None) -> ibis.expr.types.Table:
        """Get Ibis table reference"""
        pass

    @abstractmethod
    def execute_query(
        self,
        table_name: str,
        schema: Optional[str] = None,
        limit: Optional[int] = None,
        custom_query: Optional[str] = None,
        sample_size: Optional[int] = None,
        sampling_method: str = 'random',
        sampling_key_column: Optional[str] = None,
        columns: Optional[List[str]] = None
    ) -> pl.DataFrame:
        """Execute query and return Polars DataFrame"""
        pass

    def get_table_metadata(self, table_name: str, schema: Optional[str] = None) -> Dict[str, Any]:
        """Get table metadata by filtering cached DataFrames"""
        effective_schema = schema or self.connection_params.get('schema')
        if not effective_schema:
            return {}

        self._ensure_metadata(effective_schema, [table_name])

        # Filter tables_df by both schema and table_name
        table_info = self._tables_df.filter(
            (pl.col('schema_name') == effective_schema) & (pl.col('table_name') == table_name)
        )
        if len(table_info) == 0:
            return {}

        # Filter columns_df for partition/clustering info
        columns_info = self._columns_df.filter(
            (pl.col('schema_name') == effective_schema) & (pl.col('table_name') == table_name)
        )

        metadata = {
            'table_name': str(table_info['table_name'][0]),
            'table_type': str(table_info['table_type'][0]) if table_info['table_type'][0] is not None else None,
            'created_time': str(table_info['creation_time'][0]) if 'creation_time' in table_info.columns and table_info['creation_time'][0] is not None else None,
        }

        # Add table UID (fully qualified name with project/dataset or database/schema)
        metadata['table_uid'] = self._build_table_uid(table_name, effective_schema)

        # Row count, size, and timestamps from __TABLES__ or equivalent
        # Try _rowcount_df first (BigQuery), then fall back to _tables_df (Snowflake)
        source_df = None
        source_info = None

        if self._rowcount_df is not None and len(self._rowcount_df) > 0:
            # Filter by schema and table (first column is schema_name in new approach)
            source_info = self._rowcount_df.filter(
                (pl.col('schema_name') == effective_schema) & (pl.col('table_id') == table_name)
            )
            if len(source_info) > 0:
                source_df = self._rowcount_df

        # If not found in rowcount_df, try tables_df (Snowflake has these fields in tables)
        if source_df is None and len(table_info) > 0:
            source_info = table_info
            source_df = self._tables_df

        if source_df is not None and source_info is not None and len(source_info) > 0:
            # Row count
            if 'row_count' in source_df.columns:
                metadata['row_count'] = int(source_info['row_count'][0]) if source_info['row_count'][0] is not None else None

            # Size in bytes
            if 'size_bytes' in source_df.columns:
                metadata['size_bytes'] = int(source_info['size_bytes'][0]) if source_info['size_bytes'][0] is not None else None

            # Created at timestamp
            if 'created_at' in source_df.columns:
                metadata['created_at'] = str(source_info['created_at'][0]) if source_info['created_at'][0] is not None else None

            # Modified at timestamp
            if 'modified_at' in source_df.columns:
                metadata['modified_at'] = str(source_info['modified_at'][0]) if source_info['modified_at'][0] is not None else None

        # Partition column
        if 'is_partitioning_column' in columns_info.columns:
            partition_cols = columns_info.filter(pl.col('is_partitioning_column') == 'YES')
            if len(partition_cols) > 0:
                metadata['partition_column'] = str(partition_cols['column_name'][0])
                metadata['partition_type'] = 'TIME'

        # Clustering columns
        if 'clustering_ordinal_position' in columns_info.columns:
            cluster_cols = columns_info.filter(
                pl.col('clustering_ordinal_position').is_not_null()
            ).sort('clustering_ordinal_position')
            if len(cluster_cols) > 0:
                metadata['clustering_columns'] = cluster_cols['column_name'].to_list()

        return metadata

    def get_table_schema(self, table_name: str, schema: Optional[str] = None) -> Dict[str, str]:
        """Get column names and data types by filtering cached columns_df"""
        effective_schema = schema or self.connection_params.get('schema')
        if not effective_schema:
            return {}

        self._ensure_metadata(effective_schema, [table_name])

        table_cols = self._columns_df.filter(
            (pl.col('schema_name') == effective_schema) & (pl.col('table_name') == table_name)
        )

        return {
            str(row['column_name']): str(row['data_type'])
            for row in table_cols.iter_rows(named=True)
        }

    def get_primary_key_columns(self, table_name: str, schema: Optional[str] = None) -> List[str]:
        """Get primary key columns by filtering cached pk_df"""
        effective_schema = schema or self.connection_params.get('schema')
        if not effective_schema:
            return []

        self._ensure_metadata(effective_schema, [table_name])

        if self._pk_df is None or len(self._pk_df) == 0:
            return []

        pk_cols = self._pk_df.filter(
            (pl.col('schema_name') == effective_schema) & (pl.col('table_name') == table_name)
        )

        return [str(col) for col in pk_cols['column_name'].to_list()]

    def get_row_count(self, table_name: str, schema: Optional[str] = None, approximate: bool = True) -> Optional[int]:
        """Get row count from cached metadata or exact count"""
        effective_schema = schema or self.connection_params.get('schema')
        if not effective_schema:
            return None

        if approximate:
            self._ensure_metadata(effective_schema, [table_name])

            if self._rowcount_df is not None and len(self._rowcount_df) > 0:
                rowcount_info = self._rowcount_df.filter(
                    (pl.col('schema_name') == effective_schema) & (pl.col('table_id') == table_name)
                )
                if len(rowcount_info) > 0 and 'row_count' in rowcount_info.columns:
                    return int(rowcount_info['row_count'][0])

        # Fallback to exact count
        try:
            table = self.get_table(table_name, schema)
            count_result = table.count().to_polars()
            return int(count_result[0, 0])
        except Exception as e:
            print(f"⚠️  Could not get row count: {e}")
            return None

    def get_all_tables(self, schema: Optional[str] = None) -> List[str]:
        """Get list of all tables by filtering cached tables_df"""
        effective_schema = schema or self.connection_params.get('schema')
        if not effective_schema:
            return []

        # Listing all tables requires full coverage
        self._ensure_metadata(effective_schema, None)

        if self._tables_df is None or len(self._tables_df) == 0:
            return []

        return self._tables_df['table_name'].to_list()

    @abstractmethod
    def estimate_bytes_scanned(
        self,
        table_name: str,
        schema: Optional[str] = None,
        custom_query: Optional[str] = None,
        sample_size: Optional[int] = None,
        sampling_method: str = 'random',
        sampling_key_column: Optional[str] = None,
        columns: Optional[List[str]] = None
    ) -> Optional[int]:
        """Estimate bytes to be scanned (BigQuery only)"""
        pass

    @abstractmethod
    def list_tables(self, schema: Optional[str] = None) -> List[str]:
        """List tables using Ibis native method"""
        pass

    @abstractmethod
    def _build_table_uid(self, table_name: str, schema: str) -> str:
        """Build unique table identifier (backend-specific format)"""
        pass

    def close(self):
        """Close database connection and clear cache"""
        if self.conn is not None:
            self.conn = None
            self._tables_df = None
            self._columns_df = None
            self._pk_df = None
            self._rowcount_df = None
            self._cached_schema = None
            self._fetched_tables = None

    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()
