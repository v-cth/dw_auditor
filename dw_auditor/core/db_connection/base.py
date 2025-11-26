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

        # Multi-project/schema metadata cache
        # Key: (project_id, schema) tuple where project_id can be None for single-project backends
        # Value: dict with 'tables_df', 'columns_df', 'pk_df', 'rowcount_df', 'fetched_tables'
        self._metadata_cache: Dict[tuple, Dict[str, Any]] = {}

    def _normalize_table_name(self, table_name: str) -> str:
        """
        Normalize table name for database lookups.

        Override in subclasses if needed (e.g., Snowflake uses uppercase).
        Default implementation returns table name as-is.
        """
        return table_name

    @abstractmethod
    def connect(self) -> ibis.BaseBackend:
        """Establish database connection"""
        pass

    @abstractmethod
    def _fetch_all_metadata(self, schema: str, table_names: Optional[List[str]] = None, database_id: Optional[str] = None):
        """
        Fetch metadata for tables in schema (3-4 queries total)

        Args:
            schema: Schema/dataset name
            table_names: Optional list of specific table names to fetch (if None, fetch all)
            database_id: Optional database/project/catalog ID for cross-database queries

        Stores results in _metadata_cache[(database_id, schema)]
        """
        pass

    def _ensure_metadata(self, schema: str, table_names: Optional[List[str]] = None, database_id: Optional[str] = None):
        """Fetch metadata if not cached or (database_id, schema) changed; avoid unnecessary refetches."""
        logger = logging.getLogger(__name__)
        cache_key = (database_id, schema)

        # Normalize table names for this database backend
        normalized_table_names = [self._normalize_table_name(t) for t in table_names] if table_names else None

        # Get or create cache entry for this (database_id, schema) combination
        if cache_key not in self._metadata_cache:
            logger.debug(f"[metadata] fetch INIT database={database_id} schema={schema} tables={'ALL' if normalized_table_names is None else ','.join(normalized_table_names)}")
            self._fetch_all_metadata(schema, normalized_table_names, database_id)
            return

        cache_entry = self._metadata_cache[cache_key]
        fetched_tables = cache_entry.get('fetched_tables')

        # Cache exists for this (project_id, schema)
        if normalized_table_names is None:
            # Caller wants full coverage. If we don't already have all, upgrade to all.
            if fetched_tables is not None:
                logger.debug(f"[metadata] fetch UPGRADE database={database_id} schema={schema} tables=ALL (from subset of {len(fetched_tables)})")
                self._fetch_all_metadata(schema, None, database_id)
            return

        # Caller wants a subset of tables
        requested = set(normalized_table_names)
        if fetched_tables is None:
            # Already have full coverage
            return

        if requested.issubset(fetched_tables):
            # Already covered
            return

        # Need to extend cache to cover union of requested and existing subset
        union_tables = fetched_tables | requested
        logger.debug(f"[metadata] fetch EXTEND database={database_id} schema={schema} tables={','.join(sorted(list(union_tables)))}")
        self._fetch_all_metadata(schema, sorted(list(union_tables)), database_id)

    def prefetch_metadata(self, schema: str, table_names: List[str], database_id: Optional[str] = None):
        """
        Pre-fetch metadata for specific tables (recommended for multi-table audits)

        Args:
            schema: Schema/dataset name
            table_names: List of table names to fetch metadata for
            database_id: Optional database/project/catalog ID for cross-database queries
        """
        # Use _ensure_metadata which handles all the caching logic
        self._ensure_metadata(schema, table_names, database_id)

    @abstractmethod
    def get_table(self, table_name: str, schema: Optional[str] = None, database_id: Optional[str] = None) -> ibis.expr.types.Table:
        """Get Ibis table reference"""
        pass

    def execute_query(
        self,
        table_name: str,
        schema: Optional[str] = None,
        limit: Optional[int] = None,
        custom_query: Optional[str] = None,
        sample_size: Optional[int] = None,
        sampling_method: str = 'random',
        sampling_key_column: Optional[str] = None,
        columns: Optional[List[str]] = None,
        database_id: Optional[str] = None
    ) -> pl.DataFrame:
        """
        Execute query and return Polars DataFrame
        
        Common implementation - override _qualify_custom_query for dialect-specific behavior
        """
        logger = logging.getLogger(__name__)
        if self.conn is None:
            self.connect()

        if custom_query:
            # Qualify table names in custom query using dialect-specific logic
            custom_query = self._qualify_custom_query(
                custom_query, table_name, schema, database_id
            )
            
            backend_name = self.__class__.__name__.replace('Adapter', '')
            logger.debug(f"[query] {backend_name} custom query:\n{custom_query}")
            result = self.conn.sql(custom_query)
        else:
            # Build table reference
            table = self.get_table(table_name, schema, database_id)

            # Apply column selection
            if columns:
                table = table.select(columns)

            # Apply sampling or limit
            if sample_size:
                from .utils import apply_sampling
                table = apply_sampling(table, sample_size, sampling_method, sampling_key_column)
            elif limit:
                table = table.limit(limit)

            result = table

            # Log the compiled SQL query
            try:
                compiled_query = ibis.to_sql(result)
                backend_name = self.__class__.__name__.replace('Adapter', '')
                logger.debug(f"[query] {backend_name} generated query:\n{compiled_query}")
            except Exception as e:
                logger.debug(f"[query] Could not compile query to SQL: {e}")

        return result.to_polars()

    @abstractmethod
    def _qualify_custom_query(
        self,
        custom_query: str,
        table_name: str,
        schema: Optional[str],
        database_id: Optional[str]
    ) -> str:
        """
        Qualify table names in custom query (dialect-specific)
        
        Args:
            custom_query: The custom SQL query to qualify
            table_name: Name of the table being queried
            schema: Schema/dataset name
            database_id: Optional database/project/catalog ID
            
        Returns:
            Qualified SQL query
        """
        pass

    @abstractmethod
    def get_table(self, table_name: str, schema: Optional[str] = None, database_id: Optional[str] = None) -> ibis.Table:
        """Get Ibis table reference"""
        pass

    def get_table_metadata(self, table_name: str, schema: Optional[str] = None, database_id: Optional[str] = None) -> Dict[str, Any]:
        """Get table metadata by filtering cached DataFrames

        Args:
            table_name: Name of the table
            schema: Schema/dataset name
            database_id: Optional database/project/catalog ID for cross-database queries
        """
        effective_schema = schema or self.connection_params.get('default_schema')
        if not effective_schema:
            return {}

        # Normalize table name for database-specific lookups
        normalized_table_name = self._normalize_table_name(table_name)

        # Ensure metadata is cached for this (database_id, schema, table) combination
        self._ensure_metadata(effective_schema, [table_name], database_id)

        # Get cache entry for this (database_id, schema)
        cache_key = (database_id, effective_schema)
        if cache_key not in self._metadata_cache:
            return {}

        cache_entry = self._metadata_cache[cache_key]
        tables_df = cache_entry.get('tables_df')
        columns_df = cache_entry.get('columns_df')
        rowcount_df = cache_entry.get('rowcount_df')

        if tables_df is None:
            return {}

        # Filter tables_df by both schema and normalized table_name
        table_info = tables_df.filter(
            (pl.col('schema_name') == effective_schema) & (pl.col('table_name') == normalized_table_name)
        )
        if len(table_info) == 0:
            return {}

        # Filter columns_df for partition/clustering info
        columns_info = columns_df.filter(
            (pl.col('schema_name') == effective_schema) & (pl.col('table_name') == normalized_table_name)
        ) if columns_df is not None else pl.DataFrame()

        metadata = {
            'table_name': str(table_info['table_name'][0]),
            'table_type': str(table_info['table_type'][0]) if table_info['table_type'][0] is not None else None,
            'description': str(table_info['description'][0]) if 'description' in table_info.columns and table_info['description'][0] is not None else None,
            'created_time': str(table_info['creation_time'][0]) if 'creation_time' in table_info.columns and table_info['creation_time'][0] is not None else None,
        }

        # Add table UID (fully qualified name with project/dataset or database/schema)
        metadata['table_uid'] = self._build_table_uid(table_name, effective_schema)

        # Row count, size, and timestamps from __TABLES__ or equivalent
        # Try rowcount_df first (BigQuery), then fall back to tables_df (Snowflake)
        source_df = None
        source_info = None

        if rowcount_df is not None and len(rowcount_df) > 0:
            # Filter by schema and normalized table (first column is schema_name in new approach)
            source_info = rowcount_df.filter(
                (pl.col('schema_name') == effective_schema) & (pl.col('table_id') == normalized_table_name)
            )
            if len(source_info) > 0:
                source_df = rowcount_df

        # If not found in rowcount_df, try tables_df (Snowflake has these fields in tables)
        if source_df is None and len(table_info) > 0:
            source_info = table_info
            source_df = tables_df

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

    def get_table_schema(self, table_name: str, schema: Optional[str] = None, database_id: Optional[str] = None) -> Dict[str, Dict[str, Any]]:
        """
        Get column metadata (data types and descriptions) by filtering cached columns_df

        Args:
            table_name: Name of the table
            schema: Schema/dataset name
            database_id: Optional database/project/catalog ID for cross-database queries
        """
        import logging
        logger = logging.getLogger(__name__)

        effective_schema = schema or self.connection_params.get('default_schema')
        if not effective_schema:
            logger.debug(f"No effective schema for table {table_name}")
            return {}

        # Normalize table name for database-specific lookups
        normalized_table_name = self._normalize_table_name(table_name)

        # Ensure metadata is cached for this (database_id, schema, table) combination
        self._ensure_metadata(effective_schema, [table_name], database_id)

        # Get cache entry for this (database_id, schema)
        cache_key = (database_id, effective_schema)
        if cache_key not in self._metadata_cache:
            return {}

        cache_entry = self._metadata_cache[cache_key]
        columns_df = cache_entry.get('columns_df')

        if columns_df is None or columns_df.is_empty():
            return {}

        table_cols = columns_df.filter(
            (pl.col('schema_name') == effective_schema) & (pl.col('table_name') == normalized_table_name)
        )

        # Check if description column exists
        has_descriptions = 'description' in columns_df.columns
        if not has_descriptions:
            logger.debug(f"'description' column not found in columns_df for {table_name}")

        logger.debug(f"Found {len(table_cols)} columns for {effective_schema}.{table_name}")

        result = {}
        for row in table_cols.iter_rows(named=True):
            col_name = str(row['column_name'])
            result[col_name] = {
                'data_type': str(row['data_type']),
                'description': str(row['description']) if has_descriptions and row['description'] is not None else None
            }

        return result

    def get_primary_key_columns(self, table_name: str, schema: Optional[str] = None, database_id: Optional[str] = None) -> List[str]:
        """Get primary key columns by filtering cached pk_df"""
        effective_schema = schema or self.connection_params.get('default_schema')
        if not effective_schema:
            return []

        # Ensure metadata is cached for this (database_id, schema, table) combination
        self._ensure_metadata(effective_schema, [table_name], database_id)

        # Get cache entry for this (database_id, schema)
        cache_key = (database_id, effective_schema)
        if cache_key not in self._metadata_cache:
            return []

        cache_entry = self._metadata_cache[cache_key]
        pk_df = cache_entry.get('pk_df')

        if pk_df is None or len(pk_df) == 0:
            return []

        pk_cols = pk_df.filter(
            (pl.col('schema_name') == effective_schema) & (pl.col('table_name') == table_name)
        )

        return [str(col) for col in pk_cols['column_name'].to_list()]

    def get_row_count(self, table_name: str, schema: Optional[str] = None, database_id: Optional[str] = None, approximate: bool = True) -> Optional[int]:
        """Get row count from cached metadata or exact count"""
        effective_schema = schema or self.connection_params.get('default_schema')
        if not effective_schema:
            return None

        if approximate:
            # Ensure metadata is cached for this (database_id, schema, table) combination
            self._ensure_metadata(effective_schema, [table_name], database_id)

            # Check if this is a VIEW - if so, skip approximate count and go to exact count
            cache_key = (database_id, effective_schema)
            if cache_key in self._metadata_cache:
                cache_entry = self._metadata_cache[cache_key]
                tables_df = cache_entry.get('tables_df')

                if tables_df is not None and len(tables_df) > 0:
                    table_info = tables_df.filter(pl.col('table_name') == table_name)
                    if len(table_info) > 0 and 'table_type' in table_info.columns:
                        table_type = table_info['table_type'][0]
                        if table_type == 'VIEW':
                            # Skip approximate count for VIEWs, go directly to exact count
                            approximate = False

            if approximate and cache_key in self._metadata_cache:
                cache_entry = self._metadata_cache[cache_key]
                rowcount_df = cache_entry.get('rowcount_df')

                if rowcount_df is not None and len(rowcount_df) > 0:
                    rowcount_info = rowcount_df.filter(
                        (pl.col('schema_name') == effective_schema) & (pl.col('table_id') == table_name)
                    )
                    if len(rowcount_info) > 0 and 'row_count' in rowcount_info.columns:
                        row_count = rowcount_info['row_count'][0]
                        # If row count is 0 or None, fall through to exact count
                        if row_count and row_count > 0:
                            return int(row_count)

        # Fallback to exact count
        try:
            table = self.get_table(table_name, schema, database_id)
            count_result = table.count().to_polars()

            # Handle both DataFrame and scalar returns
            if isinstance(count_result, (int, float)):
                return int(count_result)
            else:
                return int(count_result[0, 0])
        except Exception as e:
            logging.getLogger(__name__).error(f"Could not get row count: {e}")
            return None

    def get_all_tables(self, schema: Optional[str] = None, database_id: Optional[str] = None) -> List[str]:
        """Get list of all tables by filtering cached tables_df"""
        effective_schema = schema or self.connection_params.get('default_schema')
        if not effective_schema:
            return []

        # Listing all tables requires full coverage
        self._ensure_metadata(effective_schema, None, database_id)

        # Get cache entry for this (database_id, schema)
        cache_key = (database_id, effective_schema)
        if cache_key not in self._metadata_cache:
            return []

        cache_entry = self._metadata_cache[cache_key]
        tables_df = cache_entry.get('tables_df')

        if tables_df is None or len(tables_df) == 0:
            return []

        return tables_df['table_name'].to_list()

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
        """
        Estimate bytes to be scanned (BigQuery only)
        
        Default implementation for non-BigQuery backends.
        BigQuery adapter should override this method.
        """
        logger = logging.getLogger(__name__)
        logger.debug(f"Cost estimation not supported for {self.__class__.__name__}")
        return None

    def list_tables(self, schema: Optional[str] = None) -> List[str]:
        """List tables using Ibis native method"""
        if self.conn is None:
            self.connect()
        
        if schema:
            return self.conn.list_tables(database=schema)
        else:
            return self.conn.list_tables()

    def _build_table_uid(self, table_name: str, schema: str) -> str:
        """Build unique table identifier (backend-specific format)"""
        db_id = self._get_database_id()
        if db_id:
            return f"{db_id}.{schema}.{table_name}"
        return f"{schema}.{table_name}"

    @abstractmethod
    def _get_database_id(self) -> Optional[str]:
        """Get the database/project/catalog ID for the connection"""
        pass

    def close(self):
        """Close database connection and clear cache"""
        if self.conn is not None:
            self.conn = None
            self._metadata_cache.clear()

    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()
