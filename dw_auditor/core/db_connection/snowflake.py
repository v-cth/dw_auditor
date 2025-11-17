"""
Snowflake adapter implementation
"""

import ibis
import polars as pl
import logging
from typing import Optional, List, Dict, Any

from .base import BaseAdapter
from .utils import apply_sampling
from .metadata_helpers import split_columns_pk_dataframe, normalize_snowflake_columns, build_table_filters

logger = logging.getLogger(__name__)


class SnowflakeAdapter(BaseAdapter):
    """Snowflake-specific adapter"""

    def _normalize_table_name(self, table_name: str) -> str:
        """Snowflake stores table names in uppercase by default"""
        return table_name.upper()

    def connect(self) -> ibis.BaseBackend:
        """Establish Snowflake connection"""
        if self.conn is not None:
            return self.conn

        # Check for required parameters
        if 'account' not in self.connection_params:
            raise ValueError("Snowflake requires 'account' parameter")
        if 'user' not in self.connection_params:
            raise ValueError("Snowflake requires 'user' parameter")

        # Password is required unless using external browser authentication
        authenticator = self.connection_params.get('authenticator')
        if not authenticator and 'password' not in self.connection_params:
            raise ValueError("Snowflake requires 'password' parameter (or set authenticator='externalbrowser' for SSO)")

        conn_kwargs = {
            'user': self.connection_params['user'],
            'account': self.connection_params['account'],
        }

        # Add password if provided (not needed for externalbrowser)
        if 'password' in self.connection_params:
            conn_kwargs['password'] = self.connection_params['password']

        # Map unified naming to Snowflake terms
        if 'default_database' in self.connection_params:
            conn_kwargs['database'] = self.connection_params['default_database']
        if 'default_schema' in self.connection_params:
            conn_kwargs['schema'] = self.connection_params['default_schema']

        # Add other optional parameters
        optional_params = ['warehouse', 'role', 'authenticator']
        for param in optional_params:
            if param in self.connection_params:
                conn_kwargs[param] = self.connection_params[param]

        self.conn = ibis.snowflake.connect(**conn_kwargs)
        auth_method = "external browser" if authenticator == 'externalbrowser' else "username/password"
        logger.info(f"Connected to SNOWFLAKE ({auth_method})")
        return self.conn

    def _fetch_all_metadata(self, schema: str, table_names: Optional[List[str]] = None, project_id: Optional[str] = None):
        """Fetch metadata for schema in fewer queries (filtered by table_names if provided)

        Optimizations:
        - Tables query already includes row_count/bytes/timestamps (1 query)
        - Combine COLUMNS with PRIMARY KEY info via join, then split in-memory (1 query)

        Args:
            schema: Schema/dataset name
            table_names: Optional list of specific table names to fetch (if None, fetch all)
            project_id: Ignored for Snowflake (used for BigQuery cross-project queries)
        """
        if self.conn is None:
            self.connect()

        database = self.connection_params.get('default_database')
        if not database:
            raise ValueError("Snowflake requires 'default_database' parameter")

        schema_name = schema or self.connection_params.get('default_schema', 'PUBLIC')

        # Snowflake doesn't use project_id, always None in cache key
        cache_key = (None, schema)

        # Initialize cache entry if it doesn't exist
        if cache_key not in self._metadata_cache:
            self._metadata_cache[cache_key] = {
                'tables_df': None,
                'columns_df': None,
                'pk_df': None,
                'rowcount_df': None,
                'fetched_tables': None if table_names is None else set(t.upper() for t in table_names)
            }

        cache_entry = self._metadata_cache[cache_key]

        # Build WHERE clause filters for table filtering (Snowflake uses uppercase)
        filters = build_table_filters(table_names, normalize_uppercase=True)
        table_filter = filters['tables']
        table_filter_qualified = filters['qualified']
        table_filter_columns = filters['columns']

        # Query 1: Tables (filtered, includes row_count, size, timestamps, and clustering_key)
        try:
            tables_query = f"""
            SELECT
                '{schema_name}' AS schema_name,
                table_name,
                table_type,
                created,
                last_altered,
                row_count,
                bytes,
                clustering_key,
                comment AS description
            FROM {database}.INFORMATION_SCHEMA.TABLES AS t
            WHERE table_schema = '{schema_name}'
              AND table_type IN ('BASE TABLE', 'VIEW', 'MATERIALIZED VIEW')
              {table_filter}
            ORDER BY table_name
            """
            logger.debug(f"[query] Snowflake metadata tables query:\n{tables_query}")
            new_tables_df = self.conn.sql(tables_query).to_polars()

            # Normalize column names to lowercase
            new_tables_df = normalize_snowflake_columns(new_tables_df, {
                'SCHEMA_NAME': 'schema_name',
                'TABLE_NAME': 'table_name',
                'TABLE_TYPE': 'table_type',
                'CREATED': 'creation_time',
                'LAST_ALTERED': 'modified_at',
                'ROW_COUNT': 'row_count',
                'BYTES': 'size_bytes',
                'CLUSTERING_KEY': 'clustering_key',
                'DESCRIPTION': 'description'
            })

            # Add created_at as alias for creation_time for consistency with BigQuery
            new_tables_df = new_tables_df.with_columns(
                pl.col('creation_time').alias('created_at')
            )

            # Store in cache entry
            cache_entry['tables_df'] = new_tables_df
        except Exception as e:
            logger.error(f"Could not fetch tables metadata: {e}")
            cache_entry['tables_df'] = pl.DataFrame()

        # Query 2: Primary Keys using Snowflake SHOW command
        try:
            # First, execute SHOW PRIMARY KEYS
            show_pk_cmd = f"SHOW PRIMARY KEYS IN SCHEMA {database}.{schema_name}"
            logger.debug(f"[query] Snowflake show primary keys:\n{show_pk_cmd}")
            self.conn.raw_sql(show_pk_cmd)

            # Then scan the result
            pk_query = 'SELECT "table_name", "column_name", "key_sequence" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) ORDER BY "table_name", "key_sequence"'
            logger.debug(f"[query] Snowflake fetch PK results:\n{pk_query}")
            pk_df = self.conn.sql(pk_query).to_polars()

            # Normalize column names and add schema_name
            if len(pk_df) > 0:
                pk_df = normalize_snowflake_columns(pk_df, {
                    'table_name': 'table_name',
                    'column_name': 'column_name',
                    'key_sequence': 'ordinal_position'
                })
                # Add schema_name column
                pk_df = pk_df.with_columns(pl.lit(schema_name).alias('schema_name'))
                # Reorder columns to match expected format
                pk_df = pk_df.select(['schema_name', 'table_name', 'column_name', 'ordinal_position'])

            cache_entry['pk_df'] = pk_df
        except Exception as e:
            logger.warning(f"Could not fetch primary key metadata: {e}")
            cache_entry['pk_df'] = pl.DataFrame()

        # Query 3: Columns metadata
        try:
            columns_query = f"""
            SELECT
                '{schema_name}' AS schema_name,
                TABLE_NAME,
                COLUMN_NAME,
                DATA_TYPE,
                ORDINAL_POSITION,
                COMMENT
            FROM {database}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{schema_name}'
              {table_filter_columns}
            ORDER BY TABLE_NAME, ORDINAL_POSITION
            """
            logger.debug(f"[query] Snowflake metadata columns query:\n{columns_query}")
            columns_df = self.conn.sql(columns_query).to_polars()

            # Normalize metadata column names to lowercase
            columns_df = normalize_snowflake_columns(columns_df, {
                'SCHEMA_NAME': 'schema_name',
                'TABLE_NAME': 'table_name',
                'COLUMN_NAME': 'column_name',
                'DATA_TYPE': 'data_type',
                'ORDINAL_POSITION': 'ordinal_position',
                'COMMENT': 'description'
            })

            cache_entry['columns_df'] = columns_df
        except Exception as e:
            logger.error(f"Could not fetch columns metadata: {e}")
            cache_entry['columns_df'] = pl.DataFrame()

        # Snowflake includes row_count in TABLES query, so use that
        # Create rowcount_df with schema_name (consistent with BigQuery)
        tables_df = cache_entry.get('tables_df')
        if tables_df is not None and len(tables_df) > 0:
            cache_entry['rowcount_df'] = tables_df.select([
                pl.col('schema_name'),
                pl.col('table_name').alias('table_id'),
                pl.col('row_count'),
                pl.col('size_bytes'),
                pl.col('created_at'),
                pl.col('modified_at')
            ])
        else:
            cache_entry['rowcount_df'] = pl.DataFrame()

        # Update fetched_tables tracking
        cache_entry['fetched_tables'] = None if table_names is None else set(t.upper() for t in table_names)

    def get_table_metadata(self, table_name: str, schema: Optional[str] = None, project_id: Optional[str] = None) -> Dict[str, Any]:
        """Get table metadata with Snowflake-specific fields

        Note: project_id parameter is ignored for Snowflake (used for BigQuery cross-project queries)
        """
        # Base class handles table name normalization via _normalize_table_name()
        metadata = super().get_table_metadata(table_name, schema, project_id)

        # Add clustering_key if present
        effective_schema = schema or self.connection_params.get('default_schema')
        cache_key = (None, effective_schema)  # Snowflake always uses None for project_id

        if cache_key in self._metadata_cache:
            cache_entry = self._metadata_cache[cache_key]
            tables_df = cache_entry.get('tables_df')

            if tables_df is not None:
                # Use normalized table name for lookup
                table_name_normalized = self._normalize_table_name(table_name)
                table_info = tables_df.filter(pl.col('table_name') == table_name_normalized)
                if len(table_info) > 0 and 'clustering_key' in table_info.columns:
                    clustering_key = table_info['clustering_key'][0]
                    if clustering_key is not None and str(clustering_key) != 'null':
                        metadata['clustering_key'] = str(clustering_key)

        return metadata

    def get_table(self, table_name: str, schema: Optional[str] = None, project_id: Optional[str] = None) -> ibis.expr.types.Table:
        """Get Snowflake table reference

        Note: project_id parameter is ignored for Snowflake (used for BigQuery cross-project queries)
        """
        if self.conn is None:
            self.connect()

        # Normalize table name (uppercase for Snowflake)
        table_name_normalized = self._normalize_table_name(table_name)

        if schema:
            return self.conn.table(table_name_normalized, database=schema)
        else:
            return self.conn.table(table_name_normalized)

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
        project_id: Optional[str] = None
    ) -> pl.DataFrame:
        """Execute Snowflake query

        Note: project_id parameter is ignored for Snowflake (used for BigQuery cross-project queries)
        """
        if self.conn is None:
            self.connect()

        if custom_query:
            logger.debug(f"[query] Snowflake custom query:\n{custom_query}")
            result = self.conn.sql(custom_query)
        else:
            table = self.get_table(table_name, schema, project_id)

            if columns:
                table = table.select(columns)

            if sample_size:
                table = apply_sampling(table, sample_size, sampling_method, sampling_key_column)
            elif limit:
                table = table.limit(limit)

            result = table

            # Log the compiled SQL query
            try:
                compiled_query = ibis.to_sql(result)
                logger.debug(f"[query] Snowflake generated query:\n{compiled_query}")
            except Exception as e:
                logger.debug(f"[query] Could not compile query to SQL: {e}")

        return result.to_polars()

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
        """Snowflake does not support bytes estimation"""
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
        """Build Snowflake table UID: database.schema.table"""
        database = self.connection_params.get('default_database')
        return f"{database}.{schema}.{table_name}"
