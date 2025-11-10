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
            FROM {database}.INFORMATION_SCHEMA.TABLES
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

        # Query 2: Columns + Primary Keys (single joined query), then split to two frames
        try:
            columns_pk_query = f"""
            WITH PK AS (
                SELECT
                    tc.TABLE_NAME,
                    kcu.COLUMN_NAME,
                    kcu.ORDINAL_POSITION AS PK_ORDINAL_POSITION
                FROM {database}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                JOIN {database}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                    ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                    AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
                WHERE tc.TABLE_SCHEMA = '{schema_name}'
                  AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                  {table_filter_qualified}
            )
            SELECT
                '{schema_name}' AS schema_name,
                c.TABLE_NAME,
                c.COLUMN_NAME,
                c.DATA_TYPE,
                c.ORDINAL_POSITION,
                c.COMMENT,
                IFF(pk.COLUMN_NAME IS NOT NULL, TRUE, FALSE) AS IS_PK,
                pk.PK_ORDINAL_POSITION
            FROM {database}.INFORMATION_SCHEMA.COLUMNS c
            LEFT JOIN PK pk
              ON c.TABLE_NAME = pk.TABLE_NAME AND c.COLUMN_NAME = pk.COLUMN_NAME
            WHERE c.TABLE_SCHEMA = '{schema_name}'
              {table_filter_columns}
            ORDER BY c.TABLE_NAME, c.ORDINAL_POSITION
            """
            logger.debug(f"[query] Snowflake metadata columns+PK query:\n{columns_pk_query}")
            combined_df = self.conn.sql(columns_pk_query).to_polars()

            # Normalize metadata column names to lowercase (keep actual Snowflake column names as-is)
            combined_df = normalize_snowflake_columns(combined_df, {
                'SCHEMA_NAME': 'schema_name',
                'TABLE_NAME': 'table_name',
                'COLUMN_NAME': 'column_name',
                'DATA_TYPE': 'data_type',
                'ORDINAL_POSITION': 'ordinal_position',
                'COMMENT': 'description',
                'IS_PK': 'is_pk',
                'PK_ORDINAL_POSITION': 'pk_ordinal_position'
            })

            # Split into columns and PK DataFrames
            new_columns_df, new_pk_df = split_columns_pk_dataframe(
                combined_df,
                is_pk_column='is_pk',
                pk_ordinal_column='pk_ordinal_position'
            )

            # Store in cache entry
            cache_entry['columns_df'] = new_columns_df
            cache_entry['pk_df'] = new_pk_df
        except Exception as e:
            logger.error(f"Could not fetch columns/primary key metadata: {e}")
            cache_entry['columns_df'] = pl.DataFrame()
            cache_entry['pk_df'] = pl.DataFrame()

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
        metadata = super().get_table_metadata(table_name, schema, project_id)

        # Add clustering_key if present
        effective_schema = schema or self.connection_params.get('default_schema')
        cache_key = (None, effective_schema)  # Snowflake always uses None for project_id

        if cache_key in self._metadata_cache:
            cache_entry = self._metadata_cache[cache_key]
            tables_df = cache_entry.get('tables_df')

            if tables_df is not None:
                table_info = tables_df.filter(pl.col('table_name') == table_name)
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

        if schema:
            return self.conn.table(table_name, database=schema)
        else:
            return self.conn.table(table_name)

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
