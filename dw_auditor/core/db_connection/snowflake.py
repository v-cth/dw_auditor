"""
Snowflake adapter implementation
"""

import ibis
import polars as pl
from typing import Optional, List, Dict, Any

from .base import BaseAdapter
from .utils import apply_sampling


class SnowflakeAdapter(BaseAdapter):
    """Snowflake-specific adapter"""

    def connect(self) -> ibis.BaseBackend:
        """Establish Snowflake connection"""
        if self.conn is not None:
            return self.conn

        required_params = ['account', 'user', 'password']
        for param in required_params:
            if param not in self.connection_params:
                raise ValueError(f"Snowflake requires '{param}' parameter")

        conn_kwargs = {
            'user': self.connection_params['user'],
            'password': self.connection_params['password'],
            'account': self.connection_params['account'],
        }

        optional_params = ['database', 'schema', 'warehouse', 'role']
        for param in optional_params:
            if param in self.connection_params:
                conn_kwargs[param] = self.connection_params[param]

        self.conn = ibis.snowflake.connect(**conn_kwargs)
        print(f"✅ Connected to SNOWFLAKE")
        return self.conn

    def _fetch_all_metadata(self, schema: str, table_names: Optional[List[str]] = None):
        """Fetch metadata for schema in 3 queries (filtered by table_names if provided)"""
        if self.conn is None:
            self.connect()

        database = self.connection_params.get('database')
        if not database:
            raise ValueError("Snowflake requires 'database' parameter")

        schema_name = schema or self.connection_params.get('schema', 'PUBLIC')

        # Build WHERE clause for table filtering
        if table_names:
            # Snowflake stores table names in uppercase
            table_list = ", ".join(f"'{t.upper()}'" for t in table_names)
            table_filter = f"AND table_name IN ({table_list})"
            # For queries with JOINs where table_name is ambiguous, qualify with table alias
            table_filter_qualified = f"AND tc.table_name IN ({table_list})"
        else:
            table_filter = ""
            table_filter_qualified = ""

        # Query 1: Tables (filtered, includes row_count, size, timestamps, and clustering_key)
        try:
            tables_query = f"""
            SELECT
                table_name,
                table_type,
                created,
                last_altered,
                row_count,
                bytes,
                clustering_key
            FROM {database}.INFORMATION_SCHEMA.TABLES
            WHERE table_schema = '{schema_name}'
              AND table_type = 'BASE TABLE'
              {table_filter}
            ORDER BY table_name
            """
            self._tables_df = self.conn.sql(tables_query).to_polars()

            # Normalize column names to lowercase
            self._tables_df = self._tables_df.rename({
                'TABLE_NAME': 'table_name',
                'TABLE_TYPE': 'table_type',
                'CREATED': 'creation_time',
                'LAST_ALTERED': 'modified_at',
                'ROW_COUNT': 'row_count',
                'BYTES': 'size_bytes',
                'CLUSTERING_KEY': 'clustering_key'
            })

            # Add created_at as alias for creation_time for consistency with BigQuery
            self._tables_df = self._tables_df.with_columns(
                pl.col('creation_time').alias('created_at')
            )
        except Exception as e:
            print(f"⚠️  Could not fetch tables metadata: {e}")
            self._tables_df = pl.DataFrame()

        # Query 2: Columns (filtered)
        try:
            columns_query = f"""
            SELECT
                table_name,
                column_name,
                data_type,
                ordinal_position
            FROM {database}.INFORMATION_SCHEMA.COLUMNS
            WHERE table_schema = '{schema_name}'
              {table_filter}
            ORDER BY table_name, ordinal_position
            """
            self._columns_df = self.conn.sql(columns_query).to_polars()

            # Normalize column names to lowercase
            self._columns_df = self._columns_df.rename({
                'TABLE_NAME': 'table_name',
                'COLUMN_NAME': 'column_name',
                'DATA_TYPE': 'data_type',
                'ORDINAL_POSITION': 'ordinal_position'
            })

            # Lowercase actual column names for consistency
            self._columns_df = self._columns_df.with_columns(
                pl.col('column_name').str.to_lowercase().alias('column_name')
            )
        except Exception as e:
            print(f"⚠️  Could not fetch columns metadata: {e}")
            self._columns_df = pl.DataFrame()

        # Query 3: Primary keys (filtered)
        try:
            pk_query = f"""
            SELECT tc.table_name, kcu.column_name, kcu.ordinal_position
            FROM {database}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN {database}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            WHERE tc.table_schema = '{schema_name}'
              AND tc.constraint_type = 'PRIMARY KEY'
              {table_filter_qualified}
            ORDER BY tc.table_name, kcu.ordinal_position
            """
            self._pk_df = self.conn.sql(pk_query).to_polars()

            # Normalize column names
            self._pk_df = self._pk_df.rename({
                'TABLE_NAME': 'table_name',
                'COLUMN_NAME': 'column_name',
                'ORDINAL_POSITION': 'ordinal_position'
            })

            # Lowercase column names
            self._pk_df = self._pk_df.with_columns(
                pl.col('column_name').str.to_lowercase().alias('column_name')
            )
        except Exception as e:
            print(f"⚠️  Could not fetch primary keys: {e}")
            self._pk_df = pl.DataFrame()

        # Snowflake includes row_count in TABLES query, so use that
        if self._tables_df is not None and len(self._tables_df) > 0:
            self._rowcount_df = self._tables_df.select(['table_name', 'row_count'])
        else:
            self._rowcount_df = pl.DataFrame()

        self._cached_schema = schema

    def get_table_metadata(self, table_name: str, schema: Optional[str] = None) -> Dict[str, Any]:
        """Get table metadata with Snowflake-specific fields"""
        metadata = super().get_table_metadata(table_name, schema)

        # Add clustering_key if present
        effective_schema = schema or self.connection_params.get('schema')
        if effective_schema and self._tables_df is not None:
            table_info = self._tables_df.filter(pl.col('table_name') == table_name)
            if len(table_info) > 0 and 'clustering_key' in table_info.columns:
                clustering_key = table_info['clustering_key'][0]
                if clustering_key is not None and str(clustering_key) != 'null':
                    metadata['clustering_key'] = str(clustering_key)

        return metadata

    def get_table(self, table_name: str, schema: Optional[str] = None) -> ibis.expr.types.Table:
        """Get Snowflake table reference"""
        if self.conn is None:
            self.connect()

        if schema:
            full_name = f"{schema}.{table_name}"
            return self.conn.table(full_name)
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
        columns: Optional[List[str]] = None
    ) -> pl.DataFrame:
        """Execute Snowflake query"""
        if self.conn is None:
            self.connect()

        if custom_query:
            result = self.conn.sql(custom_query)
        else:
            table = self.get_table(table_name, schema)

            if columns:
                table = table.select(columns)

            if sample_size:
                table = apply_sampling(table, sample_size, sampling_method, sampling_key_column)
            elif limit:
                table = table.limit(limit)

            result = table

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
        database = self.connection_params.get('database')
        return f"{database}.{schema}.{table_name}"
