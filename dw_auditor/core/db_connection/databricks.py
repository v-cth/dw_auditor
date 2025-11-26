"""
Databricks adapter implementation
"""

import ibis
import polars as pl
import logging
import os
from typing import Optional, List, Dict, Any

from .base import BaseAdapter
from .utils import apply_sampling, qualify_query_tables
from .metadata_helpers import should_skip_query, split_columns_pk_dataframe, build_table_filters

logger = logging.getLogger(__name__)



class DatabricksAdapter(BaseAdapter):
    """Databricks-specific adapter with Unity Catalog and cross-catalog support"""

    def __init__(self, **connection_params):
        super().__init__(**connection_params)
        self.source_catalog = connection_params.get('source_catalog')

    def connect(self) -> ibis.BaseBackend:
        """Establish Databricks connection with OAuth/AAD or token authentication"""
        if self.conn is not None:
            return self.conn

        # Map unified naming to Databricks-specific terms
        default_database = self.connection_params.get('default_database')  # Databricks catalog
        default_schema = self.connection_params.get('default_schema', 'default')  # Databricks schema

        # Connection parameters
        server_hostname = self.connection_params.get('server_hostname')
        http_path = self.connection_params.get('http_path')

        # Authentication parameters (priority: auth_type → access_token → env vars)
        auth_type = self.connection_params.get('auth_type')
        access_token = self.connection_params.get('access_token')
        username = self.connection_params.get('username')
        password = self.connection_params.get('password')

        # Validate required parameters
        if not server_hostname:
            # Try environment variable
            server_hostname = os.environ.get('DATABRICKS_SERVER_HOSTNAME')
            if not server_hostname:
                raise ValueError("Databricks requires 'server_hostname' (or env: DATABRICKS_SERVER_HOSTNAME)")

        if not http_path:
            # Try environment variable
            http_path = os.environ.get('DATABRICKS_HTTP_PATH')
            if not http_path:
                raise ValueError("Databricks requires 'http_path' (or env: DATABRICKS_HTTP_PATH)")

        conn_kwargs = {
            'server_hostname': server_hostname,
            'http_path': http_path,
        }

        # Set authentication
        if auth_type:
            conn_kwargs['auth_type'] = auth_type
            if auth_type in ['databricks-oauth', 'azure-oauth', 'oauth']:
                # OAuth/AAD authentication - no token needed, uses browser flow
                logger.info("Using OAuth/AAD authentication")
            if username and password:
                conn_kwargs['username'] = username
                conn_kwargs['password'] = password
        elif access_token:
            conn_kwargs['access_token'] = access_token
            logger.info("Using Personal Access Token authentication")
        else:
            # Try environment variable
            access_token = os.environ.get('DATABRICKS_TOKEN')
            if access_token:
                conn_kwargs['access_token'] = access_token
                logger.info("Using Personal Access Token from environment")
            else:
                raise ValueError("Databricks requires authentication: 'auth_type' for OAuth/AAD or 'access_token' for token-based auth")

        # Set default catalog and schema if provided
        if default_database:
            conn_kwargs['catalog'] = default_database
        if default_schema:
            conn_kwargs['schema'] = default_schema

        self.conn = ibis.databricks.connect(**conn_kwargs)
        logger.info(f"Connected to DATABRICKS (catalog={default_database}, schema={default_schema})")
        return self.conn

    def _fetch_all_metadata(self, schema: str, table_names: Optional[List[str]] = None, database_id: Optional[str] = None):
        """Fetch metadata for schema in fewer queries (filtered by table_names if provided)

        Args:
            schema: Schema name
            table_names: Optional list of specific table names to fetch (if None, fetch all)
            database_id: Optional catalog name for cross-catalog queries (Databricks uses catalogs, not projects)
        """
        if self.conn is None:
            self.connect()

        # Use provided database_id (catalog) or fall back to source_catalog or default_database
        catalog_for_metadata = database_id or self.source_catalog or self.connection_params.get('default_database')

        if not catalog_for_metadata:
            raise ValueError("Databricks requires a catalog name for metadata queries")

        cache_key = (database_id, schema)

        # Initialize cache entry if it doesn't exist
        if cache_key not in self._metadata_cache:
            self._metadata_cache[cache_key] = {
                'tables_df': None,
                'columns_df': None,
                'pk_df': None,
                'rowcount_df': None,
                'fetched_tables': None if table_names is None else set(table_names)
            }

        cache_entry = self._metadata_cache[cache_key]

        # Build WHERE clause filters for table filtering
        filters = build_table_filters(table_names)
        table_filter_tables = filters['tables']
        table_filter_only = filters['only']
        table_filter_qualified = filters['qualified']
        table_filter_columns = filters['columns']

        # Query 1: Tables metadata from INFORMATION_SCHEMA
        # Note: In Databricks, INFORMATION_SCHEMA is at catalog level, not schema level
        try:
            tables_query = f"""
            SELECT
                '{schema}' AS schema_name,
                t.table_name,
                t.table_type,
                t.created AS creation_time,
                t.created AS created_at,
                t.last_altered AS modified_at,
                t.comment AS description
            FROM `{catalog_for_metadata}`.INFORMATION_SCHEMA.TABLES t
            WHERE t.table_schema = '{schema}'
                AND t.table_type IN ('BASE TABLE', 'TABLE', 'VIEW', 'MANAGED', 'EXTERNAL')
                {table_filter_tables}
            ORDER BY t.table_name
            """
            logger.debug(f"[query] Databricks metadata tables query:\n{tables_query}")
            new_tables_df = self.conn.sql(tables_query).to_polars()

            # Store in cache entry
            cache_entry['tables_df'] = new_tables_df

            # Query 1b: Fetch detailed table metadata using DESCRIBE EXTENDED
            # This gives us row counts and size from Statistics field
            rowcount_data = []
            if len(new_tables_df) > 0:
                for row in new_tables_df.iter_rows(named=True):
                    table_name = row['table_name']
                    # Get timestamps from INFORMATION_SCHEMA as fallback
                    table_created_at = row.get('created_at')
                    table_modified_at = row.get('modified_at')

                    try:
                        # Use DESCRIBE EXTENDED to get detailed table metadata
                        desc_query = f"DESCRIBE EXTENDED `{catalog_for_metadata}`.`{schema}`.`{table_name}`"
                        logger.debug(f"[query] Databricks table details: {desc_query}")

                        # Execute raw SQL and fetch results
                        result = self.conn.raw_sql(desc_query)
                        desc_df = result.to_polars()

                        # Parse the key-value pairs from DESCRIBE EXTENDED output
                        # Format: col_name='Statistics', data_type='1497 bytes, 7 rows'
                        # Note: We only extract statistics (row_count, size_bytes) from DESCRIBE EXTENDED
                        # Timestamps come from INFORMATION_SCHEMA for consistent formatting
                        metadata = {}
                        import re
                        for desc_row in desc_df.iter_rows(named=True):
                            key = str(desc_row['col_name']).strip() if desc_row['col_name'] else ''
                            value = str(desc_row['data_type']).strip() if desc_row['data_type'] else ''

                            if key == 'Statistics' and value:
                                # Parse format: "1497 bytes, 7 rows"
                                bytes_match = re.search(r'(\d+)\s+bytes', value)
                                rows_match = re.search(r'(\d+)\s+rows?', value)
                                if bytes_match:
                                    metadata['size_bytes'] = int(bytes_match.group(1))
                                if rows_match:
                                    metadata['row_count'] = int(rows_match.group(1))

                        # Always use INFORMATION_SCHEMA timestamps for consistent formatting
                        rowcount_data.append({
                            'schema_name': schema,
                            'table_id': table_name,
                            'row_count': metadata.get('row_count'),
                            'size_bytes': metadata.get('size_bytes'),
                            'created_at': table_created_at,
                            'modified_at': table_modified_at
                        })
                    except Exception as desc_error:
                        logger.debug(f"Could not fetch detailed metadata for {table_name}: {desc_error}")
                        # Add placeholder entry with INFORMATION_SCHEMA timestamps
                        rowcount_data.append({
                            'schema_name': schema,
                            'table_id': table_name,
                            'row_count': None,
                            'size_bytes': None,
                            'created_at': table_created_at,
                            'modified_at': table_modified_at
                        })

            cache_entry['rowcount_df'] = pl.DataFrame(rowcount_data) if rowcount_data else pl.DataFrame({
                'schema_name': [],
                'table_id': [],
                'row_count': [],
                'size_bytes': [],
                'created_at': [],
                'modified_at': []
            })
        except Exception as e:
            logger.error(f"Could not fetch tables metadata: {e}")
            cache_entry['tables_df'] = pl.DataFrame()
            cache_entry['rowcount_df'] = pl.DataFrame()

        # Query 2: Columns + Primary Keys (single joined query), then split to two frames
        try:
            # Note: Unity Catalog supports PRIMARY KEY constraints in INFORMATION_SCHEMA
            # INFORMATION_SCHEMA is at catalog level, not schema level
            columns_pk_query = f"""
            WITH pk AS (
                SELECT
                    tc.table_name,
                    kcu.column_name,
                    kcu.ordinal_position AS pk_ordinal_position
                FROM `{catalog_for_metadata}`.INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                JOIN `{catalog_for_metadata}`.INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_name = kcu.table_name
                    AND tc.table_schema = kcu.table_schema
                WHERE tc.constraint_type = 'PRIMARY KEY'
                    AND tc.table_schema = '{schema}'
                    {table_filter_qualified}
            )
            SELECT
                '{schema}' AS schema_name,
                c.table_name,
                c.column_name,
                c.data_type,
                c.ordinal_position,
                c.comment AS description,
                CASE WHEN pk.column_name IS NOT NULL THEN TRUE ELSE FALSE END AS is_pk,
                pk.pk_ordinal_position
            FROM `{catalog_for_metadata}`.INFORMATION_SCHEMA.COLUMNS c
            LEFT JOIN pk
                ON c.table_name = pk.table_name AND c.column_name = pk.column_name
            WHERE c.table_schema = '{schema}' {table_filter_columns}
            ORDER BY c.table_name, c.ordinal_position
            """
            logger.debug(f"[query] Databricks metadata columns+PK query:\n{columns_pk_query}")
            combined_df = self.conn.sql(columns_pk_query).to_polars()

            # Split into columns and PK DataFrames
            base_columns_df, new_pk_df = split_columns_pk_dataframe(
                combined_df,
                is_pk_column="is_pk",
                pk_ordinal_column="pk_ordinal_position"
            )

            # Add Databricks-specific columns (just description for now)
            new_columns_df = base_columns_df.select([
                pl.col("schema_name"),
                pl.col("table_name"),
                pl.col("column_name"),
                pl.col("data_type"),
                pl.col("ordinal_position"),
                pl.col("description"),
            ])

            # Store in cache entry
            cache_entry['columns_df'] = new_columns_df
            cache_entry['pk_df'] = new_pk_df
        except Exception as e:
            logger.error(f"Could not fetch columns/primary key metadata: {e}")
            cache_entry['columns_df'] = pl.DataFrame()
            cache_entry['pk_df'] = pl.DataFrame()

        # Update fetched_tables tracking
        cache_entry['fetched_tables'] = None if table_names is None else set(table_names)

    def get_table(self, table_name: str, schema: Optional[str] = None, database_id: Optional[str] = None) -> ibis.expr.types.Table:
        """Get Databricks table reference

        Args:
            table_name: Name of the table
            schema: Schema name
            database_id: Optional catalog name for cross-catalog queries
        """
        if self.conn is None:
            self.connect()

        # Determine the catalog to use (priority: parameter > source_catalog > default_database)
        target_catalog = database_id or self.source_catalog
        schema_name = schema or self.connection_params.get('default_schema')

        # Cross-catalog query support
        if target_catalog and target_catalog != self.connection_params.get('default_database'):
            if not schema_name:
                raise ValueError("schema is required for Databricks cross-catalog queries")

            # Use three-level namespace: catalog.schema.table
            full_table_name = f"`{target_catalog}`.`{schema_name}`.`{table_name}`"
            return self.conn.sql(f"SELECT * FROM {full_table_name}")

        # Normal flow (same catalog)
        # Note: Databricks Ibis backend uses 'database' parameter to specify schema
        if schema_name:
            return self.conn.table(table_name, database=schema_name)
        else:
            return self.conn.table(table_name)


    def _qualify_custom_query(
        self,
        custom_query: str,
        table_name: str,
        schema: Optional[str],
        database_id: Optional[str]
    ) -> str:
        """Qualify table names in Databricks custom query"""
        schema_name = schema or self.connection_params.get('default_schema')
        target_catalog = database_id or self.source_catalog or self.connection_params.get('default_database')

        # For Databricks, always use catalog.schema.table format
        if target_catalog and schema_name:
            return qualify_query_tables(
                custom_query, table_name, schema_name, target_catalog, dialect='databricks'
            )
        elif schema_name:
            # Fallback if no catalog (shouldn't happen for Databricks)
            return qualify_query_tables(
                custom_query, table_name, schema_name, 'main', dialect='databricks'
            )
        return custom_query

    def _get_database_id(self) -> Optional[str]:
        """Get Databricks catalog name"""
        return self.source_catalog or self.connection_params.get('default_database')

