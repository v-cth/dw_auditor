"""
BigQuery adapter implementation
"""

import ibis
import polars as pl
import json
import logging
import os
from typing import Optional, List, Dict, Any

from .base import BaseAdapter
from .utils import qualify_query_tables, apply_sampling
from .metadata_helpers import should_skip_query, split_columns_pk_dataframe, build_table_filters

logger = logging.getLogger(__name__)


class BigQueryAdapter(BaseAdapter):
    """BigQuery-specific adapter with cross-project support"""

    def __init__(self, **connection_params):
        super().__init__(**connection_params)
        self.source_project_id = connection_params.get('source_project_id')

    def connect(self) -> ibis.BaseBackend:
        """Establish BigQuery connection"""
        if self.conn is not None:
            return self.conn

        project_id = self.connection_params.get('project_id')
        schema = self.connection_params.get('schema')
        credentials_path = self.connection_params.get('credentials_path')
        credentials_json = self.connection_params.get('credentials_json')

        if not project_id:
            raise ValueError("BigQuery requires 'project_id' parameter")

        # Set environment variable to prevent "No project ID" warning from BigQuery client
        os.environ['GOOGLE_CLOUD_PROJECT'] = project_id

        conn_kwargs = {'project_id': project_id}

        if credentials_path:
            conn_kwargs['credentials'] = credentials_path
        elif credentials_json:
            if isinstance(credentials_json, str):
                credentials_json = json.loads(credentials_json)
            conn_kwargs['credentials'] = credentials_json

        if schema:
            conn_kwargs['dataset_id'] = schema

        self.conn = ibis.bigquery.connect(**conn_kwargs)
        logger.info("Connected to BIGQUERY")
        return self.conn

    def _fetch_all_metadata(self, schema: str, table_names: Optional[List[str]] = None, project_id: Optional[str] = None):
        """Fetch metadata for schema in fewer queries (filtered by table_names if provided)

        Optimizations:
        - Combine TABLES with __TABLES__ metadata (same-project only)
        - Combine COLUMNS with PRIMARY KEY info via joins, then split in-memory

        Args:
            schema: Schema/dataset name
            table_names: Optional list of specific table names to fetch (if None, fetch all)
            project_id: Optional project ID for cross-project queries
        """
        if self.conn is None:
            self.connect()

        # Use provided project_id or fall back to source_project_id or connection project
        project_for_metadata = project_id or self.source_project_id or self.connection_params.get('project_id')
        cache_key = (project_id, schema)

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

        # Query 1: Tables + row counts (filtered)
        # Note: __TABLES__ works for both same-project and cross-project queries
        try:
            tables_query = f"""
            SELECT
                '{schema}' AS schema_name,
                t.table_name,
                t.table_type,
                t.creation_time,
                rt.row_count,
                rt.size_bytes,
                TIMESTAMP_MILLIS(rt.creation_time) AS created_at,
                TIMESTAMP_MILLIS(rt.last_modified_time) AS modified_at
            FROM `{project_for_metadata}.{schema}.INFORMATION_SCHEMA.TABLES` t
            LEFT JOIN `{project_for_metadata}.{schema}.__TABLES__` rt
                ON rt.table_id = t.table_name
            WHERE t.table_type IN ('BASE TABLE', 'TABLE', 'VIEW', 'MATERIALIZED VIEW') {table_filter_tables}
            ORDER BY t.table_name
            """
            logger.debug(f"[query] BigQuery metadata tables query:\n{tables_query}")
            new_tables_df = self.conn.sql(tables_query).to_polars()

            # Store in cache entry
            cache_entry['tables_df'] = new_tables_df

            # Also expose rowcount-like frame
            cache_entry['rowcount_df'] = new_tables_df.select([
                pl.col("schema_name"),
                pl.col("table_name").alias("table_id"),
                pl.col("row_count"),
                pl.col("size_bytes"),
                pl.col("created_at"),
                pl.col("modified_at"),
            ])
        except Exception as e:
            logger.error(f"Could not fetch tables/rowcount metadata: {e}")
            cache_entry['tables_df'] = pl.DataFrame()
            cache_entry['rowcount_df'] = pl.DataFrame()

        # Query 2: Columns + Primary Keys (single joined query), then split to two frames
        try:
                columns_pk_query = f"""
            WITH pk AS (
                SELECT
                    tc.table_name,
                    kcu.column_name,
                    kcu.ordinal_position AS pk_ordinal_position
                FROM `{project_for_metadata}.{schema}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS` tc
                JOIN `{project_for_metadata}.{schema}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE` kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_name = kcu.table_name
                WHERE tc.constraint_type = 'PRIMARY KEY' {table_filter_qualified}
            ),
            col_desc AS (
                SELECT
                    table_name,
                    column_name,
                    description
                FROM `{project_for_metadata}.{schema}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS`
                {table_filter_only}
            )
            SELECT
                '{schema}' AS schema_name,
                c.table_name,
                c.column_name,
                c.data_type,
                c.ordinal_position,
                c.is_partitioning_column,
                c.clustering_ordinal_position,
                cd.description,
                CASE WHEN pk.column_name IS NOT NULL THEN TRUE ELSE FALSE END AS is_pk,
                pk.pk_ordinal_position
            FROM `{project_for_metadata}.{schema}.INFORMATION_SCHEMA.COLUMNS` c
            LEFT JOIN pk
                ON c.table_name = pk.table_name AND c.column_name = pk.column_name
            LEFT JOIN col_desc cd
                ON c.table_name = cd.table_name AND c.column_name = cd.column_name
            {table_filter_columns}
            ORDER BY c.table_name, c.ordinal_position
            """
                logger.debug(f"[query] BigQuery metadata columns+PK query:\n{columns_pk_query}")
                combined_df = self.conn.sql(columns_pk_query).to_polars()

                # Split into columns and PK DataFrames
                base_columns_df, new_pk_df = split_columns_pk_dataframe(
                    combined_df,
                    is_pk_column="is_pk",
                    pk_ordinal_column="pk_ordinal_position"
                )

                # Add BigQuery-specific columns (partition, clustering, description)
                new_columns_df = base_columns_df.select([
                    pl.col("schema_name"),
                    pl.col("table_name"),
                    pl.col("column_name"),
                    pl.col("data_type"),
                    pl.col("ordinal_position"),
                    pl.col("is_partitioning_column"),
                    pl.col("clustering_ordinal_position"),
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

    def get_table(self, table_name: str, schema: Optional[str] = None, project_id: Optional[str] = None) -> ibis.expr.types.Table:
        """Get BigQuery table reference

        Args:
            table_name: Name of the table
            schema: Dataset name (schema/dataset)
            project_id: Optional project ID for cross-project queries
        """
        if self.conn is None:
            self.connect()

        # Determine the project to use (priority: parameter > source_project_id > connection project)
        target_project = project_id or self.source_project_id
        dataset = schema or self.connection_params.get('schema')

        # Cross-project query support
        if target_project and target_project != self.connection_params.get('project_id'):
            if not dataset:
                raise ValueError("schema is required for BigQuery cross-project queries")

            full_table_name = f"`{target_project}.{dataset}.{table_name}`"
            return self.conn.sql(f"SELECT * FROM {full_table_name}")

        # Normal flow (same project)
        if dataset:
            full_name = f"{dataset}.{table_name}"
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
        columns: Optional[List[str]] = None,
        project_id: Optional[str] = None
    ) -> pl.DataFrame:
        """Execute BigQuery query"""
        if self.conn is None:
            self.connect()

        if custom_query:
            dataset = schema or self.connection_params.get('schema')
            target_project = project_id or self.source_project_id

            if target_project and dataset:
                custom_query = qualify_query_tables(
                    custom_query, table_name, dataset, target_project
                )
            elif dataset:
                custom_query = qualify_query_tables(
                    custom_query, table_name, dataset
                )

            logger.debug(f"[query] BigQuery custom query:\n{custom_query}")
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
                logger.debug(f"[query] BigQuery generated query:\n{compiled_query}")
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
        """Estimate bytes using BigQuery dry_run"""
        if self.conn is None:
            self.connect()

        try:
            from google.cloud import bigquery

            bq_client = self.conn.client
            dataset = schema or self.connection_params.get('schema')

            if custom_query:
                if self.source_project_id and dataset:
                    query = qualify_query_tables(
                        custom_query, table_name, dataset, self.source_project_id
                    )
                elif dataset:
                    query = qualify_query_tables(
                        custom_query, table_name, dataset
                    )
                else:
                    query = custom_query
            else:
                if self.source_project_id and dataset:
                    full_table_name = f"`{self.source_project_id}.{dataset}.{table_name}`"
                elif dataset:
                    full_table_name = f"`{dataset}.{table_name}`"
                else:
                    full_table_name = table_name

                column_list = ", ".join(columns) if columns else "*"

                if sample_size:
                    if sampling_method == 'random':
                        query = f"SELECT {column_list} FROM {full_table_name} ORDER BY RAND() LIMIT {sample_size}"
                    elif sampling_method == 'recent' and sampling_key_column:
                        query = f"SELECT {column_list} FROM {full_table_name} ORDER BY {sampling_key_column} DESC LIMIT {sample_size}"
                    elif sampling_method == 'top' and sampling_key_column:
                        query = f"SELECT {column_list} FROM {full_table_name} ORDER BY {sampling_key_column} ASC LIMIT {sample_size}"
                    elif sampling_method == 'systematic' and sampling_key_column:
                        query = f"SELECT {column_list} FROM {full_table_name} WHERE MOD({sampling_key_column}, 10) = 0 LIMIT {sample_size}"
                    else:
                        query = f"SELECT {column_list} FROM {full_table_name} LIMIT {sample_size}"
                else:
                    query = f"SELECT {column_list} FROM {full_table_name}"

            job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)

            logger.debug(f"Estimating query: {query[:200]}..." if len(query) > 200 else f"Estimating query: {query}")

            query_job = bq_client.query(query, job_config=job_config)
            return query_job.total_bytes_processed

        except Exception as e:
            logger.warning(f"Could not estimate bytes: {e}")
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
        """Build BigQuery table UID: project.dataset.table"""
        project = self.source_project_id or self.connection_params.get('project_id')
        return f"{project}.{schema}.{table_name}"
