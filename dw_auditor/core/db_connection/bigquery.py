"""
BigQuery adapter implementation
"""

import ibis
import polars as pl
import json
from typing import Optional, List, Dict, Any

from .base import BaseAdapter
from .utils import qualify_query_tables, apply_sampling


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
        print(f"✅ Connected to BIGQUERY")
        return self.conn

    def _fetch_all_metadata(self, schema: str, table_names: Optional[List[str]] = None):
        """Fetch metadata for schema in 4 queries (filtered by table_names if provided)"""
        if self.conn is None:
            self.connect()

        project_for_metadata = self.source_project_id or self.connection_params.get('project_id')

        # Build WHERE clause for table filtering
        if table_names:
            table_list = ", ".join(f"'{t}'" for t in table_names)
            table_filter = f"AND table_name IN ({table_list})"
            table_filter_only = f"WHERE table_name IN ({table_list})"
            # For queries with JOINs where table_name is ambiguous, qualify with table alias
            table_filter_qualified = f"AND tc.table_name IN ({table_list})"
        else:
            table_filter = ""
            table_filter_only = ""
            table_filter_qualified = ""

        # Query 1: Tables (filtered)
        try:
            tables_query = f"""
            SELECT table_name, table_type, creation_time
            FROM `{project_for_metadata}.{schema}.INFORMATION_SCHEMA.TABLES`
            WHERE table_type IN ('BASE TABLE', 'TABLE') {table_filter}
            ORDER BY table_name
            """
            self._tables_df = self.conn.sql(tables_query).to_polars()
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
                ordinal_position,
                is_partitioning_column,
                clustering_ordinal_position
            FROM `{project_for_metadata}.{schema}.INFORMATION_SCHEMA.COLUMNS`
            {table_filter_only}
            ORDER BY table_name, ordinal_position
            """
            self._columns_df = self.conn.sql(columns_query).to_polars()
        except Exception as e:
            print(f"⚠️  Could not fetch columns metadata: {e}")
            self._columns_df = pl.DataFrame()

        # Query 3: Primary keys (filtered)
        try:
            pk_query = f"""
            SELECT tc.table_name, kcu.column_name, kcu.ordinal_position
            FROM `{project_for_metadata}.{schema}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS` tc
            JOIN `{project_for_metadata}.{schema}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE` kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_name = kcu.table_name
            WHERE tc.constraint_type = 'PRIMARY KEY' {table_filter_qualified}
            ORDER BY tc.table_name, kcu.ordinal_position
            """
            self._pk_df = self.conn.sql(pk_query).to_polars()
        except Exception as e:
            print(f"⚠️  Could not fetch primary keys: {e}")
            self._pk_df = pl.DataFrame()

        # Query 4: Row counts, size, and timestamps from __TABLES__ (filtered, same-project only)
        try:
            if not self.source_project_id:
                if table_names:
                    table_id_filter = "WHERE table_id IN ({})".format(
                        ", ".join(f"'{t}'" for t in table_names)
                    )
                else:
                    table_id_filter = ""
                rowcount_query = f"""
                SELECT
                    table_id,
                    row_count,
                    size_bytes,
                    TIMESTAMP_MILLIS(creation_time) as created_at,
                    TIMESTAMP_MILLIS(last_modified_time) as modified_at
                FROM `{project_for_metadata}.{schema}.__TABLES__`
                {table_id_filter}
                """
                self._rowcount_df = self.conn.sql(rowcount_query).to_polars()
            else:
                # Cross-project: skip __TABLES__, will use COUNT(*) on demand
                self._rowcount_df = pl.DataFrame()
        except Exception as e:
            print(f"⚠️  Could not fetch row counts and table info: {e}")
            self._rowcount_df = pl.DataFrame()

        self._cached_schema = schema

    def get_table(self, table_name: str, schema: Optional[str] = None) -> ibis.expr.types.Table:
        """Get BigQuery table reference"""
        if self.conn is None:
            self.connect()

        # Cross-project query support
        if self.source_project_id:
            dataset = schema or self.connection_params.get('schema')
            if not dataset:
                raise ValueError("schema is required for BigQuery cross-project queries")

            full_table_name = f"`{self.source_project_id}.{dataset}.{table_name}`"
            return self.conn.sql(f"SELECT * FROM {full_table_name}")

        # Normal flow
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
        """Execute BigQuery query"""
        if self.conn is None:
            self.connect()

        if custom_query:
            dataset = schema or self.connection_params.get('schema')

            if self.source_project_id and dataset:
                custom_query = qualify_query_tables(
                    custom_query, table_name, dataset, self.source_project_id
                )
            elif dataset:
                custom_query = qualify_query_tables(
                    custom_query, table_name, dataset
                )

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

            print(f"   🔍 Estimating query: {query[:200]}..." if len(query) > 200 else f"   🔍 Estimating query: {query}")

            query_job = bq_client.query(query, job_config=job_config)
            return query_job.total_bytes_processed

        except Exception as e:
            print(f"⚠️  Could not estimate bytes: {e}")
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
