"""
Database connection and query execution using Ibis
Supports BigQuery and Snowflake
"""

import ibis
import polars as pl
from typing import Optional, Dict, Any, List
from pathlib import Path
import json


class DatabaseConnection:
    """Handles database connections using Ibis framework"""

    SUPPORTED_BACKENDS = ['bigquery', 'snowflake']

    def __init__(self, backend: str, **connection_params):
        """
        Initialize database connection

        Args:
            backend: Database backend ('bigquery' or 'snowflake')
            **connection_params: Backend-specific connection parameters
                For BigQuery cross-project queries (e.g., public datasets):
                    project_id: Your billing project (where jobs run)
                    source_project_id: Source project containing the data (optional)
                    dataset_id: Dataset to query
        """
        if backend.lower() not in self.SUPPORTED_BACKENDS:
            raise ValueError(
                f"Backend '{backend}' not supported. "
                f"Supported backends: {', '.join(self.SUPPORTED_BACKENDS)}"
            )

        self.backend = backend.lower()
        self.connection_params = connection_params
        self.source_project_id = connection_params.get('source_project_id')  # For cross-project BigQuery queries
        self.conn = None

    def connect(self) -> ibis.BaseBackend:
        """
        Establish database connection

        Returns:
            Ibis backend connection
        """
        if self.conn is not None:
            return self.conn

        try:
            if self.backend == 'bigquery':
                self.conn = self._connect_bigquery()
            elif self.backend == 'snowflake':
                self.conn = self._connect_snowflake()

            print(f"✅ Connected to {self.backend.upper()}")
            return self.conn

        except Exception as e:
            raise ConnectionError(f"Failed to connect to {self.backend}: {str(e)}")

    def _connect_bigquery(self) -> ibis.BaseBackend:
        """Connect to BigQuery"""
        # BigQuery connection parameters
        project_id = self.connection_params.get('project_id')
        dataset_id = self.connection_params.get('dataset_id')
        credentials_path = self.connection_params.get('credentials_path')
        credentials_json = self.connection_params.get('credentials_json')

        if not project_id:
            raise ValueError("BigQuery requires 'project_id' parameter")

        conn_kwargs = {'project_id': project_id}

        # Handle credentials
        if credentials_path:
            conn_kwargs['credentials'] = credentials_path
        elif credentials_json:
            # If credentials are provided as JSON string/dict
            if isinstance(credentials_json, str):
                credentials_json = json.loads(credentials_json)
            conn_kwargs['credentials'] = credentials_json

        if dataset_id:
            conn_kwargs['dataset_id'] = dataset_id

        return ibis.bigquery.connect(**conn_kwargs)

    def _connect_snowflake(self) -> ibis.BaseBackend:
        """Connect to Snowflake"""
        # Snowflake connection parameters
        required_params = ['account', 'user', 'password']
        for param in required_params:
            if param not in self.connection_params:
                raise ValueError(f"Snowflake requires '{param}' parameter")

        conn_kwargs = {
            'user': self.connection_params['user'],
            'password': self.connection_params['password'],
            'account': self.connection_params['account'],
        }

        # Optional parameters
        optional_params = ['database', 'schema', 'warehouse', 'role']
        for param in optional_params:
            if param in self.connection_params:
                conn_kwargs[param] = self.connection_params[param]

        return ibis.snowflake.connect(**conn_kwargs)

    def get_table(self, table_name: str, schema: Optional[str] = None) -> ibis.expr.types.Table:
        """
        Get table reference

        Args:
            table_name: Name of the table
            schema: Optional schema name (overrides connection default)

        Returns:
            Ibis table expression
        """
        if self.conn is None:
            self.connect()

        # For BigQuery cross-project queries (e.g., querying public datasets)
        if self.backend == 'bigquery' and self.source_project_id:
            dataset = schema or self.connection_params.get('dataset_id')
            if not dataset:
                raise ValueError("dataset_id is required for BigQuery cross-project queries")

            # Use fully-qualified table name for cross-project access
            full_table_name = f"`{self.source_project_id}.{dataset}.{table_name}`"
            # Use sql() to reference external table, then return as table expression
            return self.conn.sql(f"SELECT * FROM {full_table_name}")

        # Normal flow (same project or non-BigQuery)
        if schema:
            # Use schema.table notation
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
        """
        Execute query and return Polars DataFrame

        Args:
            table_name: Name of the table
            schema: Optional schema name
            limit: Limit number of rows
            custom_query: Custom SQL query (overrides table_name)
            sample_size: Sample size for large tables
            sampling_method: Sampling strategy ('random', 'recent', 'top', 'systematic')
            sampling_key_column: Column to use for non-random sampling methods
            columns: Optional list of column names to SELECT (default: all columns with SELECT *)

        Returns:
            Polars DataFrame with query results
        """
        if self.conn is None:
            self.connect()

        if custom_query:
            # Execute raw SQL only if explicitly provided
            # For BigQuery with cross-project queries, we need to qualify table names
            if self.backend == 'bigquery':
                dataset = schema or self.connection_params.get('dataset_id')

                # If we have a source_project_id (cross-project query), use it
                if self.source_project_id and dataset:
                    # Build fully qualified table name
                    full_table_name = f"`{self.source_project_id}.{dataset}.{table_name}`"

                    # Replace unqualified table reference in the query
                    # This handles cases like "SELECT * FROM transactions LIMIT 100"
                    # and converts to "SELECT * FROM `project.dataset.transactions` LIMIT 100"
                    import re
                    # Match table name as a standalone word (not part of column names)
                    pattern = r'\bFROM\s+' + re.escape(table_name) + r'\b'
                    custom_query = re.sub(pattern, f'FROM {full_table_name}', custom_query, flags=re.IGNORECASE)

                    # Also handle JOIN clauses
                    pattern = r'\bJOIN\s+' + re.escape(table_name) + r'\b'
                    custom_query = re.sub(pattern, f'JOIN {full_table_name}', custom_query, flags=re.IGNORECASE)
                elif dataset:
                    # No cross-project, but still need dataset qualification
                    # Replace unqualified table reference with dataset.table
                    import re
                    pattern = r'\bFROM\s+' + re.escape(table_name) + r'\b'
                    custom_query = re.sub(pattern, f'FROM `{dataset}.{table_name}`', custom_query, flags=re.IGNORECASE)

                    pattern = r'\bJOIN\s+' + re.escape(table_name) + r'\b'
                    custom_query = re.sub(pattern, f'JOIN `{dataset}.{table_name}`', custom_query, flags=re.IGNORECASE)

            result = self.conn.sql(custom_query)
        else:
            # Get table reference
            table = self.get_table(table_name, schema)

            # Select specific columns if provided
            if columns:
                table = table.select(columns)

            # Apply sampling if specified
            if sample_size:
                table = self._apply_sampling(
                    table,
                    sample_size,
                    sampling_method,
                    sampling_key_column
                )
            elif limit:
                table = table.limit(limit)

            result = table

        # Convert to Polars DataFrame directly
        # Ibis natively supports Polars conversion
        polars_df = result.to_polars()
        return polars_df

    def _apply_sampling(
        self,
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
            # Random sampling using database random function
            return table.order_by(ibis.random()).limit(sample_size)

        elif method == 'recent':
            # Most recent rows based on key column (descending order)
            if not key_column:
                raise ValueError("'recent' sampling method requires a key_column")
            return table.order_by(ibis.desc(key_column)).limit(sample_size)

        elif method == 'top':
            # First N rows based on key column (ascending order)
            if not key_column:
                raise ValueError("'top' sampling method requires a key_column")
            return table.order_by(key_column).limit(sample_size)

        elif method == 'systematic':
            # Systematic sampling using modulo on key column
            if not key_column:
                raise ValueError("'systematic' sampling method requires a key_column")

            # For systematic sampling, we need to estimate the stride
            # Try to get row count first (if available)
            try:
                row_count = table.count().execute()
                if row_count and row_count > sample_size:
                    stride = max(1, row_count // sample_size)
                    # Use modulo filter: WHERE key_column % stride = 0
                    return table.filter(table[key_column] % stride == 0).limit(sample_size)
                else:
                    # If we can't get count or table is small, fallback to limit
                    return table.limit(sample_size)
            except Exception:
                # If count fails, fallback to every Nth row with estimated stride
                # Default stride of 10 for safety
                stride = 10
                return table.filter(table[key_column] % stride == 0).limit(sample_size)

        else:
            raise ValueError(f"Unknown sampling method: {method}. Use 'random', 'recent', 'top', or 'systematic'")

    def get_all_tables(self, schema: Optional[str] = None) -> List[str]:
        """
        Get list of all tables in the schema from INFORMATION_SCHEMA

        Args:
            schema: Optional schema name (overrides connection default)

        Returns:
            List of table names
        """
        if self.conn is None:
            self.connect()

        table_names = []

        try:
            if self.backend == 'bigquery':
                # BigQuery INFORMATION_SCHEMA.TABLES
                dataset = schema or self.connection_params.get('dataset_id')
                if not dataset:
                    return table_names

                # Use source_project_id if querying external data
                project_for_metadata = self.source_project_id or self.connection_params.get('project_id')

                # Query INFORMATION_SCHEMA.TABLES for all tables
                info_schema_table = f"`{project_for_metadata}.{dataset}.INFORMATION_SCHEMA.TABLES`"

                # Get all tables (excluding views by default, or include them if needed)
                result = self.conn.sql(
                    f"SELECT table_name "
                    f"FROM {info_schema_table} "
                    f"WHERE table_type IN ('BASE TABLE', 'TABLE') "
                    f"ORDER BY table_name"
                ).to_polars()

                if len(result) > 0:
                    table_names = result['table_name'].to_list()

            elif self.backend == 'snowflake':
                # Snowflake INFORMATION_SCHEMA.TABLES
                schema_name = schema or self.connection_params.get('schema')
                database_name = self.connection_params.get('database')

                if not database_name or not schema_name:
                    return table_names

                # Query INFORMATION_SCHEMA.TABLES
                result = self.conn.sql(
                    f"SELECT table_name "
                    f"FROM {database_name}.INFORMATION_SCHEMA.TABLES "
                    f"WHERE table_schema = '{schema_name}' "
                    f"AND table_type = 'BASE TABLE' "
                    f"ORDER BY table_name"
                ).to_polars()

                if len(result) > 0:
                    table_names = result['table_name'].to_list()

        except Exception as e:
            print(f"⚠️  Could not query INFORMATION_SCHEMA for table list: {e}")

        return table_names

    def get_table_metadata(self, table_name: str, schema: Optional[str] = None) -> Dict[str, Any]:
        """
        Get table metadata from INFORMATION_SCHEMA including UID/unique identifiers

        Args:
            table_name: Name of the table
            schema: Optional schema name

        Returns:
            Dictionary with table metadata (row_count, created_time, table_type, etc.)
        """
        if self.conn is None:
            self.connect()

        metadata = {}

        try:
            if self.backend == 'bigquery':
                # BigQuery INFORMATION_SCHEMA.TABLES
                dataset = schema or self.connection_params.get('dataset_id')
                if not dataset:
                    return metadata

                # Use source_project_id if querying external data, otherwise use project_id
                project_for_metadata = self.source_project_id or self.connection_params.get('project_id')
                billing_project = self.connection_params.get('project_id')

                # Query INFORMATION_SCHEMA.TABLES using Ibis SQL - use region INFORMATION_SCHEMA
                # In BigQuery, INFORMATION_SCHEMA is dataset-scoped, not project-scoped
                info_schema_table = f"`{project_for_metadata}.{dataset}.INFORMATION_SCHEMA.TABLES`"

                # Use Ibis SQL interface for INFORMATION_SCHEMA views
                result = self.conn.sql(
                    f"SELECT table_catalog, table_schema, table_name, table_type, creation_time "
                    f"FROM {info_schema_table} "
                    f"WHERE table_name = '{table_name}'"
                ).to_polars()

                if len(result) > 0:
                    # Extract scalar values from Polars DataFrame
                    metadata['table_name'] = str(result['table_name'][0])
                    metadata['table_type'] = str(result['table_type'][0]) if result['table_type'][0] is not None else None
                    metadata['created_time'] = str(result['creation_time'][0]) if result['creation_time'][0] is not None else None
                    # Construct table UID
                    metadata['table_uid'] = f"{project_for_metadata}.{dataset}.{table_name}"

                # Get row count from __TABLES__ using Ibis table API
                # Note: __TABLES__ may not be accessible for cross-project queries
                try:
                    if self.source_project_id:
                        # For cross-project, use COUNT(*) instead of __TABLES__
                        # This is slower but works across projects
                        table_expr = self.get_table(table_name, schema)
                        count_result = table_expr.count().execute()
                        metadata['row_count'] = int(count_result) if count_result is not None else None
                    else:
                        # For same-project, use __TABLES__ (faster)
                        tables_meta = self.conn.table(f'{dataset}.__TABLES__')
                        row_result = (
                            tables_meta
                            .filter(tables_meta.table_id == table_name)
                            .select(['row_count'])
                            .to_polars()
                        )
                        if len(row_result) > 0:
                            metadata['row_count'] = int(row_result['row_count'][0]) if row_result['row_count'][0] is not None else None
                except:
                    pass

                # Get partition and clustering information from INFORMATION_SCHEMA.COLUMNS
                try:
                    info_schema_columns = f"`{project_for_metadata}.{dataset}.INFORMATION_SCHEMA.COLUMNS`"

                    partition_cluster_result = self.conn.sql(
                        f"SELECT column_name, is_partitioning_column, clustering_ordinal_position "
                        f"FROM {info_schema_columns} "
                        f"WHERE table_name = '{table_name}' "
                        f"AND (is_partitioning_column = 'YES' OR clustering_ordinal_position IS NOT NULL) "
                        f"ORDER BY clustering_ordinal_position"
                    ).to_polars()

                    if len(partition_cluster_result) > 0:
                        # Extract partition column
                        partition_cols = partition_cluster_result.filter(
                            pl.col('is_partitioning_column') == 'YES'
                        )
                        if len(partition_cols) > 0:
                            metadata['partition_column'] = str(partition_cols['column_name'][0])
                            # Note: partition_type (DAY, HOUR, etc.) is not in INFORMATION_SCHEMA.COLUMNS
                            # Would need to query table options or DDL for this
                            metadata['partition_type'] = 'TIME'  # Default assumption for BigQuery

                        # Extract clustering columns
                        cluster_cols = partition_cluster_result.filter(
                            pl.col('clustering_ordinal_position').is_not_null()
                        ).sort('clustering_ordinal_position')

                        if len(cluster_cols) > 0:
                            metadata['clustering_columns'] = cluster_cols['column_name'].to_list()
                except Exception as e:
                    print(f"⚠️  Could not get partition/cluster info: {e}")

            elif self.backend == 'snowflake':
                # Snowflake INFORMATION_SCHEMA.TABLES
                database = self.connection_params.get('database')
                schema_name = schema or self.connection_params.get('schema', 'PUBLIC')

                info_schema = self.conn.table(f'{database}.INFORMATION_SCHEMA.TABLES')
                result = (
                    info_schema
                    .filter(
                        (info_schema.table_schema == schema_name) &
                        (info_schema.table_name == table_name.upper())
                    )
                    .select(['table_name', 'row_count', 'created', 'table_type', 'clustering_key'])
                    .to_polars()
                )

                if len(result) > 0:
                    # Extract scalar values from Polars DataFrame
                    metadata['table_name'] = str(result['TABLE_NAME'][0])
                    metadata['row_count'] = int(result['ROW_COUNT'][0]) if result['ROW_COUNT'][0] is not None else None
                    metadata['created_time'] = str(result['CREATED'][0]) if result['CREATED'][0] is not None else None
                    metadata['table_type'] = str(result['TABLE_TYPE'][0]) if result['TABLE_TYPE'][0] is not None else None
                    # Snowflake table UID is database.schema.table
                    metadata['table_uid'] = f"{database}.{schema_name}.{table_name}"

                    # Extract clustering key if present
                    if 'CLUSTERING_KEY' in result.columns and result['CLUSTERING_KEY'][0] is not None:
                        clustering_key = str(result['CLUSTERING_KEY'][0])
                        if clustering_key and clustering_key != 'null':
                            metadata['clustering_key'] = clustering_key

        except Exception as e:
            print(f"⚠️  Could not get table metadata from INFORMATION_SCHEMA: {e}")

        return metadata

    def get_primary_key_columns(self, table_name: str, schema: Optional[str] = None) -> list:
        """
        Get primary key column names from INFORMATION_SCHEMA

        Note: BigQuery primary keys are NOT ENFORCED (metadata only since 2023)
              Snowflake primary keys can be enforced or not enforced

        Args:
            table_name: Name of the table
            schema: Optional schema name (dataset for BigQuery, schema for Snowflake)

        Returns:
            List of column names that form the primary key (empty if none declared)
        """
        if self.conn is None:
            self.connect()

        primary_keys = []

        try:
            if self.backend == 'bigquery':
                # BigQuery supports primary key declarations (NOT ENFORCED) since 2023
                # They're stored in INFORMATION_SCHEMA as metadata
                project_id = self.connection_params.get('project_id')
                dataset_id = schema or self.connection_params.get('dataset_id')

                if not dataset_id:
                    return primary_keys

                try:
                    # Query for primary key constraints
                    # Note: BigQuery PK constraints are NOT ENFORCED but exist as metadata
                    constraints_table = f'{project_id}.{dataset_id}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS'
                    constraints = self.conn.table(constraints_table)
                    pk_constraints = (
                        constraints
                        .filter(
                            (constraints.table_name == table_name.upper()) &
                            (constraints.constraint_type == 'PRIMARY KEY')
                        )
                        .select('constraint_name')
                        .to_polars()
                    )

                    if len(pk_constraints) > 0:
                        constraint_name = pk_constraints['constraint_name'][0]

                        # Get column names for this constraint
                        key_columns_table = f'{project_id}.{dataset_id}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE'
                        key_columns = self.conn.table(key_columns_table)
                        pk_columns = (
                            key_columns
                            .filter(
                                (key_columns.table_name == table_name.upper()) &
                                (key_columns.constraint_name == constraint_name)
                            )
                            .select('column_name')
                            .order_by('ordinal_position')
                            .to_polars()
                        )

                        primary_keys = [str(col).lower() for col in pk_columns['column_name'].to_list()]
                except Exception as e:
                    # If INFORMATION_SCHEMA query fails, return empty (table might not have PK declared)
                    pass

                return primary_keys

            elif self.backend == 'snowflake':
                # Snowflake stores primary key info in INFORMATION_SCHEMA.TABLE_CONSTRAINTS
                database = self.connection_params.get('database')
                schema_name = schema or self.connection_params.get('schema', 'PUBLIC')

                # Query for primary key constraints
                constraints = self.conn.table(f'{database}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS')
                pk_constraints = (
                    constraints
                    .filter(
                        (constraints.table_schema == schema_name) &
                        (constraints.table_name == table_name.upper()) &
                        (constraints.constraint_type == 'PRIMARY KEY')
                    )
                    .select('constraint_name')
                    .to_polars()
                )

                if len(pk_constraints) > 0:
                    constraint_name = pk_constraints['CONSTRAINT_NAME'][0]

                    # Get column names for this constraint
                    key_columns = self.conn.table(f'{database}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE')
                    pk_columns = (
                        key_columns
                        .filter(
                            (key_columns.table_schema == schema_name) &
                            (key_columns.table_name == table_name.upper()) &
                            (key_columns.constraint_name == constraint_name)
                        )
                        .select('column_name')
                        .to_polars()
                    )

                    primary_keys = [str(col).lower() for col in pk_columns['COLUMN_NAME'].to_list()]

        except Exception as e:
            print(f"⚠️  Could not get primary key from INFORMATION_SCHEMA: {e}")

        return primary_keys

    def get_table_schema(self, table_name: str, schema: Optional[str] = None) -> Dict[str, str]:
        """
        Get table column names and their data types (lightweight metadata query)

        Args:
            table_name: Name of the table
            schema: Optional schema name

        Returns:
            Dictionary mapping column names to their data types (as strings)
        """
        if self.conn is None:
            self.connect()

        column_schema = {}

        try:
            if self.backend == 'bigquery':
                # BigQuery INFORMATION_SCHEMA.COLUMNS
                dataset = schema or self.connection_params.get('dataset_id')
                if not dataset:
                    return column_schema

                # Use source_project_id if querying external data
                project_for_metadata = self.source_project_id or self.connection_params.get('project_id')

                # Query INFORMATION_SCHEMA.COLUMNS
                info_schema_table = f"`{project_for_metadata}.{dataset}.INFORMATION_SCHEMA.COLUMNS`"

                result = self.conn.sql(
                    f"SELECT column_name, data_type "
                    f"FROM {info_schema_table} "
                    f"WHERE table_name = '{table_name}' "
                    f"ORDER BY ordinal_position"
                ).to_polars()

                if len(result) > 0:
                    for row in result.iter_rows(named=True):
                        column_schema[row['column_name']] = row['data_type']

            elif self.backend == 'snowflake':
                # Snowflake INFORMATION_SCHEMA.COLUMNS
                database = self.connection_params.get('database')
                schema_name = schema or self.connection_params.get('schema', 'PUBLIC')

                if not database or not schema_name:
                    return column_schema

                result = self.conn.sql(
                    f"SELECT column_name, data_type "
                    f"FROM {database}.INFORMATION_SCHEMA.COLUMNS "
                    f"WHERE table_schema = '{schema_name}' "
                    f"AND table_name = '{table_name.upper()}' "
                    f"ORDER BY ordinal_position"
                ).to_polars()

                if len(result) > 0:
                    for row in result.iter_rows(named=True):
                        # Snowflake returns uppercase column names
                        column_schema[str(row['COLUMN_NAME']).lower()] = str(row['DATA_TYPE'])

        except Exception as e:
            print(f"⚠️  Could not get table schema from INFORMATION_SCHEMA: {e}")

        return column_schema

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
        Estimate bytes that will be scanned by a query (BigQuery only)
        Uses dry_run to get accurate estimate without executing

        Args:
            table_name: Name of the table
            schema: Optional schema name
            custom_query: Custom SQL query (overrides table_name)
            sample_size: Sample size if sampling will be applied
            sampling_method: Sampling strategy
            sampling_key_column: Column for non-random sampling
            columns: Optional list of column names to SELECT (default: all columns with SELECT *)

        Returns:
            Estimated bytes to be scanned, or None if estimation not available
        """
        if self.backend != 'bigquery':
            # Only BigQuery supports dry run estimation
            return None

        if self.conn is None:
            self.connect()

        try:
            from google.cloud import bigquery

            # Get the underlying BigQuery client from Ibis connection
            # Ibis wraps the google-cloud-bigquery client
            bq_client = self.conn.client

            # Build the query we'll actually run
            if custom_query:
                # Use custom query
                dataset = schema or self.connection_params.get('dataset_id')

                # Handle cross-project references
                if self.source_project_id and dataset:
                    full_table_name = f"`{self.source_project_id}.{dataset}.{table_name}`"
                    import re
                    pattern = r'\bFROM\s+' + re.escape(table_name) + r'\b'
                    query = re.sub(pattern, f'FROM {full_table_name}', custom_query, flags=re.IGNORECASE)
                    pattern = r'\bJOIN\s+' + re.escape(table_name) + r'\b'
                    query = re.sub(pattern, f'JOIN {full_table_name}', query, flags=re.IGNORECASE)
                elif dataset:
                    import re
                    pattern = r'\bFROM\s+' + re.escape(table_name) + r'\b'
                    query = re.sub(pattern, f'FROM `{dataset}.{table_name}`', custom_query, flags=re.IGNORECASE)
                    pattern = r'\bJOIN\s+' + re.escape(table_name) + r'\b'
                    query = re.sub(pattern, f'JOIN `{dataset}.{table_name}`', query, flags=re.IGNORECASE)
                else:
                    query = custom_query
            else:
                # Build query from table reference
                dataset = schema or self.connection_params.get('dataset_id')

                if self.source_project_id and dataset:
                    full_table_name = f"`{self.source_project_id}.{dataset}.{table_name}`"
                elif dataset:
                    full_table_name = f"`{dataset}.{table_name}`"
                else:
                    full_table_name = table_name

                # Build column list for SELECT
                column_list = ", ".join(columns) if columns else "*"

                # Build SELECT query with sampling if needed
                if sample_size:
                    if sampling_method == 'random':
                        query = f"SELECT {column_list} FROM {full_table_name} ORDER BY RAND() LIMIT {sample_size}"
                    elif sampling_method == 'recent' and sampling_key_column:
                        query = f"SELECT {column_list} FROM {full_table_name} ORDER BY {sampling_key_column} DESC LIMIT {sample_size}"
                    elif sampling_method == 'top' and sampling_key_column:
                        query = f"SELECT {column_list} FROM {full_table_name} ORDER BY {sampling_key_column} ASC LIMIT {sample_size}"
                    elif sampling_method == 'systematic' and sampling_key_column:
                        # For systematic, we'll approximate with modulo
                        query = f"SELECT {column_list} FROM {full_table_name} WHERE MOD({sampling_key_column}, 10) = 0 LIMIT {sample_size}"
                    else:
                        query = f"SELECT {column_list} FROM {full_table_name} LIMIT {sample_size}"
                else:
                    query = f"SELECT {column_list} FROM {full_table_name}"

            # Configure dry run
            job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)

            # Debug: Print the query being estimated
            print(f"   🔍 Estimating query: {query[:200]}..." if len(query) > 200 else f"   🔍 Estimating query: {query}")

            # Run dry run
            query_job = bq_client.query(query, job_config=job_config)

            # Get estimated bytes
            return query_job.total_bytes_processed

        except Exception as e:
            print(f"⚠️  Could not estimate bytes for BigQuery query: {e}")
            return None

    def get_row_count(self, table_name: str, schema: Optional[str] = None, approximate: bool = True) -> int:
        """
        Get row count for a table

        Args:
            table_name: Name of the table
            schema: Optional schema name
            approximate: If True, use INFORMATION_SCHEMA for fast approximate count (default: True)
                        If False, use COUNT(*) for exact count (slower, more expensive)

        Returns:
            Number of rows in the table
        """
        if self.conn is None:
            self.connect()

        # Use INFORMATION_SCHEMA for fast approximate counts
        if approximate:
            try:
                if self.backend == 'bigquery':
                    # BigQuery INFORMATION_SCHEMA uses __TABLES__ for row counts
                    # Note: __TABLES__ may not be accessible for cross-project queries
                    dataset = schema or self.connection_params.get('dataset_id')
                    if not dataset:
                        raise ValueError("Dataset must be specified for BigQuery row count")

                    # Use source_project_id if querying external data
                    project_for_metadata = self.source_project_id or self.connection_params.get('project_id')

                    # For cross-project queries, __TABLES__ might not be accessible
                    # Fall through to COUNT(*) if this fails
                    if self.source_project_id:
                        # Skip __TABLES__ for cross-project, use COUNT(*) instead
                        raise Exception("Cross-project __TABLES__ not supported, using COUNT(*)")

                    # Use Ibis API to query __TABLES__ metadata (same-project only)
                    tables_meta = self.conn.table(f'{project_for_metadata}.{dataset}.__TABLES__')
                    result = (
                        tables_meta
                        .filter(tables_meta.table_id == table_name)
                        .select('row_count')
                        .to_polars()
                    )
                    if len(result) > 0 and result['row_count'][0] is not None:
                        return int(result['row_count'][0])

                elif self.backend == 'snowflake':
                    # Snowflake INFORMATION_SCHEMA
                    database = self.connection_params.get('database')
                    schema_name = schema or self.connection_params.get('schema', 'PUBLIC')

                    # Use Ibis API to query INFORMATION_SCHEMA
                    info_schema = self.conn.table(f'{database}.INFORMATION_SCHEMA.TABLES')
                    result = (
                        info_schema
                        .filter(
                            (info_schema.table_schema == schema_name) &
                            (info_schema.table_name == table_name.upper())
                        )
                        .select('row_count')
                        .to_polars()
                    )
                    if len(result) > 0:
                        return int(result['ROW_COUNT'][0])

            except Exception as e:
                print(f"⚠️  Could not get approximate row count from INFORMATION_SCHEMA: {e}")
                print(f"   Falling back to COUNT(*) query...")

        # Fallback to exact count using COUNT(*)
        try:
            table = self.get_table(table_name, schema)
            count_result = table.count().to_polars()
            # Polars returns a DataFrame with one row, extract the value
            return int(count_result[0, 0])
        except Exception as e:
            print(f"⚠️  Could not get exact count: {e}")
            # If all else fails, return None to skip row count
            return None

    def list_tables(self, schema: Optional[str] = None) -> list:
        """
        List available tables

        Args:
            schema: Optional schema name to filter tables

        Returns:
            List of table names
        """
        if self.conn is None:
            self.connect()

        if schema:
            return self.conn.list_tables(database=schema)
        else:
            return self.conn.list_tables()

    def close(self):
        """Close database connection"""
        if self.conn is not None:
            # Ibis connections don't always need explicit closing,
            # but we'll set to None to allow garbage collection
            self.conn = None
            print(f"🔒 Closed {self.backend.upper()} connection")

    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()


def create_connection(backend: str, **connection_params) -> DatabaseConnection:
    """
    Factory function to create database connection

    Args:
        backend: Database backend ('bigquery' or 'snowflake')
        **connection_params: Backend-specific connection parameters

    Returns:
        DatabaseConnection instance

    Examples:
        # BigQuery
        conn = create_connection(
            'bigquery',
            project_id='my-project',
            dataset_id='my_dataset',
            credentials_path='/path/to/credentials.json'
        )

        # Snowflake
        conn = create_connection(
            'snowflake',
            account='my-account',
            user='my-user',
            password='my-password',
            database='my_db',
            schema='my_schema',
            warehouse='my_warehouse'
        )
    """
    return DatabaseConnection(backend, **connection_params)
