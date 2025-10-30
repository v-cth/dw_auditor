"""
Main auditor class that coordinates all auditing functionality
"""

import polars as pl
from typing import Dict, List, Optional, Union
from datetime import datetime, timezone
from pathlib import Path

# Import from checks package to trigger check registration
from ..checks import run_check_sync
from ..utils.security import mask_pii_columns, sanitize_connection_string
from ..utils.output import print_results
from .db_connection import DatabaseConnection
from ..insights import generate_column_insights
from .exporter_mixin import AuditorExporterMixin


# Complex types that don't support standard quality checks
COMPLEX_TYPES = [
    pl.Struct,     # Nested structures
    pl.List,       # Arrays/Lists
    pl.Array,      # Fixed-size arrays
    pl.Binary,     # Binary data
    pl.Object,     # Python objects
]


def is_complex_type(dtype) -> bool:
    """
    Check if dtype is a complex type that can't be audited with standard checks

    Args:
        dtype: Polars data type

    Returns:
        True if complex type, False otherwise
    """
    # Get the base type class
    dtype_class = type(dtype)

    # Check if it's one of the complex types
    for complex_type in COMPLEX_TYPES:
        if dtype_class == complex_type:
            return True
        # Also check isinstance for parameterized types like List[Int64]
        try:
            if isinstance(dtype, complex_type):
                return True
        except TypeError:
            # Some types don't support isinstance check
            pass

    return False


class SecureTableAuditor(AuditorExporterMixin):
    """Audit data warehouse tables with security controls"""

    def __init__(
        self,
        sample_size: int = 100000,
        outlier_threshold_pct: float = 0.0
    ):
        """
        Args:
            sample_size: Number of rows to sample (samples when table has more rows than this)
            outlier_threshold_pct: Minimum percentage to report outliers (default: 0.0 = report all)
        """
        self.sample_size = sample_size
        self.outlier_threshold_pct = outlier_threshold_pct
        self.audit_log = []

    @staticmethod
    def determine_columns_to_load(
        table_schema: Dict[str, str],
        table_name: str,
        column_check_config: Optional[any] = None,
        primary_key_columns: Optional[List[str]] = None,
        include_columns: Optional[List[str]] = None,
        exclude_columns: Optional[List[str]] = None,
        audit_mode: str = 'full',
        store_dataframe: bool = False
    ) -> List[str]:
        """
        Determine which columns to load based on checks, insights, filters, and mode

        Args:
            table_schema: Dictionary mapping column names to data types
            table_name: Name of the table being audited
            column_check_config: Optional configuration for column checks and insights
            store_dataframe: If True, load all columns (for relationship detection)
            primary_key_columns: Optional list of primary key column names (always included)
            include_columns: Optional list of columns to include (if specified, only load these)
            exclude_columns: Optional list of columns to exclude
            audit_mode: Audit mode ('full', 'checks', 'insights', 'discover')

        Returns:
            List of column names to load (empty list means load all columns)
        """
        if not table_schema:
            # If we can't get schema, fallback to loading all columns
            return []

        # Define unsupported complex types that Polars cannot handle
        unsupported_types = ['JSON', 'VARIANT', 'OBJECT', 'ARRAY', 'STRUCT', 'GEOGRAPHY', 'GEOMETRY']

        # If storing dataframe for relationship detection, load all non-complex columns
        if store_dataframe:
            result = []
            for col_name, data_type in table_schema.items():
                data_type_upper = data_type.upper()
                if not any(unsupported_type in data_type_upper for unsupported_type in unsupported_types):
                    result.append(col_name)
            return result

        # In discovery mode, load all columns except complex types
        if audit_mode == 'discover':
            result = []
            for col_name, data_type in table_schema.items():
                data_type_upper = data_type.upper()
                if not any(unsupported_type in data_type_upper for unsupported_type in unsupported_types):
                    result.append(col_name)
            return result

        columns_to_load = set()

        # Always include primary key columns (but filter complex types)
        if primary_key_columns:
            columns_to_load.update(primary_key_columns)

        # Process each column in the schema
        for column_name, data_type in table_schema.items():
            # Skip if in exclude list
            if exclude_columns and column_name in exclude_columns:
                continue

            # If include list specified, only load those columns (plus PKs)
            if include_columns and column_name not in include_columns:
                continue

            # Map BigQuery types to general categories
            data_type_upper = data_type.upper()

            # Skip unsupported complex types - Polars cannot handle these
            unsupported_types = ['JSON', 'VARIANT', 'OBJECT', 'ARRAY', 'STRUCT', 'GEOGRAPHY', 'GEOMETRY']
            if any(unsupported_type in data_type_upper for unsupported_type in unsupported_types):
                continue

            # Determine if this column should be loaded based on data type and config
            should_load = False

            # String types - load based on mode
            if any(t in data_type_upper for t in ['STRING', 'VARCHAR', 'TEXT', 'CHAR']):
                # In 'checks' or 'full' mode, check if string checks are enabled
                if audit_mode in ['checks', 'full']:
                    if column_check_config and hasattr(column_check_config, 'get_column_checks') and column_check_config.checks_enabled:
                        col_checks = column_check_config.get_column_checks(table_name, column_name, data_type)
                        # Load if any check is enabled
                        if any(col_checks.values()):
                            should_load = True
                    else:
                        # Default: load string columns for checks (only if checks_enabled not explicitly False)
                        if not column_check_config or column_check_config.checks_enabled:
                            should_load = True

                # In 'insights' or 'full' mode, also check if insights are configured
                if audit_mode in ['insights', 'full']:
                    if column_check_config and hasattr(column_check_config, 'get_column_insights') and column_check_config.insights_enabled:
                        col_insights = column_check_config.get_column_insights(table_name, column_name, data_type)
                        if col_insights:
                            should_load = True

            # DateTime/Date types - load based on mode
            elif any(t in data_type_upper for t in ['DATE', 'TIME', 'TIMESTAMP']):
                # In 'checks' or 'full' mode, check if datetime checks are enabled
                if audit_mode in ['checks', 'full']:
                    if column_check_config and hasattr(column_check_config, 'get_column_checks') and column_check_config.checks_enabled:
                        col_checks = column_check_config.get_column_checks(table_name, column_name, data_type)
                        if any(col_checks.values()):
                            should_load = True
                    else:
                        # Default: load datetime columns for checks (only if checks_enabled not explicitly False)
                        if not column_check_config or column_check_config.checks_enabled:
                            should_load = True

                # In 'insights' or 'full' mode, also check if insights are configured
                if audit_mode in ['insights', 'full']:
                    if column_check_config and hasattr(column_check_config, 'get_column_insights'):
                        col_insights = column_check_config.get_column_insights(table_name, column_name, data_type)
                        if col_insights:
                            should_load = True

            # Numeric types - load for checks (range validation) or insights
            elif any(t in data_type_upper for t in ['INT', 'FLOAT', 'DOUBLE', 'NUMERIC', 'DECIMAL', 'NUMBER']):
                # In 'checks' or 'full' mode, check if numeric range validation is configured
                if audit_mode in ['checks', 'full']:
                    if column_check_config and hasattr(column_check_config, 'get_column_checks') and column_check_config.checks_enabled:
                        col_checks = column_check_config.get_column_checks(table_name, column_name, data_type)
                        # Load if any range validation is configured
                        range_keys = ['min', 'max', 'greater_than', 'greater_than_or_equal', 'less_than', 'less_than_or_equal']
                        if any(key in col_checks for key in range_keys):
                            should_load = True

                # In 'insights' or 'full' mode, load for insights
                if audit_mode in ['insights', 'full']:
                    if column_check_config and hasattr(column_check_config, 'get_column_insights') and column_check_config.insights_enabled:
                        col_insights = column_check_config.get_column_insights(table_name, column_name, data_type)
                        if col_insights:
                            should_load = True

            # Boolean types - load only in insights or full mode
            elif any(t in data_type_upper for t in ['BOOL', 'BOOLEAN']):
                # Load booleans if insights configured
                # Skip in 'checks' mode since they have no checks
                if audit_mode in ['insights', 'full']:
                    if column_check_config and hasattr(column_check_config, 'get_column_insights'):
                        col_insights = column_check_config.get_column_insights(table_name, column_name, data_type)
                        if col_insights:
                            should_load = True

            if should_load:
                columns_to_load.add(column_name)

        # Convert to list, preserving schema order
        result = [col for col in table_schema.keys() if col in columns_to_load]

        return result

    def audit_from_database(
        self,
        table_name: str,
        backend: str,
        connection_params: Dict,
        schema: Optional[str] = None,
        mask_pii: bool = True,
        sample_in_db: bool = True,
        custom_query: Optional[str] = None,
        custom_pii_keywords: List[str] = None,
        user_primary_key: Optional[List[str]] = None,
        column_check_config: Optional[any] = None,
        sampling_method: str = 'random',
        sampling_key_column: Optional[str] = None,
        audit_mode: str = 'full',
        store_dataframe: bool = False,
        db_conn: Optional['DatabaseConnection'] = None
    ) -> Dict:
        """
        Audit table directly from database using Ibis (RECOMMENDED)
        No intermediate files - query directly and audit in memory

        Args:
            table_name: Name of table to audit
            backend: Database backend ('bigquery' or 'snowflake')
            connection_params: Database connection parameters (backend-specific)
                BigQuery example:
                {
                    'project_id': 'my-project',
                    'schema': 'my_dataset',
                    'credentials_path': '/path/to/credentials.json'
                }
                Snowflake example:
                {
                    'account': 'my-account',
                    'user': 'my-user',
                    'password': 'my-password',
                    'database': 'my_db',
                    'schema': 'my_schema',
                    'warehouse': 'my_warehouse'
                }
            schema: Schema name (optional, overrides connection default)
            mask_pii: Automatically mask columns with PII keywords
            sample_in_db: Use database sampling for large tables (faster & more secure)
            custom_query: Custom SQL query instead of SELECT * (advanced)
            custom_pii_keywords: Additional PII keywords beyond defaults
            audit_mode: Audit mode ('full', 'checks', 'insights', 'discover')
                - 'full': Run both quality checks and insights (default)
                - 'checks': Run quality checks only, skip insights
                - 'insights': Run insights only, skip quality checks
                - 'discover': Skip both (metadata only)
            store_dataframe: If True, store the Polars DataFrame in results['data']
                            (used for relationship detection, default: False)

        Returns:
            Dictionary with audit results
        """
        # Log the audit
        self._log_audit(table_name, f"{backend}://{connection_params.get('project_id') or connection_params.get('account')}")

        print(f"🔐 Secure audit mode: Direct database query via Ibis (no file export)")

        # Track timing for different phases
        phase_timings = {}
        phase_start = datetime.now()

        # Create or reuse database connection
        should_close_conn = False
        if db_conn is None:
            db_conn = DatabaseConnection(backend, **connection_params)
            db_conn.connect()
            should_close_conn = True
        phase_timings['connection'] = (datetime.now() - phase_start).total_seconds()

        try:
            # Get table metadata (including UID and row count)
            phase_start = datetime.now()
            table_metadata = {}
            row_count = None
            primary_key_columns = []

            try:
                table_metadata = db_conn.get_table_metadata(table_name, schema)
                if table_metadata:
                    if 'table_uid' in table_metadata:
                        print(f"🔖 Table UID: {table_metadata['table_uid']}")
                    if 'table_type' in table_metadata and table_metadata['table_type']:
                        table_type = table_metadata['table_type']
                        # INFORMATION_SCHEMA.TABLES returns readable types like "BASE TABLE", "VIEW", etc.
                        print(f"📑 Table type: {table_type}")
                    if 'row_count' in table_metadata and table_metadata['row_count'] is not None:
                        row_count = table_metadata['row_count']
                        print(f"📊 Table has {row_count:,} rows")

                    # Display partition information
                    if 'partition_column' in table_metadata and table_metadata['partition_column']:
                        partition_type = table_metadata.get('partition_type', 'UNKNOWN')
                        print(f"🔹 Partitioned by: {table_metadata['partition_column']} ({partition_type})")

                    # Display clustering information
                    if 'clustering_columns' in table_metadata and table_metadata['clustering_columns']:
                        cluster_cols = ', '.join(table_metadata['clustering_columns'])
                        print(f"🔸 Clustered by: {cluster_cols}")
                    elif 'clustering_key' in table_metadata and table_metadata['clustering_key']:
                        # Snowflake clustering key
                        print(f"🔸 Clustering key: {table_metadata['clustering_key']}")

            except Exception as e:
                print(f"⚠️  Could not get table metadata: {e}")

            # Get primary key columns - prioritize user-defined, then INFORMATION_SCHEMA
            if user_primary_key:
                primary_key_columns = user_primary_key
                print(f"🔑 Primary key from config: {', '.join(primary_key_columns)}")
                table_metadata['primary_key_columns'] = primary_key_columns
                table_metadata['primary_key_source'] = 'user_config'
            else:
                try:
                    primary_key_columns = db_conn.get_primary_key_columns(table_name, schema)
                    if primary_key_columns:
                        print(f"🔑 Primary key from schema: {', '.join(primary_key_columns)}")
                        table_metadata['primary_key_columns'] = primary_key_columns
                        table_metadata['primary_key_source'] = 'information_schema'
                except Exception as e:
                    print(f"⚠️  Could not get primary key info: {e}")

            # Fallback to row count query if not in metadata
            # For cross-project queries (e.g., BigQuery public datasets), skip COUNT(*) as it's too expensive
            is_cross_project = backend == 'bigquery' and hasattr(db_conn, 'source_project_id') and db_conn.source_project_id

            # If using a custom query, we need to count the result of that query, not the base table
            if custom_query:
                print(f"📝 Using custom query - will count rows from query result")
                # We'll count after executing the query
                row_count = None
            elif row_count is None and sample_in_db and not is_cross_project:
                try:
                    row_count = db_conn.get_row_count(table_name, schema)
                    if row_count is not None:
                        print(f"📊 Table has {row_count:,} rows")
                    else:
                        print(f"⚠️  Could not determine row count, will load full table")
                except Exception as e:
                    print(f"⚠️  Could not get row count: {e}")
                    print(f"   Will load full table")
            elif is_cross_project:
                print(f"⚠️  Cross-project query detected - skipping row count (too expensive)")
                print(f"   Will sample {self.sample_size:,} rows")

            phase_timings['metadata'] = (datetime.now() - phase_start).total_seconds()

            # Get table schema and determine which columns to load (optimization)
            phase_start = datetime.now()
            columns_to_load = None
            try:
                # Get table schema (column names and types)
                table_schema = db_conn.get_table_schema(table_name, schema)

                if table_schema:
                    # Get filter configuration
                    include_columns = getattr(column_check_config, 'include_columns', None) if column_check_config else None
                    exclude_columns = getattr(column_check_config, 'exclude_columns', None) if column_check_config else None

                    # Determine which columns to load
                    columns_to_load = self.determine_columns_to_load(
                        table_schema=table_schema,
                        table_name=table_name,
                        column_check_config=column_check_config,
                        primary_key_columns=primary_key_columns,
                        include_columns=include_columns,
                        exclude_columns=exclude_columns,
                        audit_mode=audit_mode,
                        store_dataframe=store_dataframe  # Load all columns if relationship detection is enabled
                    )

                    if columns_to_load:
                        print(f"📊 Optimized column loading: selecting {len(columns_to_load)}/{len(table_schema)} columns")
                    else:
                        print(f"📊 Loading all {len(table_schema)} columns")
                else:
                    print(f"⚠️  Could not get table schema, will load all columns")
            except Exception as e:
                print(f"⚠️  Column optimization failed ({e}), will load all columns")
                columns_to_load = None

            phase_timings['column_selection'] = (datetime.now() - phase_start).total_seconds()

            # Determine if we should sample
            # For custom queries, don't sample - use the query as-is (user controls the data with their query)
            # For cross-project, always sample since we don't know the row count
            if custom_query:
                should_sample = False
                print(f"ℹ️  Custom query provided - using query as-is (no additional sampling)")
            elif is_cross_project:
                should_sample = sample_in_db and (row_count is None or row_count > self.sample_size)
            else:
                should_sample = sample_in_db and row_count and row_count > self.sample_size

            if should_sample:
                method_display = sampling_method
                if sampling_key_column:
                    method_display = f"{sampling_method} (key: {sampling_key_column})"
                print(f"🔍 Sampling {self.sample_size:,} rows from table using '{method_display}' method")

            # Execute query
            phase_start = datetime.now()
            df = db_conn.execute_query(
                table_name=table_name,
                schema=schema,
                custom_query=custom_query,
                sample_size=self.sample_size if should_sample else None,
                sampling_method=sampling_method,
                sampling_key_column=sampling_key_column,
                columns=columns_to_load if columns_to_load else None
            )
            phase_timings['data_loading'] = (datetime.now() - phase_start).total_seconds()

            print(f"✅ Loaded {len(df):,} rows into memory")

            # For custom queries, the row count is the result set size
            if custom_query and row_count is None:
                row_count = len(df)
                print(f"📊 Custom query returned {row_count:,} rows")
                # Update metadata with query result count
                if table_metadata:
                    table_metadata['row_count'] = row_count
                    table_metadata['query_result_count'] = row_count

            # Mask PII if requested
            if mask_pii:
                df = mask_pii_columns(df, custom_pii_keywords)

            # Run audit - pass the actual row count, metadata, primary key columns, check config, and schema
            phase_start = datetime.now()
            results = self.audit_table(
                df,
                table_name,
                total_row_count=row_count,
                primary_key_columns=primary_key_columns,
                column_check_config=column_check_config,
                audit_mode=audit_mode,
                table_schema=table_schema,
                store_dataframe=store_dataframe
            )
            phase_timings['audit_checks'] = (datetime.now() - phase_start).total_seconds()

            # Add table metadata and phase timings to results
            if table_metadata:
                results['table_metadata'] = table_metadata
            results['phase_timings'] = phase_timings

            # Clear from memory
            del df

            return results

        finally:
            # Close connection only if we created it (not if it was passed in)
            if should_close_conn:
                db_conn.close()

    def audit_from_file(
        self,
        file_path: Union[str, Path],
        table_name: Optional[str] = None,
        mask_pii: bool = True,
        custom_pii_keywords: List[str] = None
    ) -> Dict:
        """
        Audit from CSV or Parquet file

        Args:
            file_path: Path to CSV or Parquet file
            table_name: Optional name for reporting
            mask_pii: Mask PII columns
            custom_pii_keywords: Additional PII keywords beyond defaults
        """
        file_path = Path(file_path)
        table_name = table_name or file_path.stem

        print(f"📁 Loading file: {file_path}")

        # Read based on extension
        if file_path.suffix.lower() == '.csv':
            df = pl.read_csv(file_path)
        elif file_path.suffix.lower() in ['.parquet', '.pq']:
            df = pl.read_parquet(file_path)
        else:
            raise ValueError(f"Unsupported file type: {file_path.suffix}")

        print(f"✅ Loaded {len(df):,} rows")

        # Mask PII if requested
        if mask_pii:
            df = mask_pii_columns(df, custom_pii_keywords)

        return self.audit_table(df, table_name)

    def audit_table(
        self,
        df: pl.DataFrame,
        table_name: str = "table",
        check_config: Optional[Dict] = None,
        total_row_count: Optional[int] = None,
        primary_key_columns: Optional[List[str]] = None,
        column_check_config: Optional[any] = None,
        audit_mode: str = 'full',
        table_schema: Optional[Dict[str, str]] = None,
        store_dataframe: bool = False
    ) -> Dict:
        """
        Main audit function - runs all checks on a Polars DataFrame

        Args:
            df: Polars DataFrame to audit
            table_name: Name of the table for reporting
            check_config: Optional configuration for which checks to run
            total_row_count: Optional total row count from database (if known)
            primary_key_columns: Optional list of primary key column names
            column_check_config: Optional column-level check configuration
            audit_mode: Audit mode ('full', 'checks', 'insights', 'discover')
            table_schema: Optional table schema (all columns with types) for showing unloaded columns

        Returns:
            Dictionary with audit results
        """
        # Start timing (capture both local and UTC time)
        start_time = datetime.now()
        start_time_utc = datetime.now(timezone.utc)

        # Determine the actual total row count
        # If we have it from the database, use that; otherwise use DataFrame length
        actual_total_rows = total_row_count if total_row_count is not None else len(df)

        print(f"\n{'='*60}")
        print(f"Auditing: {table_name}")
        print(f"Total rows in table: {actual_total_rows:,}")
        if total_row_count is not None and len(df) < actual_total_rows:
            print(f"Analyzing sample: {len(df):,} rows")
        print(f"{'='*60}\n")

        # Sample if needed (only if not already sampled in DB)
        analyzed_rows = len(df)
        if len(df) > self.sample_size and total_row_count is None:
            # Only sample in-memory if we didn't already sample in DB
            df = df.sample(n=min(self.sample_size, len(df)), seed=42)
            analyzed_rows = len(df)
            print(f"⚠️  Sampling {analyzed_rows:,} rows from loaded data\n")

        results = {
            'table_name': table_name,
            'total_rows': actual_total_rows,
            'sampled': analyzed_rows < actual_total_rows,
            'analyzed_rows': analyzed_rows,
            'columns': {},
            'column_summary': {},  # Summary for ALL columns
            'column_insights': {},  # Insights for columns
            'timestamp': start_time_utc.strftime('%Y-%m-%d %H:%M:%S UTC'),
            'start_time': start_time.isoformat()
        }

        # Default check config
        if check_config is None:
            check_config = {
                'trailing_characters': True,
                'ending_characters': False,
                'case_duplicates': True,
                'regex_patterns': False,  # No default - must be explicitly configured
                'numeric_strings': True,
                'timestamp_patterns': True
                # Date range checks: after, after_or_equal, before, before_or_equal (no defaults)
            }

        # Analyze each column and track check durations
        potential_keys = []
        check_durations = {}

        # Iterate over all columns in schema (if provided), otherwise just loaded columns
        all_columns = list(table_schema.keys()) if table_schema else df.columns

        for col in all_columns:
            col_start = datetime.now()

            # Check if column was loaded (may be missing if optimized out)
            if col not in df.columns:
                # Column exists in schema but wasn't loaded (optimization)
                results['column_summary'][col] = {
                    'dtype': table_schema[col] if table_schema else 'Unknown',
                    'null_count': 'N/A',
                    'null_pct': 'N/A',
                    'distinct_count': 'N/A',
                    'status': 'NOT_LOADED'
                }
                continue  # Skip to next column

            # Get column-specific check configuration
            col_dtype = str(df[col].dtype)
            if column_check_config and hasattr(column_check_config, 'get_column_checks') and column_check_config.checks_enabled:
                # Use column-specific config from the matrix
                col_check_config = column_check_config.get_column_checks(table_name, col, col_dtype)
            else:
                # Fallback to global check_config (or empty dict if checks disabled)
                col_check_config = check_config or {}

            col_results = self._audit_column(df, col, col_check_config, primary_key_columns, audit_mode)
            col_duration = (datetime.now() - col_start).total_seconds()

            # Determine status based on column type and issues found
            dtype = df[col].dtype
            if audit_mode == 'discover':
                # In discover mode, all columns are marked as DISCOVERED
                status = 'DISCOVERED'
            elif audit_mode == 'insights':
                # In insights mode, columns are marked as PROFILED
                status = 'PROFILED'
            elif dtype in [pl.Utf8, pl.String, pl.Datetime, pl.Date,
                          pl.Int8, pl.Int16, pl.Int32, pl.Int64,
                          pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64,
                          pl.Float32, pl.Float64]:
                # Columns that are checked for quality issues
                # Distinguish between "checks run and passed" vs "no checks configured"
                if col_results['issues']:
                    status = 'ERROR'
                elif col_results.get('checks_run'):
                    status = 'OK'  # Checks were run and passed
                else:
                    status = 'NO_CHECKS'  # No checks configured for this column

                if dtype in [pl.Utf8, pl.String]:
                    check_type = 'string_checks'
                elif dtype in [pl.Datetime, pl.Date]:
                    check_type = 'date_checks'
                else:
                    check_type = 'numeric_checks'
                check_durations[check_type] = check_durations.get(check_type, 0) + col_duration
            else:
                # Columns not checked (boolean, complex types, etc.)
                status = 'NOT_CHECKED'

            # Store summary for ALL columns
            results['column_summary'][col] = {
                'dtype': col_results['dtype'],
                'null_count': col_results['null_count'],
                'null_pct': col_results['null_pct'],
                'distinct_count': col_results['distinct_count'],
                'status': status
            }

            # Check if this column could be a primary key (unique + no nulls)
            if col_results['distinct_count'] == analyzed_rows and col_results['null_count'] == 0:
                potential_keys.append(col)

            # Store detailed results for columns with checks performed (issues or not)
            if col_results.get('checks_run'):
                results['columns'][col] = col_results

            # Generate column insights (skip in discover and checks modes, or if insights disabled in config)
            if audit_mode not in ['discover', 'checks'] and column_check_config and hasattr(column_check_config, 'get_column_insights') and column_check_config.insights_enabled:
                col_insights_config = column_check_config.get_column_insights(table_name, col, col_dtype)
                if col_insights_config:
                    insights = generate_column_insights(df, col, col_insights_config)
                    if insights:
                        results['column_insights'][col] = insights

        # Store check durations
        results['check_durations'] = check_durations

        # Store potential primary key columns
        if potential_keys:
            results['potential_primary_keys'] = potential_keys
            print(f"\n🔑 Potential primary key column(s): {', '.join(potential_keys)}")

        # Calculate duration
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        results['end_time'] = end_time.isoformat()
        results['duration_seconds'] = round(duration, 2)

        print_results(results)

        # Print duration breakdown
        print(f"\n⏱️  Audit Duration Breakdown:")
        if 'check_durations' in results and results['check_durations']:
            for check_type, check_duration in results['check_durations'].items():
                print(f"   • {check_type}: {check_duration:.3f}s")
        print(f"   • Total: {duration:.2f}s\n")

        # Store DataFrame if requested (for relationship detection)
        if store_dataframe:
            results['data'] = df

        return results

    def _audit_complex_column(self, df: pl.DataFrame, col: str) -> Dict:
        """
        Handle complex data types (Struct, List, Array, Binary)
        These types don't support standard quality checks

        Args:
            df: Polars DataFrame
            col: Column name

        Returns:
            Basic column metadata without quality checks
        """
        dtype = df[col].dtype
        total_rows = len(df)
        null_count = df[col].null_count()

        return {
            'dtype': str(dtype),
            'null_count': null_count,
            'null_pct': (null_count / total_rows * 100) if total_rows > 0 else 0,
            'distinct_count': None,  # Not applicable for complex types
            'issues': [],  # No checks for complex types
            'status': 'SKIPPED_COMPLEX_TYPE'
        }

    def _audit_column(self, df: pl.DataFrame, col: str, check_config: Dict, primary_key_columns: Optional[List[str]] = None, audit_mode: str = 'full') -> Dict:
        """Audit a single column for all issues

        Args:
            df: Polars DataFrame
            col: Column name
            check_config: Configuration for which checks to run
            primary_key_columns: Optional list of primary key column names
            audit_mode: Audit mode ('full', 'checks', 'insights', 'discover')
        """
        dtype = df[col].dtype

        # Check if complex type - skip detailed checks
        if is_complex_type(dtype):
            return self._audit_complex_column(df, col)

        null_count = df[col].null_count()
        total_rows = len(df)

        # Calculate distinct value count (excluding nulls)
        distinct_count = df[col].n_unique()

        col_result = {
            'dtype': str(dtype),
            'null_count': null_count,
            'null_pct': (null_count / total_rows * 100) if total_rows > 0 else 0,
            'distinct_count': distinct_count,
            'issues': [],
            'checks_run': []  # Track all checks performed
        }

        # In discover or insights mode, return only metadata (skip quality checks)
        if audit_mode in ['discover', 'insights']:
            return col_result

        # Skip if all nulls or masked
        if null_count == total_rows:
            return col_result

        # Skip masked columns
        first_non_null = df[col].drop_nulls().head(1)
        if len(first_non_null) > 0 and first_non_null[0] == "***PII_MASKED***":
            return col_result

        # String column checks
        if dtype in [pl.Utf8, pl.String]:
            trailing_config = check_config.get('trailing_characters', True)
            if trailing_config:
                before_count = len(col_result['issues'])
                # Pass patterns parameter if it's not just True
                if trailing_config is True:
                    results = run_check_sync('trailing_characters', df, col, primary_key_columns)
                else:
                    results = run_check_sync('trailing_characters', df, col, primary_key_columns, patterns=trailing_config)
                col_result['issues'].extend([r.model_dump() for r in results])
                after_count = len(col_result['issues'])
                issues_found = after_count - before_count
                col_result['checks_run'].append({
                    'name': 'Trailing Characters',
                    'status': 'FAILED' if issues_found > 0 else 'PASSED',
                    'issues_count': issues_found
                })

            ending_config = check_config.get('ending_characters', False)
            if ending_config:
                before_count = len(col_result['issues'])
                # Pass patterns parameter if it's not just True
                if ending_config is True:
                    results = run_check_sync('ending_characters', df, col, primary_key_columns)
                else:
                    results = run_check_sync('ending_characters', df, col, primary_key_columns, patterns=ending_config)
                col_result['issues'].extend([r.model_dump() for r in results])
                after_count = len(col_result['issues'])
                issues_found = after_count - before_count
                col_result['checks_run'].append({
                    'name': 'Ending Characters',
                    'status': 'FAILED' if issues_found > 0 else 'PASSED',
                    'issues_count': issues_found
                })

            if check_config.get('case_duplicates', True):
                before_count = len(col_result['issues'])
                results = run_check_sync('case_duplicates', df, col, primary_key_columns)
                col_result['issues'].extend([r.model_dump() for r in results])
                after_count = len(col_result['issues'])
                issues_found = after_count - before_count
                col_result['checks_run'].append({
                    'name': 'Case Duplicates',
                    'status': 'FAILED' if issues_found > 0 else 'PASSED',
                    'issues_count': issues_found
                })

            regex_config = check_config.get('regex_patterns', check_config.get('special_chars', False))
            if regex_config:
                before_count = len(col_result['issues'])

                # Parse config - can be string (pattern) or dict (full config)
                # Note: True/False are not valid - must provide explicit pattern
                if isinstance(regex_config, str):
                    # String: treat as pattern with contains mode
                    pattern = regex_config
                    mode = 'contains'
                    description = None
                elif isinstance(regex_config, dict):
                    # Dict: extract pattern, mode, and description
                    pattern = regex_config.get('pattern')
                    mode = regex_config.get('mode', 'contains')
                    description = regex_config.get('description')
                else:
                    # True/False or other - skip check (no default pattern)
                    pattern = None
                    mode = 'contains'
                    description = None

                if pattern:
                    results = run_check_sync('regex_pattern', df, col, primary_key_columns, pattern=pattern, mode=mode, description=description)
                    col_result['issues'].extend([r.model_dump() for r in results])
                    after_count = len(col_result['issues'])
                    issues_found = after_count - before_count
                    col_result['checks_run'].append({
                        'name': 'Regex Pattern',
                        'status': 'FAILED' if issues_found > 0 else 'PASSED',
                        'issues_count': issues_found
                    })

            if check_config.get('numeric_strings', True):
                before_count = len(col_result['issues'])
                results = run_check_sync('numeric_strings', df, col, primary_key_columns)
                col_result['issues'].extend([r.model_dump() for r in results])
                after_count = len(col_result['issues'])
                issues_found = after_count - before_count
                col_result['checks_run'].append({
                    'name': 'Numeric Strings',
                    'status': 'FAILED' if issues_found > 0 else 'PASSED',
                    'issues_count': issues_found
                })

            if check_config.get('uniqueness', False):
                before_count = len(col_result['issues'])
                results = run_check_sync('uniqueness', df, col, primary_key_columns)
                col_result['issues'].extend([r.model_dump() for r in results])
                after_count = len(col_result['issues'])
                issues_found = after_count - before_count
                col_result['checks_run'].append({
                    'name': 'Uniqueness',
                    'status': 'FAILED' if issues_found > 0 else 'PASSED',
                    'issues_count': issues_found
                })

        # Timestamp/Date checks
        elif dtype in [pl.Datetime, pl.Date]:
            if check_config.get('timestamp_patterns', True):
                before_count = len(col_result['issues'])
                results = run_check_sync('timestamp_patterns', df, col, primary_key_columns)
                col_result['issues'].extend([r.model_dump() for r in results])
                after_count = len(col_result['issues'])
                issues_found = after_count - before_count
                col_result['checks_run'].append({
                    'name': 'Timestamp Patterns',
                    'status': 'FAILED' if issues_found > 0 else 'PASSED',
                    'issues_count': issues_found
                })

            # Date range checks (similar to numeric range)
            range_params = {}
            range_desc_parts = []

            if 'after' in check_config:
                range_params['after'] = check_config['after']
                range_desc_parts.append(f"> {check_config['after']}")
            if 'after_or_equal' in check_config:
                range_params['after_or_equal'] = check_config['after_or_equal']
                range_desc_parts.append(f">= {check_config['after_or_equal']}")
            if 'before' in check_config:
                range_params['before'] = check_config['before']
                range_desc_parts.append(f"< {check_config['before']}")
            if 'before_or_equal' in check_config:
                range_params['before_or_equal'] = check_config['before_or_equal']
                range_desc_parts.append(f"<= {check_config['before_or_equal']}")

            # Only run check if at least one range parameter is defined
            if range_params:
                before_count = len(col_result['issues'])
                results = run_check_sync('date_range', df, col, primary_key_columns, **range_params)
                col_result['issues'].extend([r.model_dump() for r in results])
                after_count = len(col_result['issues'])
                issues_found = after_count - before_count

                range_desc = ', '.join(range_desc_parts)
                col_result['checks_run'].append({
                    'name': f'Date Range ({range_desc})',
                    'status': 'FAILED' if issues_found > 0 else 'PASSED',
                    'issues_count': issues_found
                })

            if check_config.get('future_dates', True):
                before_count = len(col_result['issues'])
                results = run_check_sync(
                    'future_dates', df, col, primary_key_columns,
                    threshold_pct=getattr(self, 'outlier_threshold_pct', 0.0)
                )
                col_result['issues'].extend([r.model_dump() for r in results])
                after_count = len(col_result['issues'])
                issues_found = after_count - before_count
                col_result['checks_run'].append({
                    'name': 'Future Dates',
                    'status': 'FAILED' if issues_found > 0 else 'PASSED',
                    'issues_count': issues_found
                })

            if check_config.get('uniqueness', False):
                before_count = len(col_result['issues'])
                results = run_check_sync('uniqueness', df, col, primary_key_columns)
                col_result['issues'].extend([r.model_dump() for r in results])
                after_count = len(col_result['issues'])
                issues_found = after_count - before_count
                col_result['checks_run'].append({
                    'name': 'Uniqueness',
                    'status': 'FAILED' if issues_found > 0 else 'PASSED',
                    'issues_count': issues_found
                })

        # Numeric range checks (all numeric types)
        if dtype in [pl.Int8, pl.Int16, pl.Int32, pl.Int64,
                     pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64,
                     pl.Float32, pl.Float64]:
            # Extract range validation parameters from check_config
            range_params = {}
            range_desc_parts = []

            if 'greater_than' in check_config:
                range_params['greater_than'] = check_config['greater_than']
                range_desc_parts.append(f"> {check_config['greater_than']}")
            if 'greater_than_or_equal' in check_config:
                range_params['greater_than_or_equal'] = check_config['greater_than_or_equal']
                range_desc_parts.append(f">= {check_config['greater_than_or_equal']}")
            if 'less_than' in check_config:
                range_params['less_than'] = check_config['less_than']
                range_desc_parts.append(f"< {check_config['less_than']}")
            if 'less_than_or_equal' in check_config:
                range_params['less_than_or_equal'] = check_config['less_than_or_equal']
                range_desc_parts.append(f"<= {check_config['less_than_or_equal']}")

            # Only run check if at least one range parameter is defined
            if range_params:
                before_count = len(col_result['issues'])
                results = run_check_sync('numeric_range', df, col, primary_key_columns, **range_params)
                col_result['issues'].extend([r.model_dump() for r in results])
                after_count = len(col_result['issues'])
                issues_found = after_count - before_count

                range_desc = ', '.join(range_desc_parts)
                col_result['checks_run'].append({
                    'name': f'Numeric Range ({range_desc})',
                    'status': 'FAILED' if issues_found > 0 else 'PASSED',
                    'issues_count': issues_found
                })

            # Uniqueness check for numeric columns
            if check_config.get('uniqueness', False):
                before_count = len(col_result['issues'])
                results = run_check_sync('uniqueness', df, col, primary_key_columns)
                col_result['issues'].extend([r.model_dump() for r in results])
                after_count = len(col_result['issues'])
                issues_found = after_count - before_count
                col_result['checks_run'].append({
                    'name': 'Uniqueness',
                    'status': 'FAILED' if issues_found > 0 else 'PASSED',
                    'issues_count': issues_found
                })

        return col_result

    def _log_audit(self, table_name: str, connection_string: str):
        """Log audit activity"""
        safe_conn = sanitize_connection_string(connection_string)

        log_entry = {
            'timestamp': datetime.now().isoformat(),
            'table': table_name,
            'connection': safe_conn
        }
        self.audit_log.append(log_entry)

    def get_audit_log(self) -> List[Dict]:
        """Get audit history"""
        return self.audit_log
