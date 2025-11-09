"""
Main auditor class that coordinates all auditing functionality
"""

import polars as pl
import logging
from enum import Enum
from typing import Dict, List, Optional, Union, Tuple, TYPE_CHECKING, Any, Type
from datetime import datetime, timezone
from pathlib import Path
from contextlib import contextmanager

if TYPE_CHECKING:
    from .config import AuditConfig

# Import from checks package to trigger check registration
from ..checks import run_check_sync
from ..utils.security import mask_pii_columns, sanitize_connection_string
from ..utils.output import print_results
from .db_connection import DatabaseConnection
from ..insights import generate_column_insights
from .exporter_mixin import AuditorExporterMixin

# Setup module logger
logger = logging.getLogger(__name__)


class AuditMode(Enum):
    """Enumeration of audit modes"""
    FULL = 'full'
    CHECKS = 'checks'
    INSIGHTS = 'insights'
    DISCOVER = 'discover'

    @classmethod
    def from_string(cls, mode: str) -> 'AuditMode':
        """Convert string to AuditMode enum"""
        mode_map = {e.value: e for e in cls}
        if mode not in mode_map:
            raise ValueError(f"Invalid audit mode: {mode}. Must be one of: {', '.join(mode_map.keys())}")
        return mode_map[mode]


@contextmanager
def timing_phase(phase_name: str, phase_timings: Dict[str, float]):
    """
    Context manager for timing audit phases

    Args:
        phase_name: Name of the phase being timed
        phase_timings: Dictionary to store timing results

    Yields:
        None
    """
    start_time = datetime.now()
    try:
        yield
    finally:
        duration = (datetime.now() - start_time).total_seconds()
        phase_timings[phase_name] = duration
        logger.debug(f"Phase '{phase_name}' completed in {duration:.3f}s")


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
        sample_size: int = 100000
    ):
        """
        Args:
            sample_size: Number of rows to sample (samples when table has more rows than this)
        """
        self.sample_size = sample_size
        self.audit_log = []

    def _get_table_metadata(
        self,
        db_conn: 'DatabaseConnection',
        table_name: str,
        schema: Optional[str],
        user_primary_key: Optional[List[str]],
        custom_query: Optional[str],
        backend: str,
        project_id: Optional[str] = None
    ) -> Tuple[Dict, Optional[int], List[str]]:
        """
        Get table metadata including row count and primary key columns

        Args:
            db_conn: Database connection
            table_name: Name of table
            schema: Schema name
            user_primary_key: User-defined primary key columns
            custom_query: Custom query if any
            backend: Database backend
            project_id: Optional project ID for cross-project queries (BigQuery only)

        Returns:
            Tuple of (metadata dict, row count, primary key columns)
        """
        table_metadata = {}
        row_count = None
        primary_key_columns = []

        # Get table metadata (including UID and row count)
        try:
            table_metadata = db_conn.get_table_metadata(table_name, schema, project_id)
            if table_metadata:
                if 'table_uid' in table_metadata:
                    logger.info(f"Table UID: {table_metadata['table_uid']}")
                if 'table_type' in table_metadata and table_metadata['table_type']:
                    table_type = table_metadata['table_type']
                    logger.info(f"Table type: {table_type}")
                if 'row_count' in table_metadata and table_metadata['row_count'] is not None:
                    row_count = table_metadata['row_count']
                    logger.info(f"Table has {row_count:,} rows")

                # Display partition information
                if 'partition_column' in table_metadata and table_metadata['partition_column']:
                    partition_type = table_metadata.get('partition_type', 'UNKNOWN')
                    logger.info(f"Partitioned by: {table_metadata['partition_column']} ({partition_type})")

                # Display clustering information
                if 'clustering_columns' in table_metadata and table_metadata['clustering_columns']:
                    cluster_cols = ', '.join(table_metadata['clustering_columns'])
                    logger.info(f"Clustered by: {cluster_cols}")
                elif 'clustering_key' in table_metadata and table_metadata['clustering_key']:
                    logger.info(f"Clustering key: {table_metadata['clustering_key']}")

        except Exception as e:
            logger.warning(f"Could not get table metadata: {e}")

        # Get primary key columns
        if user_primary_key:
            primary_key_columns = user_primary_key
            logger.info(f"Primary key from config: {', '.join(primary_key_columns)}")
            table_metadata['primary_key_columns'] = primary_key_columns
            table_metadata['primary_key_source'] = 'user_config'
        else:
            try:
                primary_key_columns = db_conn.get_primary_key_columns(table_name, schema, project_id)
                if primary_key_columns:
                    logger.info(f"Primary key from schema: {', '.join(primary_key_columns)}")
                    table_metadata['primary_key_columns'] = primary_key_columns
                    table_metadata['primary_key_source'] = 'information_schema'
            except Exception as e:
                logger.warning(f"Could not get primary key info: {e}")

        # Handle row count for different scenarios
        is_cross_project = backend == 'bigquery' and hasattr(db_conn, 'source_project_id') and db_conn.source_project_id

        if custom_query:
            logger.info("Using custom query - will count rows from query result")
            row_count = None
        elif row_count is None and not is_cross_project:
            try:
                row_count = db_conn.get_row_count(table_name, schema, project_id)
                if row_count is not None:
                    logger.info(f"Table has {row_count:,} rows")
                else:
                    logger.warning("Could not determine row count, will load full table")
            except Exception as e:
                logger.warning(f"Could not get row count: {e}")
                logger.info("Will load full table")
        elif is_cross_project:
            logger.warning("Cross-project query detected - skipping row count (too expensive)")
            logger.info(f"Will sample {self.sample_size:,} rows")

        return table_metadata, row_count, primary_key_columns

    def _load_data(
        self,
        db_conn: 'DatabaseConnection',
        table_name: str,
        schema: Optional[str],
        custom_query: Optional[str],
        columns_to_load: Optional[List[str]],
        should_sample: bool,
        sampling_method: str,
        sampling_key_column: Optional[str],
        project_id: Optional[str] = None
    ) -> pl.DataFrame:
        """
        Load data from database

        Args:
            db_conn: Database connection
            table_name: Name of table
            schema: Schema name
            custom_query: Custom query if any
            columns_to_load: List of columns to load
            should_sample: Whether to sample
            sampling_method: Sampling method
            sampling_key_column: Key column for sampling
            project_id: Optional project ID for cross-project queries (BigQuery only)

        Returns:
            Polars DataFrame with loaded data
        """
        if should_sample:
            method_display = sampling_method
            if sampling_key_column:
                method_display = f"{sampling_method} (key: {sampling_key_column})"
            logger.info(f"Sampling {self.sample_size:,} rows from table using '{method_display}' method")

        df = db_conn.execute_query(
            table_name=table_name,
            schema=schema,
            custom_query=custom_query,
            sample_size=self.sample_size if should_sample else None,
            sampling_method=sampling_method,
            sampling_key_column=sampling_key_column,
            columns=columns_to_load if columns_to_load else None,
            project_id=project_id
        )

        logger.info(f"Loaded {len(df):,} rows into memory")
        return df

    def _run_check_and_track(
        self,
        check_name: str,
        df: pl.DataFrame,
        col: str,
        primary_key_columns: List[str],
        col_result: Dict,
        **check_params
    ) -> None:
        """
        Run a check and update col_result in place

        Args:
            check_name: Name of the check to run (registry key)
            df: DataFrame to check
            col: Column name
            primary_key_columns: Primary key columns for context
            col_result: Dictionary to update with results
            **check_params: Additional parameters for the check

        Updates col_result['issues'] and col_result['checks_run'] in place
        """
        try:
            before_count = len(col_result['issues'])

            # Run the check
            results = run_check_sync(check_name, df, col, primary_key_columns, **check_params)

            # Add issues
            col_result['issues'].extend([r.model_dump() for r in results])

            # Track check execution
            after_count = len(col_result['issues'])
            issues_found = after_count - before_count

            # Get display name from registry
            from ..checks import CHECK_REGISTRY
            check_class = CHECK_REGISTRY.get(check_name)
            display_name = check_class.display_name if check_class else check_name

            col_result['checks_run'].append({
                'name': display_name,
                'status': 'FAILED' if issues_found > 0 else 'PASSED',
                'issues_count': issues_found
            })

        except Exception as e:
            logger.warning(f"Check '{check_name}' failed for column '{col}': {e}")
            # Track failed check
            col_result['checks_run'].append({
                'name': check_name,
                'status': 'ERROR',
                'issues_count': 0,
                'error': str(e)
            })

    def _map_check_config_to_params(
        self,
        check_name: str,
        config_value: Any,
        check_config: Optional[Dict] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Convert config values to check parameters

        Args:
            check_name: Name of the check
            config_value: Configuration value from YAML
                - True/False: run with defaults or skip
                - str: pattern parameter
                - dict: unpack as kwargs
                - list: patterns parameter
            check_config: Full check config (for range parameter extraction)

        Returns:
            Dictionary of parameters for the check, or None to skip
        """
        # False or None → skip check
        if config_value is False or config_value is None:
            return None

        # Special handling for range checks (extract from full config)
        if check_name == 'numeric_range' and check_config:
            # Extract range parameters: greater_than, greater_than_or_equal, less_than, less_than_or_equal
            range_params = {}
            for key in ['greater_than', 'greater_than_or_equal', 'less_than', 'less_than_or_equal']:
                if key in check_config:
                    range_params[key] = check_config[key]
            return range_params if range_params else None

        if check_name == 'date_range' and check_config:
            # Extract date range parameters: after, after_or_equal, before, before_or_equal
            range_params = {}
            for key in ['after', 'after_or_equal', 'before', 'before_or_equal']:
                if key in check_config:
                    range_params[key] = check_config[key]
            return range_params if range_params else None

        # True → run with defaults
        if config_value is True:
            return {}

        # String → pattern parameter (for regex checks)
        if isinstance(config_value, str):
            return {'pattern': config_value}

        # List → patterns parameter (for trailing/leading checks)
        if isinstance(config_value, list):
            return {'patterns': config_value}

        # Dict → use as-is
        if isinstance(config_value, dict):
            return config_value

        # Unknown type → run with defaults
        logger.warning(f"Unexpected config type for '{check_name}': {type(config_value)}, using defaults")
        return {}

    def _get_applicable_checks(
        self,
        dtype: Type[pl.DataType],
        check_config: Dict
    ) -> List[Tuple[str, Dict]]:
        """
        Discover which checks apply to this dtype based on config and check metadata

        Args:
            dtype: Polars data type of the column
            check_config: Configuration dict with check settings

        Returns:
            List of (check_name, params) tuples for checks that should run
        """
        from ..checks import CHECK_REGISTRY

        applicable = []

        for check_name, check_class in CHECK_REGISTRY.items():
            # Check if this check is in the config
            config_value = check_config.get(check_name)

            # Skip if not configured or explicitly disabled
            if config_value is False or config_value is None:
                continue

            # Check if dtype is supported
            supported_dtypes = check_class.supported_dtypes

            # Empty list means universal (works on all types)
            if not supported_dtypes:
                params = self._map_check_config_to_params(check_name, config_value, check_config)
                if params is not None:
                    applicable.append((check_name, params))
                continue

            # Check if column dtype matches any supported dtype
            dtype_matches = False
            for supported_dtype in supported_dtypes:
                if isinstance(dtype, type(supported_dtype)) or dtype == supported_dtype:
                    dtype_matches = True
                    break

            if dtype_matches:
                params = self._map_check_config_to_params(check_name, config_value, check_config)
                if params is not None:
                    applicable.append((check_name, params))

        return applicable

    @staticmethod
    def determine_columns_to_load(
        table_schema: Dict[str, Dict[str, Any]],
        table_name: str,
        column_check_config: Optional['AuditConfig'] = None,
        primary_key_columns: Optional[List[str]] = None,
        include_columns: Optional[List[str]] = None,
        exclude_columns: Optional[List[str]] = None,
        audit_mode: Union[str, AuditMode] = 'full',
        store_dataframe: bool = False
    ) -> List[str]:
        """
        Determine which columns to load based on checks, insights, filters, and mode

        Args:
            table_schema: Dictionary mapping column names to {'data_type': str, 'description': Optional[str]}
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
            for col_name, col_info in table_schema.items():
                data_type_upper = col_info['data_type'].upper()
                if not any(unsupported_type in data_type_upper for unsupported_type in unsupported_types):
                    result.append(col_name)
            return result

        # In discovery mode, load all columns except complex types
        if audit_mode == 'discover':
            result = []
            for col_name, col_info in table_schema.items():
                data_type_upper = col_info['data_type'].upper()
                if not any(unsupported_type in data_type_upper for unsupported_type in unsupported_types):
                    result.append(col_name)
            return result

        columns_to_load = set()

        # Always include primary key columns (but filter complex types)
        if primary_key_columns:
            columns_to_load.update(primary_key_columns)

        # Process each column in the schema
        for column_name, col_info in table_schema.items():
            data_type = col_info['data_type']
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
        custom_pii_keywords: Optional[List[str]] = None,
        user_primary_key: Optional[List[str]] = None,
        column_check_config: Optional['AuditConfig'] = None,
        sampling_method: str = 'random',
        sampling_key_column: Optional[str] = None,
        audit_mode: Union[str, AuditMode] = 'full',
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

        logger.info("Secure audit mode: Direct database query via Ibis (no file export)")

        # Track timing for different phases
        phase_timings = {}

        # Create or reuse database connection
        should_close_conn = False
        with timing_phase('connection', phase_timings):
            if db_conn is None:
                db_conn = DatabaseConnection(backend, **connection_params)
                db_conn.connect()
                should_close_conn = True

        try:
            # Extract project_id for cross-project queries (BigQuery only)
            project_id = connection_params.get('project_id') if backend == 'bigquery' else None

            # Get table metadata
            with timing_phase('metadata', phase_timings):
                table_metadata, row_count, primary_key_columns = self._get_table_metadata(
                    db_conn, table_name, schema, user_primary_key, custom_query, backend, project_id
                )

            # Get table schema and determine which columns to load (optimization)
            with timing_phase('column_selection', phase_timings):
                columns_to_load = None
                table_schema = None
                try:
                    # Get table schema (column names and types)
                    table_schema = db_conn.get_table_schema(table_name, schema, project_id)

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
                            logger.info(f"Optimized column loading: selecting {len(columns_to_load)}/{len(table_schema)} columns")
                        else:
                            logger.info(f"Loading all {len(table_schema)} columns")
                    else:
                        logger.warning("Could not get table schema, will load all columns")
                except Exception as e:
                    logger.warning(f"Column optimization failed ({e}), will load all columns")
                    columns_to_load = None

            # Determine if we should sample
            is_cross_project = backend == 'bigquery' and hasattr(db_conn, 'source_project_id') and db_conn.source_project_id

            if custom_query:
                should_sample = False
                logger.info("Custom query provided - using query as-is (no additional sampling)")
            elif is_cross_project:
                should_sample = sample_in_db and (row_count is None or row_count > self.sample_size)
            else:
                should_sample = sample_in_db and row_count and row_count > self.sample_size

            # Load data
            with timing_phase('data_loading', phase_timings):
                df = self._load_data(
                    db_conn, table_name, schema, custom_query, columns_to_load,
                    should_sample, sampling_method, sampling_key_column, project_id
                )

            # For custom queries, the row count is the result set size
            if custom_query and row_count is None:
                row_count = len(df)
                logger.info(f"Custom query returned {row_count:,} rows")
                # Update metadata with query result count
                if table_metadata:
                    table_metadata['row_count'] = row_count
                    table_metadata['query_result_count'] = row_count

            # Mask PII if requested
            if mask_pii:
                df = mask_pii_columns(df, custom_pii_keywords)

            # Run audit - pass the actual row count, metadata, primary key columns, check config, and schema
            with timing_phase('audit_checks', phase_timings):
                results = self.audit_table(
                    df,
                    table_name,
                    total_row_count=row_count,
                    primary_key_columns=primary_key_columns,
                    column_check_config=column_check_config,
                    audit_mode=audit_mode,
                    table_schema=table_schema,
                    store_dataframe=store_dataframe,
                    db_conn=db_conn,
                    schema=schema
                )

            # Add table metadata and phase timings to results
            if table_metadata:
                results['table_metadata'] = table_metadata

            # Extract conversion and insights durations, and reorder phase_timings
            # Type conversion and insights happen inside audit_checks, so we need to:
            # 1. Subtract them from audit_checks time
            # 2. Insert them as separate phases in correct order
            conversion_time = results.get('conversion_duration', 0)
            insights_time = results.get('insights_duration', 0)

            if conversion_time > 0 or insights_time > 0:
                # Adjust audit_checks to exclude type_conversion and insights
                if 'audit_checks' in phase_timings:
                    phase_timings['audit_checks'] -= (conversion_time + insights_time)

                # Rebuild dict with correct order
                ordered_timings = {}
                for key, value in phase_timings.items():
                    if key == 'audit_checks':
                        # Insert type_conversion before audit_checks
                        if conversion_time > 0:
                            ordered_timings['type_conversion'] = conversion_time
                    ordered_timings[key] = value
                    if key == 'audit_checks':
                        # Insert insights after audit_checks
                        if insights_time > 0:
                            ordered_timings['data_insights'] = insights_time

                phase_timings = ordered_timings

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

        logger.info(f"Loading file: {file_path}")

        # Read based on extension
        if file_path.suffix.lower() == '.csv':
            df = pl.read_csv(file_path)
        elif file_path.suffix.lower() in ['.parquet', '.pq']:
            df = pl.read_parquet(file_path)
        else:
            raise ValueError(f"Unsupported file type: {file_path.suffix}")

        logger.info(f"Loaded {len(df):,} rows")

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
        column_check_config: Optional['AuditConfig'] = None,
        audit_mode: Union[str, AuditMode] = 'full',
        table_schema: Optional[Dict[str, str]] = None,
        store_dataframe: bool = False,
        db_conn: Optional['DatabaseConnection'] = None,
        schema: Optional[str] = None
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

        logger.info("=" * 60)
        logger.info(f"Auditing: {table_name}")
        logger.info(f"Total rows in table: {actual_total_rows:,}")
        if total_row_count is not None and len(df) < actual_total_rows:
            logger.info(f"Analyzing sample: {len(df):,} rows")
        logger.info("=" * 60)

        # Sample if needed (only if not already sampled in DB)
        analyzed_rows = len(df)
        if len(df) > self.sample_size and total_row_count is None:
            # Only sample in-memory if we didn't already sample in DB
            df = df.sample(n=min(self.sample_size, len(df)), seed=42)
            analyzed_rows = len(df)
            logger.warning(f"Sampling {analyzed_rows:,} rows from loaded data")

        # Store original schema before type conversion
        original_schema = {col: str(df[col].dtype) for col in df.columns}

        # Attempt automatic type conversions on string columns before auditing
        conversion_start = datetime.now()
        df, conversion_log = self._attempt_type_conversions(df)
        conversion_duration = (datetime.now() - conversion_start).total_seconds()

        # Create a mapping of converted types for easy lookup
        converted_types = {conv['column']: {'from': conv['from_type'], 'to': conv['to_type']}
                          for conv in conversion_log}

        results = {
            'table_name': table_name,
            'total_rows': actual_total_rows,
            'sampled': analyzed_rows < actual_total_rows,
            'analyzed_rows': analyzed_rows,
            'columns': {},
            'column_summary': {},  # Summary for ALL columns
            'column_insights': {},  # Insights for columns
            'timestamp': start_time_utc.strftime('%Y-%m-%d %H:%M:%S UTC'),
            'start_time': start_time.isoformat(),
            'type_conversions': conversion_log  # Track conversions
        }

        # Default check config
        if check_config is None:
            check_config = {
                'trailing_characters': True,
                'leading_characters': False,
                'case_duplicates': True,
                'regex_patterns': False,  # No default - must be explicitly configured
                'numeric_strings': True,
                'timestamp_patterns': True
                # Date range checks: after, after_or_equal, before, before_or_equal (no defaults)
            }

        # Analyze each column and track check durations
        potential_keys = []
        check_durations = {}
        insights_duration = 0.0

        # Extract column descriptions from table_schema (already fetched above)
        column_descriptions = {}
        if table_schema:
            column_descriptions = {col: meta['description'] for col, meta in table_schema.items()}
            desc_count = sum(1 for v in column_descriptions.values() if v is not None)
            if desc_count > 0:
                logger.info(f"Retrieved descriptions for {desc_count}/{len(column_descriptions)} columns")

        # Iterate over all columns in schema (if provided), otherwise just loaded columns
        all_columns = list(table_schema.keys()) if table_schema else df.columns

        for col in all_columns:
            col_start = datetime.now()

            # Check if column was loaded (may be missing if optimized out)
            if col not in df.columns:
                # Column exists in schema but wasn't loaded (optimization)
                results['column_summary'][col] = {
                    'dtype': table_schema[col]['data_type'].lower() if table_schema else 'unknown',
                    'null_count': 'N/A',
                    'null_pct': 'N/A',
                    'distinct_count': 'N/A',
                    'status': 'NOT_LOADED',
                    'description': column_descriptions.get(col, None)
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
            column_summary = {
                'dtype': col_results['dtype'],
                'null_count': col_results['null_count'],
                'null_pct': col_results['null_pct'],
                'distinct_count': col_results['distinct_count'],
                'status': status,
                'description': column_descriptions.get(col, None)
            }

            # Add source type if this column was converted
            if col in converted_types:
                column_summary['source_dtype'] = original_schema.get(col, 'unknown')

            results['column_summary'][col] = column_summary

            # Debug log for first column with description
            if column_descriptions.get(col) and not hasattr(self, '_logged_description'):
                logger.debug(f"Sample: Column '{col}' has description: '{column_descriptions.get(col)[:50]}...'")
                self._logged_description = True

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
                    insights_start = datetime.now()
                    insights = generate_column_insights(df, col, col_insights_config)
                    insights_duration += (datetime.now() - insights_start).total_seconds()
                    if insights:
                        # Serialize InsightResult objects to dicts for JSON export
                        results['column_insights'][col] = [insight.model_dump() for insight in insights]

        # Store check durations
        results['check_durations'] = check_durations

        # Store potential primary key columns
        if potential_keys:
            results['potential_primary_keys'] = potential_keys
            logger.info(f"Potential primary key column(s): {', '.join(potential_keys)}")

        # Calculate duration
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        results['end_time'] = end_time.isoformat()
        results['duration_seconds'] = round(duration, 2)
        results['conversion_duration'] = conversion_duration
        results['insights_duration'] = insights_duration

        print_results(results)

        # Log duration breakdown
        logger.info("Audit Duration Breakdown:")
        if 'check_durations' in results and results['check_durations']:
            for check_type, check_duration in results['check_durations'].items():
                logger.info(f"  • {check_type}: {check_duration:.3f}s")
        logger.info(f"  • Total: {duration:.2f}s")

        # Store DataFrame if requested (for relationship detection)
        if store_dataframe:
            results['data'] = df

        return results

    def _attempt_type_conversions(self, df: pl.DataFrame, conversion_threshold: float = 0.95) -> Tuple[pl.DataFrame, List[Dict]]:
        """
        Attempt to convert string columns to more specific types (date, datetime, numeric)

        Args:
            df: Polars DataFrame
            conversion_threshold: Minimum proportion of successful conversions to apply type change (default: 0.95)

        Returns:
            Tuple of (modified DataFrame, list of conversion info dicts)
        """
        conversion_log = []

        # Identify string columns
        string_columns = [col for col in df.columns if df[col].dtype in [pl.Utf8, pl.String]]

        if not string_columns:
            return df, conversion_log

        logger.info(f"Attempting type conversions on {len(string_columns)} string column(s)...")

        for col in string_columns:
            # Get non-null values for conversion testing
            non_null_values = df[col].drop_nulls()
            if len(non_null_values) == 0:
                continue

            total_non_null = len(non_null_values)

            # Try conversions in order: int → float → datetime → date
            # If a conversion is successful, skip remaining types

            # 1. Try INTEGER conversion
            try:
                # Attempt to cast to integer
                converted = df[col].cast(pl.Int64, strict=False)
                successful_conversions = converted.drop_nulls().len()
                success_rate = successful_conversions / total_non_null if total_non_null > 0 else 0

                if success_rate >= conversion_threshold:
                    df = df.with_columns(converted.alias(col))
                    conversion_log.append({
                        'column': col,
                        'from_type': 'string',
                        'to_type': 'int64',
                        'success_rate': success_rate,
                        'converted_values': successful_conversions
                    })
                    logger.info(f"  ✓ {col}: string → int64 ({success_rate:.1%} success)")
                    continue
            except Exception:
                pass  # Integer conversion failed, try next type

            # 2. Try FLOAT conversion
            try:
                # Attempt to cast to float
                converted = df[col].cast(pl.Float64, strict=False)
                successful_conversions = converted.drop_nulls().len()
                success_rate = successful_conversions / total_non_null if total_non_null > 0 else 0

                if success_rate >= conversion_threshold:
                    df = df.with_columns(converted.alias(col))
                    conversion_log.append({
                        'column': col,
                        'from_type': 'string',
                        'to_type': 'float64',
                        'success_rate': success_rate,
                        'converted_values': successful_conversions
                    })
                    logger.info(f"  ✓ {col}: string → float64 ({success_rate:.1%} success)")
                    continue
            except Exception:
                pass  # Float conversion failed, try next type

            # 3. Try DATETIME conversion
            try:
                # Attempt to parse as datetime
                converted = df[col].str.to_datetime(strict=False)
                successful_conversions = converted.drop_nulls().len()
                success_rate = successful_conversions / total_non_null if total_non_null > 0 else 0

                if success_rate >= conversion_threshold:
                    df = df.with_columns(converted.alias(col))
                    conversion_log.append({
                        'column': col,
                        'from_type': 'string',
                        'to_type': 'datetime',
                        'success_rate': success_rate,
                        'converted_values': successful_conversions
                    })
                    logger.info(f"  ✓ {col}: string → datetime ({success_rate:.1%} success)")
                    continue
            except Exception:
                pass  # Datetime conversion failed, try next type

            # 4. Try DATE conversion
            try:
                # Attempt to parse as date
                converted = df[col].str.to_date(strict=False)
                successful_conversions = converted.drop_nulls().len()
                success_rate = successful_conversions / total_non_null if total_non_null > 0 else 0

                if success_rate >= conversion_threshold:
                    df = df.with_columns(converted.alias(col))
                    conversion_log.append({
                        'column': col,
                        'from_type': 'string',
                        'to_type': 'date',
                        'success_rate': success_rate,
                        'converted_values': successful_conversions
                    })
                    logger.info(f"  ✓ {col}: string → date ({success_rate:.1%} success)")
                    continue
            except Exception:
                pass  # Date conversion failed, keep as string

        if conversion_log:
            logger.info(f"Successfully converted {len(conversion_log)} column(s) to more specific types")
        else:
            logger.info(f"No string columns could be converted (threshold: {conversion_threshold:.0%})")

        return df, conversion_log

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
            'dtype': str(dtype).lower(),
            'null_count': null_count,
            'null_pct': (null_count / total_rows * 100) if total_rows > 0 else 0,
            'distinct_count': None,  # Not applicable for complex types
            'issues': [],  # No checks for complex types
            'status': 'SKIPPED_COMPLEX_TYPE'
        }

    def _audit_column(
        self,
        df: pl.DataFrame,
        col: str,
        check_config: Dict,
        primary_key_columns: Optional[List[str]] = None,
        audit_mode: Union[str, AuditMode] = 'full'
    ) -> Dict:
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
            'dtype': str(dtype).lower(),
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

        # Get applicable checks for this column's dtype
        applicable_checks = self._get_applicable_checks(dtype, check_config)

        # Run all applicable checks
        for check_name, check_params in applicable_checks:
            self._run_check_and_track(
                check_name=check_name,
                df=df,
                col=col,
                primary_key_columns=primary_key_columns or [],
                col_result=col_result,
                **check_params
            )

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
