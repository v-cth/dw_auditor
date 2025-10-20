"""
Main auditor class that coordinates all auditing functionality
"""

import polars as pl
from typing import Dict, List, Optional, Union
from datetime import datetime
from pathlib import Path

from ..checks.string_checks import (
    check_trailing_spaces,
    check_case_duplicates,
    check_special_chars,
    check_numeric_strings
)
from ..checks.timestamp_checks import check_timestamp_patterns, check_date_outliers
from ..utils.security import mask_pii_columns, sanitize_connection_string
from ..utils.output import print_results, get_summary_stats
from ..exporters.dataframe_export import export_to_dataframe
from ..exporters.json_export import export_to_json
from ..exporters.html_export import export_to_html
from ..exporters.summary_export import export_column_summary_to_dataframe
from .database import DatabaseConnection
from ..insights import generate_column_insights


class SecureTableAuditor:
    """Audit data warehouse tables with security controls"""

    def __init__(
        self,
        sample_size: int = 100000,
        sample_threshold: int = 1000000,
        min_year: int = 1950,
        max_year: int = 2100,
        outlier_threshold_pct: float = 0.01
    ):
        """
        Args:
            sample_size: Number of rows to sample if table exceeds threshold
            sample_threshold: Row count threshold for sampling
            min_year: Minimum reasonable year for date outlier detection (default: 1950)
            max_year: Maximum reasonable year for date outlier detection (default: 2100)
            outlier_threshold_pct: Minimum percentage to report outliers (default: 0.01 = 1%)
        """
        self.sample_size = sample_size
        self.sample_threshold = sample_threshold
        self.min_year = min_year
        self.max_year = max_year
        self.outlier_threshold_pct = outlier_threshold_pct
        self.audit_log = []

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
        column_check_config: Optional[any] = None
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
                    'dataset_id': 'my_dataset',
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

        Returns:
            Dictionary with audit results
        """
        # Log the audit
        self._log_audit(table_name, f"{backend}://{connection_params.get('project_id') or connection_params.get('account')}")

        print(f"🔐 Secure audit mode: Direct database query via Ibis (no file export)")

        # Track timing for different phases
        phase_timings = {}
        phase_start = datetime.now()

        # Create database connection
        db_conn = DatabaseConnection(backend, **connection_params)
        db_conn.connect()
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
            if row_count is None and sample_in_db:
                try:
                    row_count = db_conn.get_row_count(table_name, schema)
                    if row_count is not None:
                        print(f"📊 Table has {row_count:,} rows")
                    else:
                        print(f"⚠️  Could not determine row count, will load full table")
                except Exception as e:
                    print(f"⚠️  Could not get row count: {e}")
                    print(f"   Will load full table")

            phase_timings['metadata'] = (datetime.now() - phase_start).total_seconds()

            # Determine if we should sample
            should_sample = sample_in_db and row_count and row_count > self.sample_threshold

            if should_sample:
                print(f"🔍 Sampling {self.sample_size:,} rows from table")

            # Execute query
            phase_start = datetime.now()
            df = db_conn.execute_query(
                table_name=table_name,
                schema=schema,
                custom_query=custom_query,
                sample_size=self.sample_size if should_sample else None
            )
            phase_timings['data_loading'] = (datetime.now() - phase_start).total_seconds()

            print(f"✅ Loaded {len(df):,} rows into memory")

            # Mask PII if requested
            if mask_pii:
                df = mask_pii_columns(df, custom_pii_keywords)

            # Run audit - pass the actual row count, metadata, primary key columns, and check config
            phase_start = datetime.now()
            results = self.audit_table(
                df,
                table_name,
                total_row_count=row_count,
                primary_key_columns=primary_key_columns,
                column_check_config=column_check_config
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
            # Always close the connection
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
        column_check_config: Optional[any] = None
    ) -> Dict:
        """
        Main audit function - runs all checks on a Polars DataFrame

        Args:
            df: Polars DataFrame to audit
            table_name: Name of the table for reporting
            check_config: Optional configuration for which checks to run
            total_row_count: Optional total row count from database (if known)

        Returns:
            Dictionary with audit results
        """
        # Start timing
        start_time = datetime.now()

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
        if len(df) > self.sample_threshold and total_row_count is None:
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
            'timestamp': start_time.isoformat(),
            'start_time': start_time.isoformat()
        }

        # Default check config
        if check_config is None:
            check_config = {
                'trailing_spaces': True,
                'case_duplicates': True,
                'special_chars': True,
                'numeric_strings': True,
                'timestamp_patterns': True,
                'date_outliers': True
            }

        # Analyze each column and track check durations
        potential_keys = []
        check_durations = {}

        for col in df.columns:
            col_start = datetime.now()

            # Get column-specific check configuration
            col_dtype = str(df[col].dtype)
            if column_check_config and hasattr(column_check_config, 'get_column_checks'):
                # Use column-specific config from the matrix
                col_check_config = column_check_config.get_column_checks(table_name, col, col_dtype)
            else:
                # Fallback to global check_config
                col_check_config = check_config or {}

            col_results = self._audit_column(df, col, col_check_config, primary_key_columns)
            col_duration = (datetime.now() - col_start).total_seconds()

            # Determine status based on column type and issues found
            dtype = df[col].dtype
            if dtype in [pl.Utf8, pl.String, pl.Datetime, pl.Date]:
                # Columns that are checked for quality issues
                status = 'ERROR' if col_results['issues'] else 'OK'
                check_type = 'string_checks' if dtype in [pl.Utf8, pl.String] else 'date_checks'
                check_durations[check_type] = check_durations.get(check_type, 0) + col_duration
            else:
                # Columns not checked (numeric, boolean, etc.)
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

            # Store detailed results only for columns with issues
            if col_results['issues']:
                results['columns'][col] = col_results

            # Generate column insights
            if column_check_config and hasattr(column_check_config, 'get_column_insights'):
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

        return results

    def _audit_column(self, df: pl.DataFrame, col: str, check_config: Dict, primary_key_columns: Optional[List[str]] = None) -> Dict:
        """Audit a single column for all issues"""
        dtype = df[col].dtype
        null_count = df[col].null_count()
        total_rows = len(df)

        # Calculate distinct value count (excluding nulls)
        distinct_count = df[col].n_unique()

        col_result = {
            'dtype': str(dtype),
            'null_count': null_count,
            'null_pct': (null_count / total_rows * 100) if total_rows > 0 else 0,
            'distinct_count': distinct_count,
            'issues': []
        }

        # Skip if all nulls or masked
        if null_count == total_rows:
            return col_result

        # Skip masked columns
        first_non_null = df[col].drop_nulls().head(1)
        if len(first_non_null) > 0 and first_non_null[0] == "***PII_MASKED***":
            return col_result

        # String column checks
        if dtype in [pl.Utf8, pl.String]:
            if check_config.get('trailing_spaces', True):
                col_result['issues'].extend(check_trailing_spaces(df, col, primary_key_columns))
            if check_config.get('case_duplicates', True):
                col_result['issues'].extend(check_case_duplicates(df, col, primary_key_columns))
            if check_config.get('special_chars', True):
                col_result['issues'].extend(check_special_chars(df, col, primary_key_columns))
            if check_config.get('numeric_strings', True):
                col_result['issues'].extend(check_numeric_strings(df, col, primary_key_columns))

        # Timestamp/Date checks
        elif dtype in [pl.Datetime, pl.Date]:
            if check_config.get('timestamp_patterns', True):
                col_result['issues'].extend(check_timestamp_patterns(df, col))
            if check_config.get('date_outliers', True):
                col_result['issues'].extend(check_date_outliers(
                    df, col,
                    min_year=getattr(self, 'min_year', 1950),
                    max_year=getattr(self, 'max_year', 2100),
                    outlier_threshold_pct=getattr(self, 'outlier_threshold_pct', 0.01)
                ))

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

    def export_results_to_dataframe(self, results: Dict) -> pl.DataFrame:
        """
        Export audit results to a Polars DataFrame for easy analysis

        Returns:
            DataFrame with one row per issue found
        """
        return export_to_dataframe(results)

    def export_results_to_json(self, results: Dict, file_path: Optional[str] = None) -> str:
        """
        Export audit results to JSON

        Args:
            results: Audit results dictionary
            file_path: Optional path to save JSON file

        Returns:
            JSON string
        """
        return export_to_json(results, file_path)

    def export_results_to_html(self, results: Dict, file_path: str = "audit_report.html") -> str:
        """
        Export audit results to a beautiful HTML report

        Args:
            results: Audit results dictionary
            file_path: Path to save HTML file

        Returns:
            Path to saved HTML file
        """
        return export_to_html(results, file_path)

    def get_summary_stats(self, results: Dict) -> Dict:
        """
        Get high-level summary statistics from audit results

        Returns:
            Dictionary with summary statistics
        """
        return get_summary_stats(results)

    def export_column_summary_to_dataframe(self, results: Dict) -> pl.DataFrame:
        """
        Export column summary to a Polars DataFrame

        Args:
            results: Audit results dictionary

        Returns:
            DataFrame with one row per column with basic metrics (null count, null %, distinct count)
        """
        return export_column_summary_to_dataframe(results)
