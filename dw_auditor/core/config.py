"""
Configuration management for the data warehouse auditor
"""

from typing import Dict, List, Union
from pathlib import Path
import yaml
import fnmatch


class AuditConfig:
    """Configuration class for audit parameters"""

    def __init__(self, config_dict: Dict):
        """Initialize from dictionary (parsed from YAML)"""
        # Audit metadata (optional descriptive information)
        self.audit_version = config_dict.get('version')
        self.audit_project = config_dict.get('project')
        self.audit_description = config_dict.get('description')
        self.audit_last_modified = config_dict.get('last_modified')

        # Database connection
        db_config = config_dict.get('database') or {}
        self.backend = db_config.get('backend')  # 'bigquery' or 'snowflake'
        self.connection_params = db_config.get('connection_params', {})
        self.schema = db_config.get('schema')

        # Tables to audit - normalize format
        # Support both simple list and dict format with primary keys, custom queries, and schemas
        tables_raw = config_dict.get('tables') or []
        self.tables = []
        self.table_primary_keys = {}  # Map of table_name -> primary_key_column(s)
        self.table_queries = {}  # Map of table_name -> custom_query
        self.table_schemas = {}  # Map of table_name -> schema/dataset

        for table_entry in tables_raw:
            if isinstance(table_entry, str):
                # Simple string format
                self.tables.append(table_entry)
            elif isinstance(table_entry, dict):
                # Dictionary format with optional primary_key, query, and schema
                table_name = table_entry.get('name')
                if table_name:
                    self.tables.append(table_name)
                    primary_key = table_entry.get('primary_key')
                    if primary_key:
                        # Support both single string and list of strings
                        if isinstance(primary_key, str):
                            self.table_primary_keys[table_name] = [primary_key]
                        elif isinstance(primary_key, list):
                            self.table_primary_keys[table_name] = primary_key
                    # Store custom query if provided
                    custom_query = table_entry.get('query')
                    if custom_query:
                        self.table_queries[table_name] = custom_query
                    # Store table-specific schema if provided
                    table_schema = table_entry.get('schema')
                    if table_schema:
                        self.table_schemas[table_name] = table_schema

        # Table filtering configuration
        table_filters = config_dict.get('table_filters') or {}
        self.auto_discover = table_filters.get('auto_discover', False)
        self.exclude_patterns = table_filters.get('exclude_patterns', [])
        self.include_patterns = table_filters.get('include_patterns', [])

        # Sampling configuration
        sampling = config_dict.get('sampling') or {}
        self.sample_size = sampling.get('sample_size', 100000)
        self.sample_threshold = sampling.get('sample_threshold', 1000000)
        self.sample_in_db = sampling.get('sample_in_db', True)

        # Sampling strategy configuration
        self.sampling_method = sampling.get('method', 'random')  # random, recent, top, systematic
        self.sampling_key_column = sampling.get('key_column')  # Column to use for non-random sampling

        # Per-table sampling overrides
        self.table_sampling_config = sampling.get('tables', {})

        # Security settings
        security = config_dict.get('security') or {}
        self.mask_pii = security.get('mask_pii', True)
        self.custom_pii_keywords = security.get('custom_pii_keywords', [])

        # Audit checks configuration
        checks = config_dict.get('checks') or {}
        self.check_trailing_characters = checks.get('trailing_characters', True)
        self.check_ending_characters = checks.get('ending_characters', False)
        self.check_case_duplicates = checks.get('case_duplicates', True)
        self.check_regex_patterns = checks.get('regex_patterns', checks.get('special_characters', False))
        self.check_numeric_strings = checks.get('numeric_strings', True)
        self.check_timestamp_patterns = checks.get('timestamp_patterns', True)
        self.check_date_outliers = checks.get('date_outliers', True)

        # Thresholds
        thresholds = config_dict.get('thresholds') or {}
        self.numeric_string_threshold = thresholds.get('numeric_string_pct', 80) / 100
        self.constant_hour_threshold = thresholds.get('constant_hour_pct', 90) / 100
        self.midnight_threshold = thresholds.get('midnight_pct', 95) / 100

        # Date outlier thresholds
        self.min_year = thresholds.get('min_year', 1950)
        self.max_year = thresholds.get('max_year', 2100)
        self.outlier_threshold_pct = thresholds.get('outlier_threshold_pct', 0.0)

        # Output configuration
        output = config_dict.get('output') or {}
        self.output_dir = Path(output.get('directory', 'audit_results'))
        self.export_formats = output.get('formats', ['html', 'csv'])
        self.file_prefix = output.get('file_prefix', 'audit')
        self.auto_open_html = output.get('auto_open_html', False)

        # Number formatting for HTML reports
        number_format = output.get('number_format') or {}
        self.thousand_separator = number_format.get('thousand_separator', ',')
        self.decimal_places = number_format.get('decimal_places', 1)

        # Column filters (optional)
        filters = config_dict.get('filters') or {}
        self.include_columns = filters.get('include_columns', [])
        self.exclude_columns = filters.get('exclude_columns', [])

        # Column-level check configuration matrix
        column_checks_config = config_dict.get('column_checks')

        # Track if checks are explicitly enabled in config
        self.checks_enabled = column_checks_config is not None

        if self.checks_enabled:
            # Global defaults per data type (only from YAML)
            self.column_check_defaults = column_checks_config.get('defaults', {})
            # Per-table, per-column overrides
            self.column_check_overrides = column_checks_config.get('tables', {})
        else:
            # No checks config - disable checks
            self.column_check_defaults = {}
            self.column_check_overrides = {}

        # Column-level insights configuration
        column_insights_config = config_dict.get('column_insights')

        # Track if insights are explicitly enabled in config
        self.insights_enabled = column_insights_config is not None

        if self.insights_enabled:
            # Global defaults per data type for insights (only from YAML)
            self.column_insights_defaults = column_insights_config.get('defaults', {})
            # Per-table, per-column insights overrides
            self.column_insights_overrides = column_insights_config.get('tables', {})
        else:
            # No insights config - disable insights
            self.column_insights_defaults = {}
            self.column_insights_overrides = {}

        # Relationship detection configuration
        relationship_detection = config_dict.get('relationship_detection') or {}
        self.relationship_detection_enabled = relationship_detection.get('enabled', False)
        self.relationship_confidence_threshold = relationship_detection.get('confidence_threshold', 0.7)
        self.relationship_min_display_confidence = relationship_detection.get('min_confidence_display', 0.5)

    def get_column_checks(self, table_name: str, column_name: str, column_dtype: str) -> Dict:
        """
        Get check configuration for a specific column

        Args:
            table_name: Name of the table
            column_name: Name of the column
            column_dtype: Data type of the column (string, datetime, numeric, etc.)

        Returns:
            Dictionary with check configuration for this column
        """
        # Start with global defaults for this data type
        dtype_key = column_dtype.lower()
        if 'string' in dtype_key or 'utf8' in dtype_key:
            base_config = self.column_check_defaults.get('string', {}).copy()
        elif 'datetime' in dtype_key or 'date' in dtype_key:
            base_config = self.column_check_defaults.get('datetime', {}).copy()
        else:
            base_config = {}

        # Apply table-level overrides
        if table_name in self.column_check_overrides:
            table_config = self.column_check_overrides[table_name]
            if column_name in table_config:
                column_overrides = table_config[column_name]
                base_config.update(column_overrides)

        return base_config

    def get_column_insights(self, table_name: str, column_name: str, column_dtype: str) -> Dict:
        """
        Get insights configuration for a specific column

        Args:
            table_name: Name of the table
            column_name: Name of the column
            column_dtype: Data type of the column (string, datetime, numeric, etc.)

        Returns:
            Dictionary with insights configuration for this column
        """
        # Start with global defaults for this data type
        dtype_key = column_dtype.lower()
        if 'string' in dtype_key or 'utf8' in dtype_key:
            base_config = self.column_insights_defaults.get('string', {}).copy()
        elif 'datetime' in dtype_key or 'date' in dtype_key:
            base_config = self.column_insights_defaults.get('datetime', {}).copy()
        elif 'int' in dtype_key or 'float' in dtype_key:
            base_config = self.column_insights_defaults.get('numeric', {}).copy()
        else:
            base_config = {}

        # Apply table-level overrides
        if table_name in self.column_insights_overrides:
            table_config = self.column_insights_overrides[table_name]
            if column_name in table_config:
                column_overrides = table_config[column_name]
                base_config.update(column_overrides)

        return base_config

    def get_table_sampling_config(self, table_name: str) -> Dict:
        """
        Get sampling configuration for a specific table

        Args:
            table_name: Name of the table

        Returns:
            Dictionary with sampling configuration (method, key_column)
        """
        # Start with global defaults
        sampling_config = {
            'method': self.sampling_method,
            'key_column': self.sampling_key_column
        }

        # Apply table-specific overrides
        if table_name in self.table_sampling_config:
            table_config = self.table_sampling_config[table_name]
            if 'method' in table_config:
                sampling_config['method'] = table_config['method']
            if 'key_column' in table_config:
                sampling_config['key_column'] = table_config['key_column']

        return sampling_config

    def get_table_schema(self, table_name: str) -> str:
        """
        Get schema/dataset for a specific table

        Args:
            table_name: Name of the table

        Returns:
            Schema/dataset name (falls back to global schema if not specified)
        """
        # Return table-specific schema if defined, otherwise use global schema
        return self.table_schemas.get(table_name, self.schema)

    def should_include_table(self, table_name: str) -> bool:
        """
        Determine if a table should be included based on filter patterns

        Args:
            table_name: Name of the table

        Returns:
            True if table should be included, False otherwise
        """
        # Step 1: Check exclude patterns (blacklist)
        for pattern in self.exclude_patterns:
            if fnmatch.fnmatch(table_name.lower(), pattern.lower()):
                return False

        # Step 2: Check include patterns (whitelist) - only if patterns are specified
        if self.include_patterns:
            # If include patterns are specified, table must match at least one
            for pattern in self.include_patterns:
                if fnmatch.fnmatch(table_name.lower(), pattern.lower()):
                    return True
            # Didn't match any include pattern
            return False

        # No include patterns specified, and didn't match exclude patterns
        return True

    @classmethod
    def from_yaml(cls, yaml_path: Union[str, Path]) -> 'AuditConfig':
        """Load configuration from YAML file"""
        with open(yaml_path, 'r') as f:
            config_dict = yaml.safe_load(f)
        return cls(config_dict)

    def to_dict(self) -> Dict:
        """Convert config back to dictionary"""
        return {
            'database': {
                'backend': self.backend,
                'connection_params': self.connection_params,
                'schema': self.schema
            },
            'tables': self.tables,
            'sampling': {
                'sample_size': self.sample_size,
                'sample_threshold': self.sample_threshold,
                'sample_in_db': self.sample_in_db
            },
            'security': {
                'mask_pii': self.mask_pii,
                'custom_pii_keywords': self.custom_pii_keywords
            },
            'checks': {
                'trailing_characters': self.check_trailing_characters,
                'ending_characters': self.check_ending_characters,
                'case_duplicates': self.check_case_duplicates,
                'regex_patterns': self.check_regex_patterns,
                'numeric_strings': self.check_numeric_strings,
                'timestamp_patterns': self.check_timestamp_patterns,
                'date_outliers': self.check_date_outliers
            },
            'thresholds': {
                'numeric_string_pct': self.numeric_string_threshold * 100,
                'constant_hour_pct': self.constant_hour_threshold * 100,
                'midnight_pct': self.midnight_threshold * 100,
                'min_year': self.min_year,
                'max_year': self.max_year,
                'outlier_threshold_pct': self.outlier_threshold_pct
            },
            'output': {
                'directory': str(self.output_dir),
                'formats': self.export_formats,
                'file_prefix': self.file_prefix
            },
            'filters': {
                'include_columns': self.include_columns,
                'exclude_columns': self.exclude_columns
            }
        }
