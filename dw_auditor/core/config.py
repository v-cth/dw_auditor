"""
Configuration management for the data warehouse auditor
"""

from typing import Dict, List, Union
from pathlib import Path
import yaml


class AuditConfig:
    """Configuration class for audit parameters"""

    def __init__(self, config_dict: Dict):
        """Initialize from dictionary (parsed from YAML)"""
        # Database connection
        db_config = config_dict.get('database', {})
        self.backend = db_config.get('backend')  # 'bigquery' or 'snowflake'
        self.connection_params = db_config.get('connection_params', {})
        self.schema = db_config.get('schema')

        # Tables to audit - normalize format
        # Support both simple list and dict format with primary keys
        tables_raw = config_dict.get('tables', [])
        self.tables = []
        self.table_primary_keys = {}  # Map of table_name -> primary_key_column(s)

        for table_entry in tables_raw:
            if isinstance(table_entry, str):
                # Simple string format
                self.tables.append(table_entry)
            elif isinstance(table_entry, dict):
                # Dictionary format with optional primary_key
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

        # Sampling configuration
        sampling = config_dict.get('sampling', {})
        self.sample_size = sampling.get('sample_size', 100000)
        self.sample_threshold = sampling.get('sample_threshold', 1000000)
        self.sample_in_db = sampling.get('sample_in_db', True)

        # Security settings
        security = config_dict.get('security', {})
        self.mask_pii = security.get('mask_pii', True)
        self.custom_pii_keywords = security.get('custom_pii_keywords', [])

        # Audit checks configuration
        checks = config_dict.get('checks', {})
        self.check_trailing_spaces = checks.get('trailing_spaces', True)
        self.check_case_duplicates = checks.get('case_duplicates', True)
        self.check_special_chars = checks.get('special_characters', True)
        self.check_numeric_strings = checks.get('numeric_strings', True)
        self.check_timestamp_patterns = checks.get('timestamp_patterns', True)
        self.check_date_outliers = checks.get('date_outliers', True)

        # Special characters configuration
        self.special_chars_pattern = checks.get('special_chars_pattern', r'[^a-zA-Z0-9\s\.,\-_@]')

        # Thresholds
        thresholds = config_dict.get('thresholds', {})
        self.numeric_string_threshold = thresholds.get('numeric_string_pct', 80) / 100
        self.constant_hour_threshold = thresholds.get('constant_hour_pct', 90) / 100
        self.midnight_threshold = thresholds.get('midnight_pct', 95) / 100

        # Date outlier thresholds
        self.min_year = thresholds.get('min_year', 1950)
        self.max_year = thresholds.get('max_year', 2100)
        self.outlier_threshold_pct = thresholds.get('outlier_threshold_pct', 0.01)

        # Output configuration
        output = config_dict.get('output', {})
        self.output_dir = Path(output.get('directory', 'audit_results'))
        self.export_formats = output.get('formats', ['html', 'csv'])
        self.file_prefix = output.get('file_prefix', 'audit')

        # Column filters (optional)
        filters = config_dict.get('filters', {})
        self.include_columns = filters.get('include_columns', [])
        self.exclude_columns = filters.get('exclude_columns', [])

        # Column-level check configuration matrix
        column_checks_config = config_dict.get('column_checks', {})

        # Global defaults per data type
        self.column_check_defaults = column_checks_config.get('defaults', {
            'string': {
                'trailing_spaces': True,
                'case_duplicates': True,
                'special_chars': True,
                'numeric_strings': True
            },
            'datetime': {
                'timestamp_patterns': True,
                'date_outliers': True
            }
        })

        # Per-table, per-column overrides
        self.column_check_overrides = column_checks_config.get('tables', {})

        # Column-level insights configuration
        column_insights_config = config_dict.get('column_insights', {})

        # Global defaults per data type for insights
        self.column_insights_defaults = column_insights_config.get('defaults', {
            'string': {
                'top_values': 10,
                'min_length': True,
                'max_length': True,
                'avg_length': True
            },
            'numeric': {
                'min': True,
                'max': True,
                'mean': True,
                'median': True,
                'std': False,
                'quantiles': [0.25, 0.5, 0.75],
                'top_values': 5
            },
            'datetime': {
                'min_date': True,
                'max_date': True,
                'date_range_days': True,
                'most_common_dates': 5
            }
        })

        # Per-table, per-column insights overrides
        self.column_insights_overrides = column_insights_config.get('tables', {})

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
                'trailing_spaces': self.check_trailing_spaces,
                'case_duplicates': self.check_case_duplicates,
                'special_characters': self.check_special_chars,
                'numeric_strings': self.check_numeric_strings,
                'timestamp_patterns': self.check_timestamp_patterns,
                'date_outliers': self.check_date_outliers,
                'special_chars_pattern': self.special_chars_pattern
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
