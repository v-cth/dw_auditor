"""
Configuration management for the data warehouse auditor with Pydantic validation
"""

from typing import Dict, List, Union, Optional, Any, Literal
from pathlib import Path
import yaml
import fnmatch
import os
import re
from pydantic import BaseModel, Field, field_validator, model_validator, ConfigDict


# ============================================================================
# Pydantic Models for Configuration Validation
# ============================================================================

class TableConfig(BaseModel):
    """Configuration for a single table"""
    model_config = ConfigDict(protected_namespaces=(), populate_by_name=True, extra='allow')

    name: str = Field(..., min_length=1, description="Table name")
    primary_key: Optional[Union[str, List[str]]] = Field(None, description="Primary key column(s)")
    query: Optional[str] = Field(None, description="Custom SQL query")

    # Unified connection parameter overrides (works for BigQuery, Snowflake, and Databricks)
    database: Optional[str] = Field(None, description="Override database (project_id for BigQuery, database for Snowflake, catalog for Databricks)")
    table_schema: Optional[str] = Field(None, alias="schema", serialization_alias="schema", description="Override schema (dataset for BigQuery, schema for Snowflake/Databricks)")

    # Per-table column filtering
    include_columns: Optional[List[str]] = Field(None, description="Columns to include (overrides global filters)")
    exclude_columns: Optional[List[str]] = Field(None, description="Columns to exclude (overrides global filters)")

    @field_validator('primary_key')
    @classmethod
    def normalize_primary_key(cls, v):
        """Convert single string to list"""
        if isinstance(v, str):
            return [v]
        return v


class ConnectionParams(BaseModel):
    """Database connection parameters - flexible to support different backends"""
    model_config = ConfigDict(extra='allow', protected_namespaces=(), populate_by_name=True)


class DatabaseConfig(BaseModel):
    """Database connection configuration"""
    backend: Literal['bigquery', 'snowflake', 'databricks'] = Field(..., description="Database backend")
    connection_params: ConnectionParams = Field(..., description="Connection parameters")

    @model_validator(mode='after')
    def validate_backend_params(self):
        """Validate backend-specific required parameters"""
        params = self.connection_params.model_dump(exclude_none=True)

        # Validate required unified params
        if 'default_database' not in params:
            raise ValueError("connection_params requires 'default_database' (project_id for BigQuery, database for Snowflake, catalog for Databricks)")
        if 'default_schema' not in params:
            raise ValueError("connection_params requires 'default_schema' (dataset for BigQuery, schema for Snowflake/Databricks)")

        if self.backend == 'bigquery':
            # BigQuery-specific: only need default_database and default_schema
            # Optional: credentials_path, credentials_json
            pass

        elif self.backend == 'snowflake':
            # Snowflake-specific: requires account, user
            required = ['account', 'user']
            missing = [p for p in required if p not in params]
            if missing:
                raise ValueError(f"Snowflake backend requires: {', '.join(missing)}")

            # Password is required unless using external browser authentication
            if 'password' not in params and params.get('authenticator') != 'externalbrowser':
                raise ValueError("Snowflake backend requires 'password' (or set authenticator='externalbrowser' for SSO)")

        elif self.backend == 'databricks':
            # Databricks-specific: requires server_hostname and http_path
            required = ['server_hostname', 'http_path']
            missing = [p for p in required if p not in params]
            if missing:
                raise ValueError(f"Databricks backend requires: {', '.join(missing)}")

            # Authentication: requires either auth_type (OAuth/AAD) or access_token
            has_auth = (
                'auth_type' in params or
                'access_token' in params or
                ('username' in params and 'password' in params)
            )
            if not has_auth:
                raise ValueError(
                    "Databricks backend requires authentication: "
                    "'auth_type' for OAuth/AAD, 'access_token' for token-based auth, "
                    "or 'username' and 'password' for basic auth"
                )

        return self


class TableFilters(BaseModel):
    """Table filtering configuration"""
    auto_discover: bool = Field(False, description="Auto-discover tables in schema")
    exclude_patterns: List[str] = Field(default_factory=list, description="Patterns to exclude")
    include_patterns: List[str] = Field(default_factory=list, description="Patterns to include")


class TableSamplingConfig(BaseModel):
    """Per-table sampling configuration"""
    method: Optional[Literal['random', 'recent', 'top', 'systematic']] = None
    key_column: Optional[str] = None


class SamplingConfig(BaseModel):
    """Sampling configuration"""
    sample_size: int = Field(100000, gt=0, description="Number of rows to sample")
    method: Literal['random', 'recent', 'top', 'systematic'] = Field('random', description="Sampling method")
    key_column: Optional[str] = Field(None, description="Column for non-random sampling")
    tables: Dict[str, TableSamplingConfig] = Field(default_factory=dict, description="Per-table overrides")


class SecurityConfig(BaseModel):
    """Security settings"""
    mask_pii: bool = Field(True, description="Mask PII data")
    custom_pii_keywords: List[str] = Field(default_factory=list, description="Custom PII keywords")


class NumberFormat(BaseModel):
    """Number formatting for reports"""
    thousand_separator: str = Field(',', max_length=1, description="Thousand separator character")
    decimal_places: int = Field(1, ge=0, le=10, description="Decimal places for percentages")


class OutputConfig(BaseModel):
    """Output configuration"""
    directory: str = Field('audit_results', description="Output directory path")
    formats: List[Literal['html', 'csv', 'json', 'parquet']] = Field(
        default_factory=lambda: ['html', 'csv'],
        min_length=1,
        description="Export formats"
    )
    file_prefix: str = Field('audit', min_length=1, description="File prefix for outputs")
    auto_open_html: bool = Field(False, description="Auto-open HTML report in browser")
    number_format: NumberFormat = Field(default_factory=NumberFormat, description="Number formatting")


class FiltersConfig(BaseModel):
    """Column filtering configuration"""
    include_columns: List[str] = Field(default_factory=list, description="Columns to include")
    exclude_columns: List[str] = Field(default_factory=list, description="Columns to exclude")


class ColumnChecksConfig(BaseModel):
    """Column-level check configuration"""
    defaults: Dict[str, Any] = Field(default_factory=dict, description="Default checks per data type")
    tables: Dict[str, Dict[str, Any]] = Field(default_factory=dict, description="Per-table/column overrides")


class ColumnInsightsConfig(BaseModel):
    """Column-level insights configuration"""
    defaults: Dict[str, Any] = Field(default_factory=dict, description="Default insights per data type")
    tables: Dict[str, Dict[str, Any]] = Field(default_factory=dict, description="Per-table/column overrides")


class RelationshipDetectionConfig(BaseModel):
    """Relationship detection configuration"""
    enabled: bool = Field(False, description="Enable relationship detection")
    confidence_threshold: float = Field(0.7, ge=0.0, le=1.0, description="Minimum confidence to report")
    min_confidence_display: float = Field(0.5, ge=0.0, le=1.0, description="Minimum confidence to display")
    exclude_tables: List[str] = Field(default_factory=list, description="Tables to exclude from relationship detection")

    @model_validator(mode='after')
    def validate_thresholds(self):
        """Ensure min_confidence_display <= confidence_threshold"""
        if self.min_confidence_display > self.confidence_threshold:
            raise ValueError(
                f"min_confidence_display ({self.min_confidence_display}) cannot be greater than "
                f"confidence_threshold ({self.confidence_threshold})"
            )
        return self


class AuditConfigModel(BaseModel):
    """Pydantic model for audit configuration validation"""
    model_config = ConfigDict(extra='ignore')  # Ignore extra fields in YAML

    # Audit metadata (optional)
    version: Optional[Union[int, float, str]] = None
    project: Optional[str] = None
    description: Optional[str] = None
    last_modified: Optional[str] = None

    # Required sections
    database: DatabaseConfig

    # Tables - can be list of strings or list of TableConfig dicts
    tables: List[Union[str, TableConfig]] = Field(default_factory=list)

    # Optional sections with defaults
    table_filters: Optional[TableFilters] = None
    sampling: SamplingConfig = Field(default_factory=SamplingConfig)
    security: SecurityConfig = Field(default_factory=SecurityConfig)
    output: OutputConfig = Field(default_factory=OutputConfig)
    filters: FiltersConfig = Field(default_factory=FiltersConfig)
    column_checks: Optional[ColumnChecksConfig] = None
    column_insights: Optional[ColumnInsightsConfig] = None
    relationship_detection: RelationshipDetectionConfig = Field(default_factory=RelationshipDetectionConfig)

    @field_validator('tables', mode='before')
    @classmethod
    def normalize_tables(cls, v):
        """Normalize tables list to handle both string and dict formats"""
        if not v:
            return []

        normalized = []
        for table in v:
            if isinstance(table, str):
                normalized.append({'name': table})
            elif isinstance(table, dict):
                normalized.append(table)
            else:
                raise ValueError(f"Table entry must be string or dict, got {type(table)}")
        return normalized


# ============================================================================
# Environment Variable Substitution
# ============================================================================

def _substitute_env_vars(value: Any) -> Any:
    """
    Recursively substitute environment variables in configuration values

    Supports multiple formats:
    - ${VAR_NAME}
    - $VAR_NAME
    - ${VAR_NAME:-default_value}  (with default)

    Args:
        value: Configuration value (can be str, dict, list, or other)

    Returns:
        Value with environment variables substituted
    """
    if isinstance(value, str):
        # Pattern: ${VAR_NAME} or ${VAR_NAME:-default}
        def replace_with_default(match):
            var_name = match.group(1)
            default_value = match.group(2) if match.group(2) else None
            env_value = os.environ.get(var_name)
            if env_value is not None:
                return env_value
            elif default_value is not None:
                return default_value
            else:
                raise ValueError(f"Environment variable '{var_name}' is not set and no default value provided")

        # First try ${VAR_NAME:-default} pattern
        value = re.sub(r'\$\{([A-Za-z_][A-Za-z0-9_]*)(:-([^}]*))?\}', replace_with_default, value)

        # Then try $VAR_NAME pattern (simpler, no default)
        def replace_simple(match):
            var_name = match.group(1)
            env_value = os.environ.get(var_name)
            if env_value is not None:
                return env_value
            else:
                raise ValueError(f"Environment variable '{var_name}' is not set")

        value = re.sub(r'\$([A-Za-z_][A-Za-z0-9_]*)', replace_simple, value)

        return value

    elif isinstance(value, dict):
        return {k: _substitute_env_vars(v) for k, v in value.items()}

    elif isinstance(value, list):
        return [_substitute_env_vars(item) for item in value]

    else:
        # Return other types as-is (int, bool, None, etc.)
        return value


# ============================================================================
# AuditConfig Class (wrapper around Pydantic model)
# ============================================================================

class AuditConfig:
    """Configuration class for audit parameters with validation"""

    def __init__(self, config_dict: Dict):
        """Initialize from dictionary (parsed from YAML) with Pydantic validation"""
        # Substitute environment variables
        config_dict = _substitute_env_vars(config_dict)

        # Validate config using Pydantic model
        try:
            self._model = AuditConfigModel(**config_dict)
        except Exception as e:
            raise ValueError(f"Configuration validation failed: {str(e)}") from e

        # Audit metadata
        self.audit_version = self._model.version
        self.audit_project = self._model.project
        self.audit_description = self._model.description
        self.audit_last_modified = self._model.last_modified

        # Database connection
        self.backend = self._model.database.backend
        self.connection_params = self._model.database.connection_params.model_dump(by_alias=True)

        # Extract default_database and default_schema (required)
        self.default_database = self.connection_params.get('default_database')
        self.default_schema = self.connection_params.get('default_schema')

        # Tables - normalize format
        self.tables = []
        self.table_primary_keys = {}
        self.table_queries = {}
        self.table_databases = {}  # Renamed: stores table-level database overrides
        self.table_schemas = {}     # Stores table-level schema overrides
        self.table_include_columns = {}  # Per-table column includes
        self.table_exclude_columns = {}  # Per-table column excludes

        for table_entry in self._model.tables:
            if isinstance(table_entry, str):
                self.tables.append(table_entry)
            else:  # TableConfig
                self.tables.append(table_entry.name)
                if table_entry.primary_key:
                    self.table_primary_keys[table_entry.name] = table_entry.primary_key
                if table_entry.query:
                    self.table_queries[table_entry.name] = table_entry.query

                # Store table-specific overrides (unified naming)
                if table_entry.database:
                    self.table_databases[table_entry.name] = table_entry.database
                if table_entry.table_schema:
                    self.table_schemas[table_entry.name] = table_entry.table_schema

                # Store per-table column filters
                if table_entry.include_columns:
                    self.table_include_columns[table_entry.name] = table_entry.include_columns
                if table_entry.exclude_columns:
                    self.table_exclude_columns[table_entry.name] = table_entry.exclude_columns

        # Table filtering
        if self._model.table_filters:
            self.auto_discover = self._model.table_filters.auto_discover
            self.exclude_patterns = self._model.table_filters.exclude_patterns
            self.include_patterns = self._model.table_filters.include_patterns
        else:
            self.auto_discover = False
            self.exclude_patterns = []
            self.include_patterns = []

        # Sampling
        self.sample_size = self._model.sampling.sample_size
        self.sampling_method = self._model.sampling.method
        self.sampling_key_column = self._model.sampling.key_column
        self.table_sampling_config = {
            k: v.model_dump(exclude_none=True)
            for k, v in self._model.sampling.tables.items()
        }

        # Security
        self.mask_pii = self._model.security.mask_pii
        self.custom_pii_keywords = self._model.security.custom_pii_keywords

        # Output
        self.output_dir = Path(self._model.output.directory)
        self.export_formats = self._model.output.formats
        self.file_prefix = self._model.output.file_prefix
        self.auto_open_html = self._model.output.auto_open_html
        self.thousand_separator = self._model.output.number_format.thousand_separator
        self.decimal_places = self._model.output.number_format.decimal_places

        # Column filters
        self.include_columns = self._model.filters.include_columns
        self.exclude_columns = self._model.filters.exclude_columns

        # Column checks
        if self._model.column_checks:
            self.checks_enabled = True
            self.column_check_defaults = self._model.column_checks.defaults
            self.column_check_overrides = self._model.column_checks.tables
        else:
            self.checks_enabled = False
            self.column_check_defaults = {}
            self.column_check_overrides = {}

        # Column insights
        if self._model.column_insights:
            self.insights_enabled = True
            self.column_insights_defaults = self._model.column_insights.defaults
            self.column_insights_overrides = self._model.column_insights.tables
        else:
            self.insights_enabled = False
            self.column_insights_defaults = {}
            self.column_insights_overrides = {}

        # Relationship detection
        self.relationship_detection_enabled = self._model.relationship_detection.enabled
        self.relationship_confidence_threshold = self._model.relationship_detection.confidence_threshold
        self.relationship_min_display_confidence = self._model.relationship_detection.min_confidence_display
        self.relationship_exclude_tables = self._model.relationship_detection.exclude_tables

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
        if 'string' in dtype_key or 'utf8' in dtype_key or 'text' in dtype_key or 'char' in dtype_key:
            base_config = self.column_check_defaults.get('string', {}).copy()
        elif 'datetime' in dtype_key or 'date' in dtype_key:
            base_config = self.column_check_defaults.get('datetime', {}).copy()
        elif 'int' in dtype_key or 'float' in dtype_key or 'decimal' in dtype_key or 'numeric' in dtype_key or 'number' in dtype_key:
            base_config = self.column_check_defaults.get('numeric', {}).copy()
        else:
            base_config = {}

        # Apply table-level overrides (case-insensitive lookup)
        # Normalize to lowercase for lookup since Snowflake returns uppercase column names
        table_name_lower = table_name.lower()
        column_name_lower = column_name.lower()

        if table_name_lower in self.column_check_overrides:
            table_config = self.column_check_overrides[table_name_lower]
            if column_name_lower in table_config:
                column_overrides = table_config[column_name_lower]
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
        if 'string' in dtype_key or 'utf8' in dtype_key or 'text' in dtype_key or 'char' in dtype_key:
            base_config = self.column_insights_defaults.get('string', {}).copy()
        elif 'datetime' in dtype_key or 'date' in dtype_key:
            base_config = self.column_insights_defaults.get('datetime', {}).copy()
        elif 'bool' in dtype_key:
            base_config = self.column_insights_defaults.get('boolean', {}).copy()
        elif 'int' in dtype_key or 'float' in dtype_key or 'decimal' in dtype_key or 'numeric' in dtype_key or 'number' in dtype_key:
            base_config = self.column_insights_defaults.get('numeric', {}).copy()
        else:
            base_config = {}

        # Apply table-level overrides (case-insensitive lookup)
        # Normalize to lowercase for lookup since Snowflake returns uppercase column names
        table_name_lower = table_name.lower()
        column_name_lower = column_name.lower()

        if table_name_lower in self.column_insights_overrides:
            table_config = self.column_insights_overrides[table_name_lower]
            if column_name_lower in table_config:
                column_overrides = table_config[column_name_lower]
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

    def get_table_database(self, table_name: str) -> str:
        """
        Get database for a specific table

        Args:
            table_name: Name of the table

        Returns:
            Database name (falls back to default_database if not specified)
        """
        # Return table-specific database if defined, otherwise use default_database
        return self.table_databases.get(table_name, self.default_database)

    def get_table_schema(self, table_name: str) -> str:
        """
        Get schema/dataset for a specific table

        Args:
            table_name: Name of the table

        Returns:
            Schema/dataset name (falls back to default_schema if not specified)
        """
        # Return table-specific schema if defined, otherwise use default_schema
        return self.table_schemas.get(table_name, self.default_schema)

    def get_table_column_filters(self, table_name: str) -> Dict[str, List[str]]:
        """
        Get column filters for a specific table

        Args:
            table_name: Name of the table

        Returns:
            Dictionary with 'include_columns' and 'exclude_columns' lists
            (per-table filters override global filters)
        """
        # Per-table filters take precedence over global filters
        include_cols = self.table_include_columns.get(table_name) or self.include_columns
        exclude_cols = self.table_exclude_columns.get(table_name) or self.exclude_columns

        return {
            'include_columns': include_cols,
            'exclude_columns': exclude_cols
        }

    def get_table_connection_params(self, table_name: str) -> Dict:
        """
        Get connection parameters for a specific table

        Args:
            table_name: Name of the table

        Returns:
            Dictionary with connection parameters (global merged with table-specific)
        """
        # Start with global connection params
        conn_params = self.connection_params.copy()

        # Override default_database if table has specific database
        table_database = self.get_table_database(table_name)
        if table_database:
            conn_params['default_database'] = table_database

        # Override default_schema if table has specific schema
        table_schema = self.get_table_schema(table_name)
        if table_schema:
            conn_params['default_schema'] = table_schema

        return conn_params

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
        """Load configuration from YAML file with validation"""
        try:
            with open(yaml_path, 'r') as f:
                config_dict = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ValueError(f"Failed to parse YAML file: {str(e)}") from e
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file not found: {yaml_path}")

        return cls(config_dict)

    def to_dict(self) -> Dict:
        """Convert config back to dictionary"""
        return {
            'database': {
                'backend': self.backend,
                'connection_params': self.connection_params
            },
            'tables': self.tables,
            'sampling': {
                'sample_size': self.sample_size
            },
            'security': {
                'mask_pii': self.mask_pii,
                'custom_pii_keywords': self.custom_pii_keywords
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
