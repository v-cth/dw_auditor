"""
Configuration management for the data warehouse auditor with Pydantic validation
"""

from typing import Dict, List, Union, Optional, Any, Literal
from pathlib import Path
import yaml
import fnmatch
from pydantic import BaseModel, Field, field_validator, model_validator, ConfigDict


# ============================================================================
# Pydantic Models for Configuration Validation
# ============================================================================

class TableConfig(BaseModel):
    """Configuration for a single table"""
    model_config = ConfigDict(protected_namespaces=(), populate_by_name=True)

    name: str = Field(..., min_length=1, description="Table name")
    primary_key: Optional[Union[str, List[str]]] = Field(None, description="Primary key column(s)")
    query: Optional[str] = Field(None, description="Custom SQL query")
    db_schema: Optional[str] = Field(None, alias="schema", serialization_alias="schema", description="Override schema/dataset for this table")

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

    # Common fields we want to validate
    db_schema: Optional[str] = Field(None, alias="schema", serialization_alias="schema", description="Default schema/dataset")


class DatabaseConfig(BaseModel):
    """Database connection configuration"""
    backend: Literal['bigquery', 'snowflake'] = Field(..., description="Database backend")
    connection_params: ConnectionParams = Field(..., description="Connection parameters")

    @model_validator(mode='after')
    def validate_backend_params(self):
        """Validate backend-specific required parameters"""
        params = self.connection_params.model_dump(by_alias=True)

        if self.backend == 'bigquery':
            if 'project_id' not in params:
                raise ValueError("BigQuery backend requires 'project_id' in connection_params")

        elif self.backend == 'snowflake':
            required = ['account', 'user', 'database']
            missing = [p for p in required if p not in params]
            if missing:
                raise ValueError(f"Snowflake backend requires: {', '.join(missing)}")

            # Password is required unless using external browser authentication
            if 'password' not in params and params.get('authenticator') != 'externalbrowser':
                raise ValueError("Snowflake backend requires 'password' (or set authenticator='externalbrowser' for SSO)")

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
    sample_in_db: bool = Field(True, description="Use database-native sampling")
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
# AuditConfig Class (wrapper around Pydantic model)
# ============================================================================

class AuditConfig:
    """Configuration class for audit parameters with validation"""

    def __init__(self, config_dict: Dict):
        """Initialize from dictionary (parsed from YAML) with Pydantic validation"""
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
        self.schema = self.connection_params.get('schema')

        # Tables - normalize format
        self.tables = []
        self.table_primary_keys = {}
        self.table_queries = {}
        self.table_schemas = {}

        for table_entry in self._model.tables:
            if isinstance(table_entry, str):
                self.tables.append(table_entry)
            else:  # TableConfig
                self.tables.append(table_entry.name)
                if table_entry.primary_key:
                    self.table_primary_keys[table_entry.name] = table_entry.primary_key
                if table_entry.query:
                    self.table_queries[table_entry.name] = table_entry.query
                if table_entry.db_schema:
                    self.table_schemas[table_entry.name] = table_entry.db_schema

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
        self.sample_in_db = self._model.sampling.sample_in_db
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
        elif 'bool' in dtype_key:
            base_config = self.column_insights_defaults.get('boolean', {}).copy()
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
                'connection_params': self.connection_params,
                'schema': self.schema
            },
            'tables': self.tables,
            'sampling': {
                'sample_size': self.sample_size,
                'sample_in_db': self.sample_in_db
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
