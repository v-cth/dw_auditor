# Data Warehouse Table Auditor

High-performance data quality checks and data profiling for data warehouses with security best practices.

## What's New

### 🎉 Major Features Added

- **Data Profiling Insights**: Comprehensive column-level data profiling separate from quality checks
  - String columns: Top N values with frequencies, length statistics
  - Numeric columns: Min/max/mean/median/std, customizable quantiles, distribution analysis
  - DateTime columns: Date ranges, most common dates

- **Column-Level Configuration Matrix**: Granular control over which checks run on each column
  - Global defaults per data type
  - Per-table, per-column overrides
  - Example: Skip special character checks for columns with expected accents or symbols

- **Enhanced Metadata & Intelligence**:
  - **Primary Key Detection**: 3-tier system (user config → INFORMATION_SCHEMA → auto-detection)
  - **Table Metadata**: Type (TABLE/VIEW/MATERIALIZED VIEW), creation time, row count, UID
  - **Column Status Indicators**: OK/ERROR/NOT_CHECKED for each column
  - **Error Context**: Primary key values included in examples for easy row identification

- **Performance Visibility**: Detailed timing breakdown by audit phase and check type

- **Comprehensive Reporting**:
  - Console output with insights visualization
  - HTML reports with beautiful insights cards and tables
  - JSON export with complete metadata
  - CSV summary exports

## Project Structure

```
database_audit/
├── dw_auditor/                 # Main package
│   ├── __init__.py             # Package exports
│   ├── core/                   # Core auditing logic
│   │   ├── __init__.py
│   │   ├── auditor.py          # Main auditor class
│   │   ├── config.py           # Configuration management
│   │   └── database.py         # Database connections (Ibis)
│   ├── checks/                 # Data quality checks
│   │   ├── __init__.py
│   │   ├── string_checks.py    # String validation checks
│   │   └── timestamp_checks.py # Timestamp validation checks
│   ├── insights/               # Data profiling insights
│   │   ├── __init__.py
│   │   ├── column_insights.py  # Insights orchestrator
│   │   ├── string_insights.py  # String column profiling
│   │   ├── numeric_insights.py # Numeric column profiling
│   │   └── datetime_insights.py # DateTime column profiling
│   ├── utils/                  # Utility functions
│   │   ├── __init__.py
│   │   ├── security.py         # PII masking & security
│   │   └── output.py           # Console output formatting
│   └── exporters/              # Export functionality
│       ├── __init__.py
│       ├── dataframe_export.py # DataFrame export
│       ├── json_export.py      # JSON export
│       ├── html_export.py      # HTML report generation
│       └── summary_export.py   # Summary CSV export
├── audit.py                    # Main audit script
├── audit_config.yaml           # Configuration file
└── requirements.txt            # Dependencies
```

## Features

### Core Capabilities
- **Direct Database Auditing**: Query tables directly without exporting files using Ibis
- **Multi-Database Support**: BigQuery and Snowflake (via Ibis framework)
- **PII Protection**: Automatic masking of sensitive columns
- **Smart Sampling**: Database-native sampling for large tables
- **Flexible Export**: HTML reports, JSON, CSV, or Polars DataFrames
- **Audit Logging**: Track all audit activities with sanitized connection strings

### Data Quality Checks
- **String Checks**: Trailing/leading spaces, case duplicates, special characters, numeric strings
- **Timestamp Checks**: Constant hour patterns, midnight detection, date outliers
- **Column-Level Configuration**: Granular control over which checks run on which columns

### Data Profiling Insights
- **String Columns**: Top N most frequent values, length statistics (min/max/avg)
- **Numeric Columns**: Min/max/mean/median/std, quantiles, top values
- **DateTime Columns**: Date ranges, most common dates
- **Configurable Profiling**: Control insights per column with global defaults and overrides

### Metadata & Intelligence
- **Primary Key Detection**: 3-tier detection (user config → INFORMATION_SCHEMA → auto-detection)
- **Table Metadata**: Table type (TABLE, VIEW, MATERIALIZED VIEW), creation time, row count
- **Status Indicators**: OK/ERROR/NOT_CHECKED status for each column
- **Performance Metrics**: Detailed timing breakdown for each audit phase and check type
- **Error Context**: Primary key values included in error examples for easy row identification

## Installation

```bash
pip install -r requirements.txt
```

Or install manually:
```bash
pip install polars ibis-framework[bigquery,snowflake] google-cloud-bigquery snowflake-connector-python pyyaml
```

## Quick Start

### Using the Main Audit Script (Recommended)

The easiest way to run audits is using the provided `audit.py` script with a YAML configuration:

```bash
python audit.py
```

This will:
1. Load configuration from `audit_config.yaml`
2. Connect to your database(s)
3. Audit all configured tables
4. Generate HTML reports, JSON, and CSV exports
5. Display results in the console with insights

### Configuration Example

Edit `audit_config.yaml` to configure your audit:

```yaml
database:
  backend: "bigquery"  # or "snowflake"
  connection_params:
    project_id: "my-gcp-project"
    dataset_id: "analytics"

tables:
  - name: users
    primary_key: user_id  # Optional: specify primary key
  - name: orders
    primary_key: [order_id, line_id]  # Composite key support

# Configure which checks to run per column
column_checks:
  defaults:
    string:
      trailing_spaces: true
      case_duplicates: true
      special_chars: true
      numeric_strings: true
    datetime:
      timestamp_patterns: true
      date_outliers: true

  tables:
    users:
      email:
        special_chars: false  # Allow @ symbols
      name:
        special_chars: false  # Allow accents

# Configure data profiling insights
column_insights:
  defaults:
    string:
      top_values: 10
      min_length: true
      max_length: true
      avg_length: true
    numeric:
      min: true
      max: true
      mean: true
      median: true
      quantiles: [0.25, 0.5, 0.75]
      top_values: 5
    datetime:
      min_date: true
      max_date: true
      date_range_days: true
      most_common_dates: 5

  tables:
    orders:
      order_amount:
        quantiles: [0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99]

output:
  directory: "audit_results"
  formats: [html, json, csv]
```

### Programmatic Usage - BigQuery

```python
from dw_auditor import AuditConfig, SecureTableAuditor

# Load configuration
config = AuditConfig.from_yaml('audit_config.yaml')

# Create auditor
auditor = SecureTableAuditor()

# Audit from BigQuery
results = auditor.audit_from_database(
    table_name='users',
    backend='bigquery',
    connection_params={
        'project_id': 'my-gcp-project',
        'dataset_id': 'analytics',
        'credentials_path': '/path/to/service-account-key.json'
    },
    column_check_config=config,  # Pass config for check matrix
    user_primary_key='user_id'   # Optional primary key
)

# Results include:
# - results['column_summary']: All columns with metrics
# - results['column_insights']: Data profiling insights
# - results['columns']: Columns with issues
# - results['table_metadata']: Table type, primary keys, etc.
```

### Programmatic Usage - Snowflake

```python
from dw_auditor import SecureTableAuditor

auditor = SecureTableAuditor()

# Audit from Snowflake
results = auditor.audit_from_database(
    table_name='CUSTOMERS',
    backend='snowflake',
    connection_params={
        'account': 'my-account',
        'user': 'my-user',
        'password': 'my-password',
        'database': 'ANALYTICS_DB',
        'warehouse': 'COMPUTE_WH',
        'schema': 'PUBLIC'
    }
)
```

## Modules

### Core (`dw_auditor/core/`)

- **`auditor.py`**: Main `SecureTableAuditor` class that coordinates all auditing, checks, and insights
- **`config.py`**: `AuditConfig` class for YAML-based configuration with column-level check matrix and insights configuration
- **`database.py`**: `DatabaseConnection` class for Ibis-based database connections with INFORMATION_SCHEMA querying

### Checks (`dw_auditor/checks/`)

- **`string_checks.py`**: String validation (trailing/leading spaces, case duplicates, special chars, numeric strings)
- **`timestamp_checks.py`**: Timestamp validation (constant hour, midnight detection, date outliers)

### Insights (`dw_auditor/insights/`)

- **`column_insights.py`**: Orchestrator that routes to type-specific insight generators
- **`string_insights.py`**: String column profiling (top values with frequencies, length statistics)
- **`numeric_insights.py`**: Numeric column profiling (min/max/mean/median/std, quantiles, top values)
- **`datetime_insights.py`**: DateTime column profiling (date ranges, most common dates)

### Utils (`dw_auditor/utils/`)

- **`security.py`**: PII masking and connection string sanitization
- **`output.py`**: Console output formatting with column summary, insights display, and summary statistics

### Exporters (`dw_auditor/exporters/`)

- **`dataframe_export.py`**: Export to Polars DataFrame
- **`json_export.py`**: Export to JSON format with all metadata
- **`html_export.py`**: Generate beautiful HTML reports with insights visualization
- **`summary_export.py`**: Export column summary to CSV

## Configuration

The `audit_config.yaml` file provides comprehensive control over all aspects of the audit process:

### Database Connection
- **Backend**: `bigquery` or `snowflake`
- **Connection Parameters**:
  - BigQuery: `project_id`, `dataset_id`, `credentials_path`
  - Snowflake: `account`, `user`, `password`, `database`, `warehouse`, `schema`

### Tables Configuration
- List of tables to audit
- Optional primary key specification per table (single or composite keys)
- Supports both simple string format and dict format with metadata

### Column-Level Check Matrix
- **Global defaults per data type** (string, datetime, numeric)
- **Per-table, per-column overrides** for fine-grained control
- Example: Disable special character checks for columns with expected accents

```yaml
column_checks:
  defaults:
    string:
      trailing_spaces: true
      case_duplicates: true
      special_chars: true
      numeric_strings: true
  tables:
    users:
      name:
        special_chars: false  # Allow accents
```

### Column Insights Configuration
- **Global defaults per data type** (string, numeric, datetime)
- **Per-table, per-column overrides** for custom profiling
- Control top N values, statistics, quantiles per column

```yaml
column_insights:
  defaults:
    numeric:
      quantiles: [0.25, 0.5, 0.75]
  tables:
    orders:
      order_amount:
        quantiles: [0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99]  # More detailed
```

### Additional Settings
- **Sampling**: Size, threshold, database-native sampling
- **Security**: PII masking, custom PII keywords
- **Thresholds**: Numeric string percentage, constant hour, date outliers
- **Output**: Directory, formats (html/json/csv), file prefix
- **Filters**: Include/exclude specific columns

## Supported Databases

The auditor uses **Ibis** as the database abstraction layer, currently supporting:

- **BigQuery** (Google Cloud)
  - Service account authentication
  - Application Default Credentials
  - Dataset-level access control
  - INFORMATION_SCHEMA querying for metadata

- **Snowflake**
  - User/password authentication
  - Role-based access control
  - Warehouse and database selection
  - INFORMATION_SCHEMA querying for metadata

## Output & Reports

Each audit generates multiple output formats in a timestamped directory:

### Console Output
- Column summary table with status indicators (✓ OK, ✗ ERROR, - NOT_CHECKED)
- Data profiling insights for each column
- Issues found with examples and primary key context
- Performance timing breakdown

### HTML Report
- Beautiful interactive report with gradient header
- Column summary table with color-coded status
- Data insights visualization:
  - Top values tables
  - Statistics cards
  - Quantile breakdowns
  - Date range displays
- Issue details with suggestions and examples
- Table metadata (type, primary keys, row counts)

### JSON Export
- Complete structured data including:
  - `column_summary`: All columns with metrics
  - `column_insights`: Profiling data
  - `columns`: Columns with issues
  - `table_metadata`: Primary keys, table type, etc.
- Perfect for programmatic analysis and automation

### CSV Exports
- Main CSV: Issues by column
- Summary CSV: All columns with basic metrics

## Examples

Run the main audit script:

```bash
python audit.py
```

This will audit all tables configured in `audit_config.yaml` and generate a complete audit report with:
- Console output showing progress and findings
- HTML reports for visual review
- JSON exports for automation
- CSV files for spreadsheet analysis

## License

MIT
