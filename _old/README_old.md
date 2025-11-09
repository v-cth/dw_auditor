# Data Warehouse Table Auditor

High-performance data quality checks and data profiling for data warehouses with security best practices.

## Features

### Data Quality Checks

Default data checks can be defined at type-level configuration, or be defined specifically per table and column, overwritten the default config.


### Data Profiling Insights

Comprehensive column-level data profiling
  - String columns: Top N values with frequencies, length statistics
  - Numeric columns: Min/max/mean/median/std, customizable quantiles, distribution analysis
  - DateTime columns: Date ranges, most common dates

## Others

### Comprehensive Reporting
  - Console output with insights visualization
  - HTML reports with beautiful insights cards and tables
  - JSON export with complete metadata
  - CSV summary exports

### Table sampling



### Column-Level Configuration Matrix

Granular control over which checks run on each column
  - Global defaults per data type
  - Per-table, per-column overrides

Example: Skip special character checks for columns with expected accents or symbols

### Enhanced Metadata & Intelligence

  - **Primary Key Detection**: 3-tier system (user config → INFORMATION_SCHEMA → auto-detection)
  - **Table Metadata**: Type (TABLE/VIEW/MATERIALIZED VIEW), creation time, row count, UID
  - **Column Status Indicators**: OK/ERROR/NOT_CHECKED for each column
  - **Error Context**: Primary key values included in examples for easy row identification

- **Performance Visibility**: Detailed timing breakdown by audit phase and check type



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

## Technical Features



### Data Quality Checks
- **String Checks**: Trailing/leading spaces, case duplicates, special characters, numeric strings
- **Timestamp Checks**: Constant hour patterns, midnight detection, date outliers
- **Column-Level Configuration**: Granular control over which checks run on which columns

### Data Profiling Insights



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


## Configuration

The `audit_config.yaml` file provides comprehensive control over all aspects of the audit process:

### Database Connection
- **Backend**: `bigquery` or `snowflake`
- **Connection Parameters**:
  - BigQuery: `project_id`, `dataset_id`
  - Snowflake: `account`, `user`, `password`, `database`, `warehouse`, `schema` 

### Tables Configuration
- List of tables to audit
- Optional primary key specification per table (single or composite keys)
- Optional schema if different from the default schema defined in database connection
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
  - Service account authentication (coming soon ...)
  - Application Default Credentials
  - Dataset-level access control
  - INFORMATION_SCHEMA querying for metadata

- **Snowflake**
  - User/password authentication
  - External brower authentification (coming soon ...)
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
- Beautiful interactive report
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
