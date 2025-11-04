# Data Warehouse Table Auditor

High-performance data quality checks and profiling for modern data
warehouses --- with granular configuration, secure handling, and rich
reporting out of the box.

------------------------------------------------------------------------

## Why Use It?

-   Catch bad data early (format issues, outliers, inconsistencies)
-   Profile columns automatically
-   Detect key/uniqueness issues
-   Detect wrong column type
-   Generate audit reports programmatically
-   Export results to HTML/JSON/CSV
-   Run locally or in CI jobs

Ideal for data engineering, quality control, governance, and migration
validation.

------------------------------------------------------------------------

## Core Features

### ✅ Data Quality Checks

-   Trailing spaces
-   Case-sensitive duplicates
-   Unexpected special characters
-   Numeric-string detection
-   Timestamp anomalies (constant hours, midnight patterns)
-   Date range outliers

Configurable: - per data type (global defaults) - per table - per column

------------------------------------------------------------------------

### 📊 Data Profiling (Insights)

Automatic column-level statistics:

**String** - Top values + frequencies - Min/avg/max length

**Numeric** - Min, max, mean, median, std - Custom quantiles

**Datetime** - Ranges - Most common dates

------------------------------------------------------------------------

### 📈 Reporting & Export

-   Console UI with status indicators
-   HTML reports (cards, tables, insights)
-   JSON (complete metadata + results)
-   CSV summaries (spreadsheet-friendly)

------------------------------------------------------------------------

### 🧠 Metadata Intelligence

-   3-tier primary key detection:
    1.  User config
    2.  INFORMATION_SCHEMA
    3.  Heuristics
-   Table type (TABLE/VIEW/MV)
-   Row counts, creation time
-   Column check status: OK / ERROR / NOT_CHECKED
-   Sample primary key values included for debugging

------------------------------------------------------------------------

## Project Structure

    database_audit/
    ├── dw_auditor/                 # Main package
    │   ├── core/                   # Core auditing logic
    │   ├── checks/                 # Data quality checks
    │   ├── insights/               # Profiling logic
    │   ├── utils/                  # Security, formatting
    │   └── exporters/              # HTML/JSON/CSV outputs
    ├── audit.py                    # Entry point script
    ├── audit_config.yaml           # Config (edit this!)
    └── requirements.txt            # Dependencies

------------------------------------------------------------------------

## Installation

Option 1: Use provided requirements:

``` bash
pip install -r requirements.txt
```

Option 2: Install manually:

``` bash
pip install polars ibis-framework[bigquery,snowflake]             google-cloud-bigquery             snowflake-connector-python             pyyaml
```

------------------------------------------------------------------------

## Quick Start (Recommended)

1.  Configure your connection and tables in `audit_config.yaml`
2.  Run:

``` bash
python audit.py
```

This will:

-   Connect to your warehouse
-   Audit configured tables
-   Print results in the console
-   Generate HTML, JSON, and CSV output under `/audit_results/`

Open the HTML files for a visual walkthrough of issues and insights.

------------------------------------------------------------------------

## Minimal Configuration Example (`audit_config.yaml`)

``` yaml
database:
  backend: "bigquery"
  connection_params:
    project_id: "my-project"
    dataset_id: "analytics"

tables:
  - name: users
  - name: orders

output:
  directory: "audit_results"
  formats: [html, json, csv]
```

------------------------------------------------------------------------

## Column Check Configuration (Optional)

``` yaml
column_checks:
  defaults:
    string:
      special_chars: true
      trailing_spaces: true

  tables:
    users:
      name:
        special_chars: false  # allow accents
```

------------------------------------------------------------------------

## Column Insights Configuration (Optional)

``` yaml
column_insights:
  defaults:
    numeric:
      quantiles: [0.25, 0.5, 0.75]

  tables:
    orders:
      order_amount:
        quantiles: [0.1, 0.9, 0.95, 0.99]
```

------------------------------------------------------------------------

## Sampling & Security (Optional)

Configure:

-   sampling counts/threshholds
-   PII masking
-   filtering columns
-   failure thresholds

Useful for large production tables or compliance-sensitive data.

------------------------------------------------------------------------

## Supported Databases

Backed by **Ibis**:

-   BigQuery
    -   Application Default Credentials
    -   Dataset-level metadata
-   Snowflake
    -   Password authentication
    -   Role, warehouse, database selection

(Additional auth methods coming soon)

------------------------------------------------------------------------

## Output Formats

Each audit run produces:

### 🖥 Console

-   Column-level statuses
-   Insight summaries
-   Issue examples w/ primary keys
-   Phase timing

### 🌐 HTML

-   Interactive tables
-   Stats cards
-   Top value breakdowns
-   Quantiles & ranges

### 📦 JSON

-   Programmatic metadata
-   Insights per column
-   Issue details

### 📄 CSV

-   Summary statistics
-   Issue summaries
-   Spreadsheet-friendly comparisons

------------------------------------------------------------------------

## Example Usage

``` bash
python audit.py
```

Results go to:

    audit_results/<timestamp>/

Look for `*.html` first --- it's the easiest way to explore results.

------------------------------------------------------------------------

## Tips

-   Start with a small table to validate config
-   Add primary keys if missing --- improves context sampling
-   Tweak special character checks for names/emails
-   Use sampling for massive fact tables

------------------------------------------------------------------------

## License

MIT
