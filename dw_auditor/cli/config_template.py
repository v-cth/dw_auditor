"""
Minimal configuration template for dw_auditor
"""

MINIMAL_CONFIG_TEMPLATE = """# Data Warehouse Audit Configuration
# ============================================================================
# Quick start configuration - Edit the values below with your database details
# For full configuration options, see: https://github.com/your-repo/docs

# Database Connection
# ----------------------------------------------------------------------------
database:
  backend: "bigquery"  # Options: bigquery, snowflake
  connection_params:
    # Required for all backends
    default_database: "your-project-id"  # BigQuery: project_id | Snowflake: DATABASE
    default_schema: "your-dataset"       # BigQuery: dataset | Snowflake: SCHEMA

    # BigQuery Authentication (optional):
    # credentials_path: "/path/to/service-account-key.json"
    # If not specified, uses Application Default Credentials (gcloud auth)

    # Snowflake Connection (if using Snowflake, use UPPERCASE):
    # account: "your-account"
    # user: "your-user"
    # password: "your-password"
    # warehouse: "your-warehouse"  # Optional
    # role: "your-role"            # Optional

# Tables to Audit
# ----------------------------------------------------------------------------
# List specific tables or leave empty to discover all tables automatically
tables:
  # Examples:
  # - customers
  # - orders
  #
  # Cross-schema/database tables:
  # - name: dim_users
  #   schema: analytics
  #
  # - name: events
  #   database: other-project
  #   schema: raw_data

# Sampling (optional)
# ----------------------------------------------------------------------------
# For large tables (>100k rows), sample data for faster audits
sampling:
  sample_size: 10000  # Number of rows to analyze from large tables

# Output Configuration
# ----------------------------------------------------------------------------
output:
  directory: "audit_results"
  formats: [html, json, csv]  # Choose: html, json, csv
  auto_open_html: false # Opens HTML reports in browser when audit ends

  # Number formatting (optional)
  number_format:
    thousand_separator: ","
    decimal_places: 1

# Quality Checks (optional)
# ----------------------------------------------------------------------------
# Enable/disable specific data quality checks
column_checks:
  defaults:
    string:
      trailing_characters: true   # Detect trailing whitespace
      case_duplicates: true        # Find duplicates ignoring case
    datetime:
      future_dates: true           # Detect dates in the future
      date_outliers:               # Detect suspicious placeholder years
        min_year: 1950
        max_year: 2100

# Column Insights (optional)
# ----------------------------------------------------------------------------
# Profile data distributions and statistics
column_insights:
  defaults:
    numeric:
      quantiles: true  # Calculate percentiles
    string:
      top_values:      # Show most frequent values
        limit: 10
"""
