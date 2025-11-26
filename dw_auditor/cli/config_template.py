"""
Minimal configuration template for dw_auditor
"""

MINIMAL_CONFIG_TEMPLATE = """# Data Warehouse Audit Configuration
# ============================================================================
# Quick start configuration - Edit the values below with your database details
# For full documentation, see: https://github.com/v-cth/database_audit

# Database Connection
# ----------------------------------------------------------------------------
database:
  backend: "snowflake"  # Options: bigquery, snowflake, databricks
  connection_params:
    # Required for all backends
    default_database: "MY_DATABASE"  # BigQuery: project_id | Snowflake: DATABASE | Databricks: CATALOG
    default_schema: "MY_SCHEMA"      # BigQuery: dataset | Snowflake/Databricks: SCHEMA

    # ==================================================
    # SNOWFLAKE CONNECTION (recommended: use environment variables)
    # ==================================================
    # Create a .env file in this directory with:
    #   export SNOWFLAKE_ACCOUNT='your-account'
    #   export SNOWFLAKE_USER='your-username'
    #   export SNOWFLAKE_PASSWORD='your-password'
    #
    # Note: Use 'export' and single quotes (especially if password contains special chars like $)
    #
    # Then reference them here (keeps credentials out of version control):
    account: "${SNOWFLAKE_ACCOUNT}"
    user: "${SNOWFLAKE_USER}"
    password: "${SNOWFLAKE_PASSWORD}"
    # warehouse: "${SNOWFLAKE_WAREHOUSE:-COMPUTE_WH}"  # Optional, with default value
    # role: "${SNOWFLAKE_ROLE}"                        # Optional

    # ==================================================
    # BIGQUERY CONNECTION
    # ==================================================
    # Uncomment and configure if using BigQuery:
    # credentials_path: "/path/to/service-account.json"  # Optional
    # If credentials_path not specified, uses: gcloud auth application-default login

    # ==================================================
    # DATABRICKS CONNECTION (recommended: use environment variables)
    # ==================================================
    # Uncomment and configure if using Databricks:
    # server_hostname: "${DATABRICKS_SERVER_HOSTNAME}"  # e.g., "myworkspace.cloud.databricks.com"
    # http_path: "${DATABRICKS_HTTP_PATH}"              # e.g., "/sql/1.0/warehouses/abc123"
    #
    # Authentication options (choose one):
    # Option 1 - OAuth/AAD (recommended for enterprise):
    # auth_type: "databricks-oauth"  # or "azure-oauth"
    #
    # Option 2 - Personal Access Token:
    # access_token: "${DATABRICKS_TOKEN}"
    #
    # Option 3 - Basic auth:
    # username: "${DATABRICKS_USERNAME}"
    # password: "${DATABRICKS_PASSWORD}"
    #


# Tables to Audit
# ----------------------------------------------------------------------------
# List specific tables or leave empty to discover all tables automatically
tables:
  # Examples:
  # - customers
  # - orders
  #
  # Cross-schema/database tables:
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
      min: true
      max: true
      mean: true
      std_dev: true
    string:
      top_values: 10     # Show 10 most frequent values

# Relationship Detection Configuration
# ----------------------------------------------------------------------------
# Automatically discover foreign key relationships between tables
relationship_detection:
  enabled: false    
"""
