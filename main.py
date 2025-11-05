"""
Data Warehouse Table Auditor - Main Entry Point

Example usage showing how to use the refactored auditor package
"""

from dw_auditor import SecureTableAuditor, AuditConfig


def main():
    """Main entry point demonstrating usage examples"""
    print("="*60)
    print("Secure Data Warehouse Table Auditor")
    print("="*60)
    print("\nUsage Examples:\n")

    # Example 1: Audit directly from database (RECOMMENDED)
    print("# Example 1: Direct database audit with BigQuery")
    print("-" * 60)
    print("""
auditor = SecureTableAuditor()

# BigQuery
results = auditor.audit_from_database(
    table_name='users',
    backend='bigquery',
    connection_params={
        'project_id': 'my-gcp-project',
        'schema': 'analytics',
        'credentials_path': '/path/to/service-account-key.json'
    },
    mask_pii=True,          # Auto-mask sensitive columns
    sample_in_db=True        # Sample in DB for speed
)

# Snowflake
results = auditor.audit_from_database(
    table_name='ORDERS',
    backend='snowflake',
    connection_params={
        'account': 'my-account',
        'user': 'my-user',
        'password': 'my-password',
        'database': 'ANALYTICS_DB',
        'warehouse': 'COMPUTE_WH',
        'schema': 'PUBLIC'
    },
    mask_pii=True,
    sample_in_db=True
)

# Custom query with BigQuery
results = auditor.audit_from_database(
    table_name='recent_transactions',
    backend='bigquery',
    connection_params={
        'project_id': 'my-project',
        'schema': 'sales'
    },
    custom_query='SELECT * FROM transactions WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)'
)
""")

    # Example 2: Audit from file
    print("\n# Example 2: Audit from file")
    print("-" * 60)
    print("""
auditor = SecureTableAuditor()

# From CSV
results = auditor.audit_from_file('data.csv', mask_pii=True)

# From Parquet
results = auditor.audit_from_file('data.parquet', table_name='my_table')
""")

    # Example 3: Direct DataFrame audit
    print("\n# Example 3: Direct DataFrame audit")
    print("-" * 60)
    print("""
import polars as pl

df = pl.DataFrame({...})
auditor = SecureTableAuditor()
results = auditor.audit_table(df, 'my_table')
""")

    # Example 4: Using configuration file
    print("\n# Example 4: Using YAML configuration")
    print("-" * 60)
    print("""
from dw_auditor import AuditConfig

# Load config from YAML
config = AuditConfig.from_yaml('audit_config.yaml')

# Create auditor with config settings
auditor = SecureTableAuditor(
    sample_size=config.sample_size
)

# Audit tables from config
for table in config.tables:
    results = auditor.audit_from_database(
        table_name=table,
        backend=config.backend,
        connection_params=config.connection_params,
        schema=config.schema,
        mask_pii=config.mask_pii,
        custom_pii_keywords=config.custom_pii_keywords
    )

    # Export based on config
    if 'html' in config.export_formats:
        auditor.export_results_to_html(results, f'{config.file_prefix}_{table}.html')
    if 'json' in config.export_formats:
        auditor.export_results_to_json(results, f'{config.file_prefix}_{table}.json')
    if 'csv' in config.export_formats:
        df = auditor.export_results_to_dataframe(results)
        df.write_csv(f'{config.file_prefix}_{table}.csv')
""")

    # Example 5: View audit log
    print("\n# Example 5: View audit history")
    print("-" * 60)
    print("""
auditor = SecureTableAuditor()
# ... run multiple audits ...
audit_log = auditor.get_audit_log()
print(audit_log)
""")

    print("\n" + "="*60)
    print("🔐 Security Features:")
    print("="*60)
    print("""
✅ No file exports - queries directly from database
✅ Automatic PII masking for sensitive columns
✅ Database-native sampling for large tables
✅ Audit logging with sanitized connection strings
✅ Temporary memory usage - data cleared after audit
✅ Support for custom queries to limit data exposure
""")

    print("\n📦 Required packages:")
    print("-" * 60)
    print("""
pip install -r requirements.txt

# Or install manually:
pip install polars ibis-framework[bigquery,snowflake] google-cloud-bigquery snowflake-connector-python pyyaml

# Supported databases via Ibis:
# - BigQuery (Google Cloud)
# - Snowflake
""")

    # Demo the export features
    print("\n" + "="*60)
    print("📤 Export Results Examples:")
    print("="*60)
    print("""
# Run audit
auditor = SecureTableAuditor()
results = auditor.audit_from_database(
    table_name='users',
    backend='bigquery',
    connection_params={
        'project_id': 'my-project',
        'schema': 'analytics'
    }
)

# Export to DataFrame for analysis
df = auditor.export_results_to_dataframe(results)
print(df)

# Filter by issue type
critical_issues = df.filter(
    pl.col('issue_type').is_in(['NUMERIC_STRINGS', 'TRAILING_SPACES'])
)

# Save to CSV for sharing
df.write_csv('audit_results.csv')

# Export to JSON
auditor.export_results_to_json(results, 'audit_results.json')

# Export to HTML report (beautiful, shareable)
auditor.export_results_to_html(results, 'audit_report.html')

# Get summary statistics
summary = auditor.get_summary_stats(results)
print(summary)
# Output:
# {
#   'table_name': 'users',
#   'total_rows': 1000000,
#   'analyzed_rows': 100000,
#   'sampled': True,
#   'total_issues': 15,
#   'columns_with_issues': 5,
#   'issue_breakdown': {
#     'TRAILING_SPACES': 3,
#     'CASE_DUPLICATES': 2,
#     'NUMERIC_STRINGS': 1,
#     ...
#   }
# }
""")


if __name__ == "__main__":
    main()
