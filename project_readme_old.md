# Data Warehouse Table Auditor

A high-performance, security-focused data quality auditing tool for data warehouses. Built with Polars for speed and designed with security best practices.

## 🎯 Overview

This tool helps you audit unfamiliar data warehouse tables by detecting common data quality issues:

- **Trailing/leading spaces** in string columns
- **Case duplicates** (values differing only in case like "JOHN" vs "john")
- **Special characters** (emojis, accents, unexpected symbols)
- **Numeric strings** (string columns that should be numeric types)
- **Timestamp patterns** (timestamps that are effectively dates)

## 🔐 Security Features

- ✅ **Direct database querying** - No intermediate file exports
- ✅ **Automatic PII masking** - Detects and masks sensitive columns
- ✅ **Database-native sampling** - Sample in DB before loading (faster & more secure)
- ✅ **Audit logging** - Track all audits with sanitized connection strings
- ✅ **Memory safety** - Data cleared from memory after use
- ✅ **Custom queries** - Limit data exposure with WHERE clauses

## 🚀 Performance

- **Polars-powered** - Multi-threaded, SIMD-optimized operations
- **Lazy evaluation** - Query optimization and efficient execution
- **Apache Arrow** - Zero-copy data handling
- **Sampling options** - Handle tables with billions of rows
- **ConnectorX** - Fast database connections

## 📦 Installation

```bash
pip install polars connectorx pyyaml
```

### Database Support

- PostgreSQL
- MySQL
- SQLite
- SQL Server
- Oracle
- Snowflake
- And more via ConnectorX

## 🎯 Quick Start

### Option 1: YAML Configuration (Recommended)

```python
from data_auditor import SecureTableAuditor

# Load config and run batch audit
auditor = SecureTableAuditor.from_yaml('audit_config.yaml')
auditor.run_batch_audit()
```

### Option 2: Direct Database Audit

```python
from data_auditor import SecureTableAuditor

auditor = SecureTableAuditor()
results = auditor.audit_from_database(
    table_name='customers',
    connection_string='postgresql://user:pass@localhost:5432/mydb',
    schema='public',
    mask_pii=True,
    sample_in_db=True
)

# Export results
auditor.export_results_to_html(results, 'audit_report.html')
```

### Option 3: Audit from File

```python
auditor = SecureTableAuditor()

# From CSV
results = auditor.audit_from_file('data.csv', mask_pii=True)

# From Parquet
results = auditor.audit_from_file('data.parquet')
```

## 📋 YAML Configuration

Create an `audit_config.yaml` file:

```yaml
# Database connection
database:
  connection_string: "postgresql://user:password@localhost:5432/mydb"
  schema: "public"

# Tables to audit
tables:
  - customers
  - orders
  - name: recent_sales
    query: "SELECT * FROM sales WHERE date > NOW() - INTERVAL '30 days'"

# Sampling
sampling:
  sample_size: 100000
  sample_threshold: 1000000
  sample_in_db: true

# Security
security:
  mask_pii: true
  custom_pii_keywords:
    - "employee_id"
    - "confidential"

# Quality checks
checks:
  trailing_spaces: true
  case_duplicates: true
  special_characters: true
  numeric_strings: true
  timestamp_patterns: true

# Thresholds (percentages)
thresholds:
  numeric_string_pct: 80
  constant_hour_pct: 90
  midnight_pct: 95

# Output
output:
  directory: "audit_results"
  formats:
    - html
    - csv
    - json
  file_prefix: "audit"

# Column filters (optional)
filters:
  include_columns: []  # Only audit these columns
  exclude_columns: []  # Skip these columns
```

## 📤 Export Formats

### 1. HTML Report (Interactive & Beautiful)

```python
auditor.export_results_to_html(results, 'report.html')
```

Features:
- Visual dashboard with key metrics
- Color-coded issues
- Actionable suggestions
- Professional design for stakeholders

### 2. DataFrame (For Analysis)

```python
df = auditor.export_results_to_dataframe(results)

# One row per issue found
print(df)

# Filter specific issues
critical = df.filter(pl.col('issue_type') == 'NUMERIC_STRINGS')

# Save to CSV
df.write_csv('audit_results.csv')
```

### 3. JSON (For APIs/Automation)

```python
json_str = auditor.export_results_to_json(results, 'results.json')
```

### 4. Summary Statistics

```python
summary = auditor.get_summary_stats(results)
# Returns:
# {
#   'table_name': 'users',
#   'total_rows': 1000000,
#   'total_issues': 15,
#   'columns_with_issues': 5,
#   'issue_breakdown': {...}
# }
```

## 🔍 Quality Checks

### 1. Trailing/Leading Spaces

Detects whitespace that shouldn't be there:
```
'John '  ← trailing space
' Jane'  ← leading space
```

### 2. Case Duplicates

Finds values differing only in case:
```
'john' vs 'JOHN' vs 'John'
```

### 3. Special Characters

Identifies unexpected characters:
```
'Café' ← accent
'😀'   ← emoji
'Test™' ← symbol
```

### 4. Numeric Strings

String columns that should be numeric:
```
Column: ['100', '200', '150', '300']
Suggestion: Convert to INT/FLOAT
```

### 5. Timestamp Patterns

Timestamps that are effectively dates:
```
All timestamps at 00:00:00 → Use DATE type
All timestamps at same hour → Date-only data
```

## ⚙️ Configuration Options

### Sampling

```python
auditor = SecureTableAuditor(
    sample_size=50000,        # Sample 50K rows
    sample_threshold=500000   # Sample if >500K rows
)
```

### Security

```python
# Custom PII keywords
config = {
    'security': {
        'mask_pii': True,
        'custom_pii_keywords': ['internal_id', 'proprietary']
    }
}
```

### Thresholds

```python
# Adjust detection sensitivity
config = {
    'thresholds': {
        'numeric_string_pct': 95,  # Very strict
        'constant_hour_pct': 99,
        'midnight_pct': 99
    }
}
```

### Column Filters

```python
# Audit only specific columns
config = {
    'filters': {
        'include_columns': ['email', 'phone', 'address']
    }
}

# Or exclude system columns
config = {
    'filters': {
        'exclude_columns': ['id', 'created_at', 'updated_at']
    }
}
```

## 📊 Batch Auditing

Audit multiple tables at once:

```python
auditor = SecureTableAuditor.from_yaml('audit_config.yaml')
all_results = auditor.run_batch_audit()

# Automatically:
# - Audits all configured tables
# - Applies security settings
# - Exports in configured formats
# - Creates summary report
```

## 🔒 PII Masking

Automatically masks columns with keywords:

**Default keywords:**
- SSN, tax_id, national_id
- Credit_card, cvv, card_number
- Password, secret, token, api_key
- Email, phone, mobile, telephone
- Address, street, zip, postal
- Passport, license, drivers
- Account_number, routing
- DOB, date_of_birth, birthdate
- Salary, wage, income, compensation

**Add custom keywords:**
```yaml
security:
  custom_pii_keywords:
    - "employee_id"
    - "customer_number"
    - "confidential"
```

## 📁 Project Structure

```
project/
├── data_auditor.py          # Main auditor class
├── audit_config.yaml        # Configuration template
├── audit_results/           # Output directory
│   ├── audit_customers_20241009_143022.html
│   ├── audit_customers_20241009_143022.csv
│   ├── audit_orders_20241009_143025.html
│   └── audit_SUMMARY_20241009_143030.csv
└── README.md
```

## 🎯 Use Cases

### 1. New Data Warehouse Exploration

```python
# Quickly understand unfamiliar tables
auditor = SecureTableAuditor.from_yaml('config.yaml')
auditor.run_batch_audit()
```

### 2. Data Quality Monitoring

```python
# Regular audits for quality checks
# Schedule with cron/airflow
auditor.audit_from_database('daily_data', conn_string)
```

### 3. Migration Validation

```python
# Validate data after ETL
auditor.audit_from_database('migrated_table', conn_string)
```

### 4. Compliance Audits

```python
# Ensure PII is properly handled
config = {'security': {'mask_pii': True}}
auditor = SecureTableAuditor(config=AuditConfig(config))
```

## 🔧 Advanced Usage

### Custom Query for Security

```python
# Limit data exposure with WHERE clause
results = auditor.audit_from_database(
    table_name='sensitive_data',
    connection_string=conn_string,
    custom_query="""
        SELECT * FROM sensitive_data 
        WHERE department = 'public'
        AND created_at > NOW() - INTERVAL '7 days'
    """
)
```

### Programmatic Configuration

```python
config_dict = {
    'database': {'connection_string': 'postgresql://...'},
    'tables': ['users', 'orders'],
    'security': {'mask_pii': True},
    'output': {'formats': ['html', 'csv']}
}

config = AuditConfig(config_dict)
auditor = SecureTableAuditor(config=config)
results = auditor.run_batch_audit()
```

### Audit History

```python
auditor = SecureTableAuditor()

# Run multiple audits
auditor.audit_from_database('table1', conn_string)
auditor.audit_from_database('table2', conn_string)

# View audit log
log = auditor.get_audit_log()
for entry in log:
    print(f"{entry['timestamp']}: {entry['table']}")
```

## 📈 Performance Tips

1. **Use database sampling** for large tables (>1M rows)
2. **Use Parquet** for file-based audits (much faster than CSV)
3. **Filter columns** to audit only what you need
4. **Custom queries** to reduce data volume at source
5. **Batch audits** are more efficient than individual runs

## 🐛 Troubleshooting

### Connection Issues

```python
# Test connection first
import connectorx as cx
df = cx.read_sql("SELECT 1", connection_string)
```

### Memory Issues

```python
# Reduce sample size
auditor = SecureTableAuditor(sample_size=10000)
```

### Slow Performance

```python
# Enable database sampling
config = {'sampling': {'sample_in_db': True}}
```

## 📚 API Reference

### SecureTableAuditor

**Constructor:**
```python
SecureTableAuditor(
    sample_size=100000,
    sample_threshold=1000000,
    config=None
)
```

**Methods:**
- `from_yaml(yaml_path)` - Create from YAML config
- `audit_from_database(table_name, connection_string, ...)` - Audit from DB
- `audit_from_file(file_path, ...)` - Audit from CSV/Parquet
- `audit_table(df, table_name)` - Audit Polars DataFrame
- `run_batch_audit()` - Audit all tables in config
- `export_results_to_dataframe(results)` - Export to Polars DataFrame
- `export_results_to_json(results, file_path)` - Export to JSON
- `export_results_to_html(results, file_path)` - Export to HTML
- `get_summary_stats(results)` - Get summary statistics
- `get_audit_log()` - Get audit history

### AuditConfig

**Constructor:**
```python
AuditConfig(config_dict)
```

**Methods:**
- `from_yaml(yaml_path)` - Load from YAML file
- `to_dict()` - Convert to dictionary

## 🤝 Contributing

Contributions welcome! Areas for enhancement:
- Additional data quality checks
- More database connectors
- Enhanced PII detection
- Custom check plugins
- Performance optimizations

## 📄 License

[Your License Here]

## 🙏 Acknowledgments

Built with:
- [Polars](https://www.pola.rs/) - Fast DataFrame library
- [ConnectorX](https://github.com/sfu-db/connector-x) - Fast database connector
- [PyYAML](https://pyyaml.org/) - YAML parser

## 📞 Support

For issues or questions:
- Check the troubleshooting section
- Review the examples in audit_config.yaml
- Enable debug logging for detailed error messages

---

**Happy Auditing! 🎉**