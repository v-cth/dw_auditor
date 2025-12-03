# Data Warehouse Table Auditor

[![PyPI version](https://badge.fury.io/py/dw-auditor.svg)](https://pypi.org/project/dw-auditor/)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**High-performance data quality auditing for BigQuery, Snowflake & Databricks with automatic relationship detection.**

✅ Find data issues before they cause problems

🔗 Discover table relationships automatically

🎨 Beautiful HTML and CSV reports 

---

## 🚀 Quick Start

### Installation

```bash
# Install with pip
pip install dw-auditor

# Or with uv (faster)
uv pip install dw-auditor
```

### Basic Usage

```bash
# 1. Create config file
dw_auditor init

# 2. Set your credentials as environment variables (recommended)
export SNOWFLAKE_ACCOUNT='your-account'
export SNOWFLAKE_USER='your-username'
export SNOWFLAKE_PASSWORD='your-password'

# 3. Edit audit_config.yaml with your database details
# Update backend, default_database, default_schema, and tables

# 4. Run the audit
dw_auditor run

# 5. Open the HTML report
open audit_results/audit_run_*/summary.html
```

---

## ✨ Key Features

- **Quality Checks** - Detect trailing spaces, case duplicates, regex patterns, range violations, future dates, and more

- **Automatic Profiling** - Distributions, top values, quantiles, string lengths, date ranges

- **Fields With Wrong Type** - Detect string columns that contain only dates, integer, booleans ...

- **Relationship Detection** - Automatically discover foreign keys

- **Rich HTML Reports** - 4-tab interface (Summary/Insights/Checks/Metadata) with visual gradients and timelines

- **Secure by Design** - Zero data exports, database-native operations via Ibis, PII masking


---

## 📋 What You Can Audit

- **Tables & Views** - Tables, views, and materialized views
- **Multiple Schemas** - Audit across datasets/databases in one run
- **Custom Queries** - Audit filtered data (e.g., "last 7 days only")

---

## 🎯 Use Cases

- **Data Migration** - Validate data before/after migrations
- **Post-ETL Quality Gates** - Catch issues in transformation pipelines
- **Schema Discovery** - Fast metadata exploration with `--discover` mode
- **Relationship Mapping** - Understand foreign keys in legacy systems
- **Compliance Audits** - PII detection and masking for governance

---

## 📊 Example Output

### Console
```
📋 Column Summary (All Columns):
==================================================
Column Name          Type        Status      Nulls
--------------------------------------------------
user_id             int64       ✓ OK        0 (0.0%)
email               string      ✗ ERROR     2 (1.2%)
created_at          datetime    ✓ OK        0 (0.0%)

🔍 Issues Found:
⚠️  EMAIL REGEX: 2 values don't match pattern
   Examples: 'invalid.email@', 'user@domain'
```

### HTML Report Tabs
1. **Summary** - Overview, primary keys, table metadata
2. **Insights** - Visual distributions with gradient bars, top values
3. **Quality Checks** - Issues with examples and primary key context
4. **Metadata** - Audit config, duration

---

## ⚙️ Configuration Examples

### Minimal Setup
#### BigQuery
```yaml
database:
  backend: "bigquery"
  connection_params:
    default_database: "my-project"
    default_schema: "analytics"

tables:
  - name: users
  - name: orders
```

#### Snowflake
```yaml
database:
  backend: "snowflake"
  connection_params:
    default_database: "MY_DB"
    default_schema: "MY_SCHEMA"
    account: "ACCOUNT"
    user: "USER"
    password: "PWD"

tables:
  - name: users
  - name: orders
```

#### Databricks
```yaml
database:
  backend: "databricks"
  connection_params:
    default_database: "main"  # Unity Catalog name
    default_schema: "default"
    server_hostname: "${DATABRICKS_SERVER_HOSTNAME}"
    http_path: "${DATABRICKS_HTTP_PATH}"
    access_token: "${DATABRICKS_TOKEN}"

tables:
  - name: users
  - name: orders
```

### Using Environment Variables (Recommended for Credentials)

**Protect sensitive credentials by using environment variables instead of hardcoding them in YAML:**

#### Supported Formats
```yaml
database:
  backend: "snowflake"
  connection_params:
    default_database: "MY_DB"
    default_schema: "MY_SCHEMA"
    account: "${SNOWFLAKE_ACCOUNT}"              # Basic format
    user: "$SNOWFLAKE_USER"                      # Short format
    password: "${SNOWFLAKE_PASSWORD}"
    warehouse: "${SNOWFLAKE_WAREHOUSE:-COMPUTE_WH}"  # With default value
```

#### Usage

**Option 1: Using .env file (recommended)**
```bash
# Create .env file (use single quotes for passwords with special chars like $)
cat > .env << 'EOF'
export SNOWFLAKE_ACCOUNT='your-account'
export SNOWFLAKE_USER='your-username'
export SNOWFLAKE_PASSWORD='your-password'
EOF

# Load and run
source .env
dw_auditor run
```

**Option 2: Export directly**
```bash
# Set environment variables (use single quotes for special chars)
export SNOWFLAKE_ACCOUNT='OOQYWEC-ND51384'
export SNOWFLAKE_USER='my_user'
export SNOWFLAKE_PASSWORD='my_password'

# Run audit
dw_auditor run
```

**Option 3: Inline (for one-time use)**
```bash
SNOWFLAKE_PASSWORD='secret' dw_auditor run
```

### Multi-Schema Auditing
```yaml
tables:
  - name: raw_customers
    schema: raw_data
  - name: stg_customers
    schema: staging
    database: uat_retail

```

### Custom Quality Checks
```yaml
column_checks:
  tables:
    users:
      email:
        regex_patterns:
          pattern: "^[\\w._%+-]+@[\\w.-]+\\.[a-zA-Z]{2,}$"
          mode: "match"
      age:
        greater_than_or_equal: 18
        less_than: 120
```

### Relationship Detection
```yaml
relationship_detection:
  enabled: true
  confidence_threshold: 0.7   # 70% confidence to detect
  min_confidence_display: 0.5 # Show relationships >= 50%
```

**Full configuration guide**: See inline comments in [`audit_config.yaml`](./audit_config.yaml)

---

## 🔧 Advanced Usage

### Initialize Config
```bash
dw_auditor init                      # Create in current directory (./audit_config.yaml)
dw_auditor init --force              # Overwrite existing config
dw_auditor init --path ./my.yaml     # Create in custom location
```

### Run Audit
```bash
dw_auditor run                       # Auto-discover config
dw_auditor run custom.yaml           # Use specific config file
dw_auditor run --yes                 # Auto-confirm prompts
```

### Audit Modes
```bash
dw_auditor run --discover            # Metadata only (fast)
dw_auditor run --check               # Quality checks only
dw_auditor run --insight             # Profiling only
```

---

## 📚 Documentation

- **[Configuration Reference](./audit_config.yaml)** - Inline documentation for all options
- **[Quality Checks Guide](./doc/checks.md)** - All checks with examples
- **[Data Insights Guide](./doc/insights.md)** - All insights with examples



---

## 🛠️ Troubleshooting

### Installation
**PyPI**: `pip install dw-auditor` or `uv pip install dw-auditor`
**From source**: Clone repo and run `pip install -e .` or `uv sync`
**Requirements**: Python 3.10 or higher

### Authentication
**BigQuery**: Use `gcloud auth application-default login` or set `credentials_path` in config

**Snowflake**: Use environment variables for credentials (see Configuration Examples) or `authenticator: externalbrowser` for SSO

**Databricks**: Use Personal Access Token (`access_token`) or OAuth (`auth_type: databricks-oauth`) - see config template for all options

Always use environment variables for passwords - never commit credentials to git.

### Performance
- Sampling is always database-native via Ibis (fast & secure)
- Increase `sample_size` carefully (default: 100,000 rows)
- Use `--discover` for metadata-only scans

### Memory Issues
- Reduce `sample_size` in config
- Audit fewer tables per run
- Disable expensive insights (e.g., reduce `quantiles` count)

---

## 🏗️ Architecture

Built on modern Python data tools:
- **Ibis** - Database abstraction (lazy SQL generation, no data exports)
- **Polars** - Fast DataFrame processing
- **Pydantic** - Type-safe configuration validation

**Design**: All computation happens in your database. No data is exported to files.

---

## 🔐 Security Features

**Built-in security controls to protect sensitive data:**

### 1. **Automatic PII Masking**
- Auto-detects 32+ PII keywords (email, phone, SSN, credit card, etc.)
- Replaces values with `***PII_MASKED***` before analysis
- Customizable keyword list per your compliance needs

```yaml
security:
  mask_pii: true
  custom_pii_keywords: ["employee_id", "internal_code"]
```

### 2. **Zero Data Export Architecture**
- **Database-native queries** - All computation happens in your database (via Ibis)
- **No intermediate files** - Data never written to disk
- **Metadata-only exports** - Reports contain statistics, not raw data

### 3. **Data Minimization**
- **Column filtering** - Exclude sensitive columns entirely
- **Sampling** - Analyze subset of data (database-native TABLESAMPLE)
- **Temporary in-memory only** - Data discarded after analysis

### 4. **What's Exported vs Protected**

✅ **Exported** (Safe for Reports):
- Column metadata (names, types, descriptions)
- Statistics (nulls, distinct counts, ranges)
- Quality check results
- Top values (with PII masked)

❌ **Never Exported**:
- Raw column data
- Full table contents
- PII values
- Credentials (use environment variables to keep them out of config files)

---

## 📝 License

MIT License - See [LICENSE](./LICENSE) file

---

