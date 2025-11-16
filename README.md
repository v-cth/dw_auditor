# Data Warehouse Table Auditor

**High-performance data quality auditing for BigQuery & Snowflake with automatic relationship detection.**

✅ Find data issues before they cause problems
🔗 Discover table relationships automatically
🎨 Beautiful HTML reports with ER diagrams

---

## 🚀 Quick Start

```bash
# 1. Install uv (one-time setup)
curl -LsSf https://astral.sh/uv/install.sh | sh
# Or on Windows: powershell -c "irm https://astral.sh/uv/install.ps1 | iex"
# Or with pip: pip install uv

# 2. Clone and setup
git clone <your-repo>
cd database_audit

# 3. Install dependencies (creates venv automatically)
uv sync

# 4. Create config file
uv run dw_auditor init

# 5. Edit config with your database details
# Config location shown after init (OS-native path)
# Linux/Mac: ~/.config/dw_auditor/config.yaml
# Windows: %APPDATA%\dw_auditor\config.yaml

# 6. Run the audit
uv run dw_auditor run

# 7. Open the HTML report
open audit_results/audit_run_*/summary.html
```

### Why uv?
- ⚡ **10-100x faster** than pip for installs
- 🔒 **Lock file** for reproducible builds (`uv.lock`)
- 🐍 **Python version management** built-in
- 🔧 **Drop-in replacement** for pip/venv

> **Note**: If you prefer pip, you can still use: `pip install -e .`

---

## ✨ Key Features

🔍 **11 Quality Checks** - Detect trailing spaces, case duplicates, regex patterns, range violations, future dates, and more

📊 **Automatic Profiling** - Distributions, top values, quantiles, string lengths, date ranges

🔗 **Relationship Detection** - Automatically discover foreign keys and generate ER diagrams

🎨 **Rich HTML Reports** - 4-tab interface (Summary/Insights/Checks/Metadata) with visual gradients and timelines

🏢 **Enterprise Ready** - Multi-schema auditing, VIEW support, PII masking, custom queries

🔒 **Secure by Design** - Zero data exports, database-native operations via Ibis

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

### 3. **Credential Protection**
- Connection strings sanitized in logs (`user:***@host`)
- Passwords never logged or displayed
- Secure credential handling (env vars, service accounts)

### 4. **Data Minimization**
- **Column filtering** - Exclude sensitive columns entirely
- **Sampling** - Analyze subset of data (database-native TABLESAMPLE)
- **Temporary in-memory only** - Data discarded after analysis

### 5. **What's Exported vs Protected**

✅ **Exported** (Safe for Reports):
- Column metadata (names, types, descriptions)
- Statistics (nulls, distinct counts, ranges)
- Quality check results
- Top values (with PII masked)

❌ **Never Exported**:
- Raw column data
- Full table contents
- PII values
- Credentials

**Result**: Comprehensive audits without exposing sensitive data.

---

## 📋 What You Can Audit

- **Tables & Views** - Base tables, VIEWs, and MATERIALIZED VIEWs
- **Multiple Schemas** - Audit across datasets/databases in one run
- **Custom Queries** - Audit filtered data (e.g., "last 7 days only")

---

## 🎯 Use Cases

- **Data Migration** - Validate data before/after migrations
- **Post-ETL Quality Gates** - Catch issues in transformation pipelines
- **Schema Discovery** - Fast metadata exploration with `--discover` mode
- **Relationship Mapping** - Understand foreign keys in legacy systems
- **CI/CD Integration** - Automated quality checks in deployment pipelines
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
4. **Metadata** - Audit config, duration, ER diagram with relationships

---

## ⚙️ Configuration Examples

### Minimal Setup
```yaml
database:
  backend: "bigquery"
  connection_params:
    project_id: "my-project"
    schema: "analytics"

tables:
  - name: users
  - name: orders
```

### Multi-Schema Auditing
```yaml
tables:
  - name: raw_customers
    schema: raw_data
  - name: stg_customers
    schema: staging
  - name: prod_customers
    schema: production
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
dw_auditor init                      # Create in OS-native location
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
- **[Quality Checks Guide](./doc/checks.md)** - All 11 checks with examples
- **[Architecture Guide](./.claude/CLAUDE.md)** - How it works, extending checks

---

## 🛠️ Troubleshooting

### Installation
**Using uv**: Make sure uv is installed: `curl -LsSf https://astral.sh/uv/install.sh | sh`
**Using pip**: You can still install with: `pip install -e .` (reads from pyproject.toml)

### Authentication
**BigQuery**: Use `gcloud auth application-default login` or set `credentials_path` in config
**Snowflake**: Check username/password or use `authenticator: externalbrowser` for SSO

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

## 📝 License

MIT License - See [LICENSE](./LICENSE) file

---

## 🤝 Contributing

Want to add a custom check? The framework is extensible:
- Review the [developer guide](./.claude/CLAUDE.md#check-framework-architecture)
- Check existing checks in `dw_auditor/checks/`
- Follow the class-based pattern with `@register_check` decorator

Pull requests welcome!
