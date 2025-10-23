# Data Warehouse Table Auditor - Claude Code Guide

## Project Overview

A high-performance data warehouse auditing tool that performs data quality checks and profiling on BigQuery and Snowflake tables using Ibis for secure, direct database access without file exports.

**Primary Language**: Python 3.10+
**Key Framework**: Ibis (database abstraction)
**Main Use Case**: Audit data warehouse tables for quality issues and generate insights

## Quick Start for Claude

### Running the Auditor
```bash
source audit_env/bin/activate && python audit.py                    # Use default audit_config.yaml
source audit_env/bin/activate && python audit.py custom_config.yaml # Use custom config
source audit_env/bin/activate && python audit.py --discover          # Discovery mode (metadata only)
```

### Key Entry Points
- **`audit.py`**: Main CLI script - starts here for user-facing audits
- **`dw_auditor/core/auditor.py`**: `SecureTableAuditor` class - core audit logic
- **`dw_auditor/exporters/html/`**: HTML report generation (modular package with 5 files)

### Recent Development Focus

**Completed** (October 2025):
1. ✅ Modularized HTML export (1,558 lines → 5 focused modules)
2. ✅ Minimalist redesign: 4-tab structure, no emojis, cleaner UI
3. ✅ Visual distribution ranges for numeric columns (gradient bars)
4. ✅ Visual timeline bars for date ranges
5. ✅ Configurable number formatting (thousand separator + decimal places)

## Architecture Philosophy

### Core Principles
1. **Security First**: Never export data to files; PII masking; secure connection handling
2. **Database-Native Operations**: Push computation to the database (Ibis expressions)
3. **Separation of Concerns**:
   - Quality checks (`checks/`) are separate from profiling (`insights/`)
   - Exporters (`exporters/`) are modular and independent
4. **Configuration-Driven**: YAML config controls all behavior
5. **Visual-First Reporting**: HTML reports use inline CSS and visual elements (no external dependencies)

### Data Flow
```
audit.py
  → AuditConfig.from_yaml()
  → SecureTableAuditor
    → DatabaseConnection (Ibis)
      → Query execution (database-native sampling)
      → Checks (string_checks, timestamp_checks)
      → Insights (numeric_insights, datetime_insights, string_insights)
    → ExporterMixin
      → HTML/JSON/CSV exports
```

## Key Conventions

### File Organization
- **`core/`**: Fundamental classes (auditor, config, database connection)
- **`checks/`**: Data quality validation functions
- **`insights/`**: Data profiling and statistics functions
- **`exporters/`**: Output format generators (HTML, JSON, CSV)
- **`utils/`**: Helper functions (security, output formatting)

### Naming Patterns
- **Functions**: `snake_case` (e.g., `get_column_insights`)
- **Classes**: `PascalCase` (e.g., `SecureTableAuditor`)
- **Private functions**: Prefix with `_` (e.g., `_render_numeric_insights`)
- **Config keys**: `snake_case` in YAML (e.g., `thousand_separator`)

### Code Style
- **Type hints**: Used throughout for function signatures
- **Docstrings**: Google style with Args/Returns sections
- **Imports**: Grouped (stdlib, third-party, local) with blank lines between
- **HTML generation**: f-strings with triple-quoted strings for readability

### HTML Report Conventions
- **Minimalist design**: Clean typography, no emojis, Inter font
- **Four-tab structure**: Summary → Insights → Quality Checks → Metadata
- **Inline CSS**: All styles inline (no external files for portability)
- **Color palette**:
  - Purple (`#6606dc`): Primary accent
  - Green (`#10b981`): Success/high frequency
  - Orange (`#f59e0b`): Mean/average markers
  - Red (`#ef4444`): Errors/issues
  - Gray (`#4b5563`, `#1f2937`): Text and labels
- **Visual elements**: CSS gradients and absolute positioning for charts

## Configuration (`audit_config.yaml`)

### Most Important Sections
1. **`database`**: Connection settings (backend, project_id, dataset_id)
2. **`tables`**: Tables to audit (with optional custom queries)
3. **`column_insights`**: Profiling configuration per data type
4. **`output.number_format`**: Display formatting (NEW: thousand_separator, decimal_places)

### Config Access Pattern
```python
config = AuditConfig.from_yaml("audit_config.yaml")
config.sample_size           # Sampling configuration
config.output_dir            # Where to save results
config.column_insights       # Profiling settings
```

## Common Development Tasks

### Adding a New Visual Element to HTML Reports

1. **Find the renderer function** in `dw_auditor/exporters/html/insights.py`:
   - Numeric: `_render_numeric_insights()`
   - DateTime: `_render_datetime_insights()`
   - String: `_render_string_insights()`

2. **Add HTML with inline CSS**:
   ```python
   html += f"""
       <div style="position: relative; height: 50px;">
           <div style="position: absolute; top: 20px; left: {position}%; ...">
               {content}
           </div>
       </div>
   """
   ```

3. **Pass config parameters** through the function chain:
   - Renderer → `_generate_column_insights()` → `export_to_html()` in `export.py`

### Adding a New Configuration Option

1. **Update `audit_config.yaml`** with the new setting and comments
2. **Update `dw_auditor/core/config.py`** if needed (for complex validation)
3. **Pass through** the relevant function chain
4. **Document** in config comments and examples

### Testing the HTML Output

```bash
# Run audit to generate HTML
python audit.py

# HTML files generated in:
# audit_results/audit_run_TIMESTAMP/TABLE_NAME/audit.html
# audit_results/audit_run_TIMESTAMP/summary.html
```

## Tech Stack

### Core Dependencies
- **Ibis**: Database abstraction layer (SQL generation)
- **Polars**: High-performance DataFrames (used post-query)
- **PyYAML**: Configuration parsing
- **google-cloud-bigquery**: BigQuery backend (via Ibis)
- **snowflake-connector-python**: Snowflake backend (via Ibis)

### Why Ibis?
- Lazy evaluation: Builds SQL expressions without executing
- Database-native sampling: `TABLESAMPLE` in BigQuery, `SAMPLE` in Snowflake
- Type safety: Strong typing prevents SQL injection
- No data export: Queries run in database, results stream directly

### Why Polars (not Pandas)?
- Faster for large datasets
- Better memory efficiency
- Expressive API for transformations
- Native Arrow format

## Known Gotchas

### HTML Report Generation
1. **Label overlap**: Use vertical stacking (`v_offset`) when labels cluster
2. **Long numbers**: Always use `format_number()` helper for thousand separators
3. **Container overflow**: Set `overflow: hidden` on positioned containers
4. **Gradient colors**: Use 5+ stops for smooth visual transitions

### Ibis Queries
1. **Lazy evaluation**: Must call `.execute()` or `.to_polars()` to run query
2. **Type conversions**: Some Ibis types differ from Polars types (check schema)
3. **Sampling**: Use `sample(fraction=)` not `limit()` for statistical validity

### Config Management
1. **Nested dicts**: Access with `.get()` chains or use getattr patterns
2. **Default values**: Always provide defaults in function signatures
3. **Type safety**: Config values are raw Python types, not validated objects

## Recent Changes (October 2025)

### October 23: Modularization & Minimalist Redesign
- **Refactored HTML export**: Split 1,558-line file into 5 focused modules
- **New structure**: `html/` package with `export.py`, `structure.py`, `insights.py`, `checks.py`, `assets.py`
- **Minimalist design**: Removed all emojis, cleaner UI, professional typography
- **Four tabs**: Summary (with column summary) → Insights → Quality Checks → Metadata
- **Layout refinements**: Duration moved to Metadata tab

### October 19-21: Visual Enhancements
- Visual gradient bars for numeric distributions (min/max/Q1/Q2/Q3/mean)
- Timeline bars for date ranges with duration badges
- Configurable number formatting (thousand separator, decimal places)
- Smart label combining and vertical stacking to prevent overlap

## Getting Help

### Understanding Existing Code
1. **Start with `audit.py`**: Follow the execution path from CLI
2. **Check docstrings**: All major functions have detailed docstrings
3. **Look at config**: `audit_config.yaml` shows all available options
4. **Run with example**: Use the BigQuery public crypto dataset (already configured)

### Making Changes
1. **Run existing audit first**: See current behavior
2. **Make small changes**: Test incrementally
3. **Check HTML output**: Visual bugs are easy to spot in browser
4. **Update config docs**: Keep YAML comments in sync with code

---

**Last Updated**: October 23, 2025
**Maintained for**: Claude Code (AI pair programming assistant)
