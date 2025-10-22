# Architecture Documentation

## System Overview

The Data Warehouse Table Auditor is built on a **layered architecture** with clear separation between data access, business logic, and presentation layers.

```
┌─────────────────────────────────────────────────────────────┐
│                     Presentation Layer                       │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐   │
│  │   CLI    │  │   HTML   │  │   JSON   │  │   CSV    │   │
│  │ (audit.py│  │ Exporter │  │ Exporter │  │ Exporter │   │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘   │
└───────┼─────────────┼─────────────┼─────────────┼──────────┘
        │             │             │             │
┌───────┼─────────────┼─────────────┼─────────────┼──────────┐
│       ▼             ▼             ▼             ▼           │
│              ExporterMixin (core/exporter_mixin.py)         │
└─────────────────────────┬───────────────────────────────────┘
                          │
┌─────────────────────────┼───────────────────────────────────┐
│                         ▼         Business Logic Layer       │
│           SecureTableAuditor (core/auditor.py)               │
│  ┌─────────────┐  ┌──────────────┐  ┌──────────────┐       │
│  │   Checks    │  │   Insights   │  │  Metadata    │       │
│  │ (checks/)   │  │ (insights/)  │  │  Collection  │       │
│  └──────┬──────┘  └──────┬───────┘  └──────┬───────┘       │
└─────────┼─────────────────┼──────────────────┼──────────────┘
          │                 │                  │
┌─────────┼─────────────────┼──────────────────┼──────────────┐
│         ▼                 ▼                  ▼               │
│                   Data Access Layer                          │
│         DatabaseConnection (core/database.py)                │
│                    ┌─────────┐                               │
│                    │  Ibis   │                               │
│                    └────┬────┘                               │
└─────────────────────────┼───────────────────────────────────┘
                          │
          ┌───────────────┼───────────────┐
          ▼               ▼               ▼
    ┌──────────┐   ┌──────────┐   ┌──────────┐
    │ BigQuery │   │Snowflake │   │PostgreSQL│
    └──────────┘   └──────────┘   └──────────┘
```

## Core Components

### 1. Configuration Layer (`core/config.py`)

**Purpose**: Parse and validate YAML configuration

**Key Classes**:
- `AuditConfig`: Central configuration object

**Responsibilities**:
- Load YAML configuration from file
- Provide typed access to configuration values
- Validate configuration structure
- Manage default values

**Design Pattern**: Builder pattern with `from_yaml()` factory method

```python
config = AuditConfig.from_yaml("audit_config.yaml")
# Provides: database settings, table list, check configuration,
#           sampling strategy, output preferences
```

### 2. Data Access Layer (`core/database.py`)

**Purpose**: Abstract database connections and queries

**Key Classes**:
- `DatabaseConnection`: Ibis connection wrapper

**Responsibilities**:
- Establish secure database connections
- Execute Ibis queries
- Handle connection lifecycle (open/close)
- Sanitize connection strings for logging

**Design Pattern**: Adapter pattern (wraps Ibis backend connections)

**Key Feature**: **Lazy evaluation** - Builds SQL expressions without executing until explicitly requested

```python
conn = DatabaseConnection(backend="bigquery", **params)
table = conn.get_table("my_table")
# No query executed yet - just creates Ibis expression

df = table.limit(1000).to_polars()  # NOW executes query
```

### 3. Business Logic Layer (`core/auditor.py`)

**Purpose**: Orchestrate the audit process

**Key Classes**:
- `SecureTableAuditor`: Main audit orchestrator

**Responsibilities**:
- Coordinate data loading (with sampling)
- Execute quality checks (via `checks/` modules)
- Generate insights (via `insights/` modules)
- Collect table metadata
- Track timing and performance metrics
- Detect primary keys

**Design Pattern**: Facade pattern (provides simple interface to complex subsystem)

**Audit Flow**:
```
1. Connect to database
2. Get table metadata (type, creation time, row count)
3. Detect/configure primary key
4. Sample data if needed (database-native TABLESAMPLE)
5. Load data into Polars DataFrame
6. Run checks module
7. Generate insights module
8. Collect results into structured dict
9. Close connection
```

### 4. Checks Module (`checks/`)

**Purpose**: Validate data quality

**Key Modules**:
- `string_checks.py`: String validation (trailing spaces, case duplicates, special chars, numeric strings)
- `timestamp_checks.py`: Timestamp validation (constant hour, midnight detection, outliers)

**Responsibilities**:
- Detect data quality issues
- Return structured issue reports with examples
- Respect column-level configuration (skip checks when configured)

**Design Pattern**: Strategy pattern (each check is an independent strategy)

**Return Format**:
```python
{
    "column_name": {
        "issues": [
            {
                "type": "trailing_spaces",
                "count": 42,
                "percentage": 2.1,
                "examples": ["value1  ", "value2 "]
            }
        ]
    }
}
```

### 5. Insights Module (`insights/`)

**Purpose**: Profile data and generate statistics

**Key Modules**:
- `column_insights.py`: Orchestrator that routes to type-specific insights
- `string_insights.py`: String profiling (top values, length stats)
- `numeric_insights.py`: Numeric profiling (min/max/mean/median/quantiles)
- `datetime_insights.py`: DateTime profiling (date ranges, common dates/hours)

**Responsibilities**:
- Calculate descriptive statistics
- Find patterns and distributions
- Generate profiling data for visualization

**Design Pattern**: Strategy pattern + Facade pattern

**Key Principle**: **Insights are separate from checks** - profiling doesn't imply quality issues

**Return Format**:
```python
{
    "column_name": {
        "min": 0,
        "max": 1000,
        "mean": 450.5,
        "median": 500,
        "quantiles": {"p25": 250, "p50": 500, "p75": 750},
        "top_values": [{"value": "foo", "count": 100, "percentage": 10.0}]
    }
}
```

### 6. Exporter Layer (`exporters/`)

**Purpose**: Transform audit results into various output formats

**Key Modules**:
- `html/` (modular HTML export package):
  - `export.py`: Main orchestration (75 lines)
  - `structure.py`: Page structure components (232 lines)
  - `insights.py`: Column insights rendering (748 lines)
  - `checks.py`: Quality checks section (181 lines)
  - `assets.py`: CSS styles and JavaScript (371 lines)
- `json_export.py`: Export to JSON format
- `dataframe_export.py`: Convert to Polars DataFrame
- `summary_export.py`: Generate summary CSV files
- `run_summary_export.py`: Multi-table summary reports

**Responsibilities**:
- Transform structured audit results into presentation formats
- Apply visual styling (HTML)
- Handle file I/O
- Format numbers and dates for display

**Design Pattern**: Strategy pattern (each exporter is interchangeable)

**Mixin Pattern**: `ExporterMixin` provides export methods to `SecureTableAuditor`

**Modular Architecture**: HTML export split into focused modules (~270 lines avg) for better maintainability

## Data Flow Patterns

### Pattern 1: Database-Native Sampling

**Problem**: Large tables (billions of rows) can't be loaded entirely into memory

**Solution**: Push sampling to database using `TABLESAMPLE` (BigQuery) or `SAMPLE` (Snowflake)

```python
# Ibis expression (lazy)
table_expr = connection.table(table_name)

if should_sample:
    # This generates: SELECT * FROM table TABLESAMPLE SYSTEM (10 PERCENT)
    table_expr = table_expr.sample(fraction=sample_fraction)

# Execute and load into memory
df = table_expr.to_polars()
```

**Benefit**: Database handles sampling efficiently; only sampled data transfers over network

### Pattern 2: Lazy Configuration Resolution

**Problem**: User config may be incomplete; need sensible defaults

**Solution**: Three-tier fallback for settings (e.g., primary key detection)

```
1. User explicit config (audit_config.yaml)
   ↓ (if not found)
2. INFORMATION_SCHEMA metadata
   ↓ (if not found)
3. Auto-detection heuristics (unique non-null columns)
```

### Pattern 3: Separation of Checks and Insights

**Problem**: Mixing quality checks with profiling creates confusion

**Solution**: Two independent pipelines with different semantics

**Checks**:
- Binary: "Is there a problem?" (yes/no)
- Threshold-based: Flag if percentage exceeds limit
- Generate issues list
- User can configure which checks run

**Insights**:
- Descriptive: "What does the data look like?"
- Always generated (if data type matches)
- Generate statistics
- User can configure level of detail

### Pattern 4: HTML Generation with Inline CSS

**Problem**: HTML reports must be portable (single file, no external dependencies)

**Solution**: Generate HTML with all CSS inline using f-strings

```python
def _render_numeric_insights(insights, thousand_separator, decimal_places):
    html = f"""
    <div style="background: white; padding: 12px; border-radius: 6px;">
        <div style="position: relative; height: 70px;">
            <div style="position: absolute; top: 30px; left: 0; right: 0;
                        height: 8px; background: linear-gradient(...); ">
            </div>
            <div style="position: absolute; top: 35px; left: {pos}%;">
                {label}
            </div>
        </div>
    </div>
    """
    return html
```

**Benefits**:
- Single file portability
- No build step
- No CSS framework dependencies
- Easy to modify in code

## Module Interactions

### Audit Execution Sequence

```
audit.py (CLI entry point)
  │
  ├─→ AuditConfig.from_yaml()
  │     └─→ Parse YAML, validate structure
  │
  ├─→ SecureTableAuditor.__init__()
  │     └─→ Store configuration
  │
  └─→ For each table in config.tables:
        │
        ├─→ SecureTableAuditor.audit_table()
        │     │
        │     ├─→ DatabaseConnection.connect()
        │     │     └─→ Ibis backend connection
        │     │
        │     ├─→ Get table metadata (row count, creation time, type)
        │     │
        │     ├─→ Detect primary key (config → INFORMATION_SCHEMA → auto)
        │     │
        │     ├─→ Load data (with sampling if needed)
        │     │     └─→ table_expr.sample().to_polars()
        │     │
        │     ├─→ Run checks for each column
        │     │     ├─→ string_checks.check_trailing_spaces()
        │     │     ├─→ string_checks.check_case_duplicates()
        │     │     ├─→ timestamp_checks.check_constant_hour()
        │     │     └─→ ... (other checks)
        │     │
        │     ├─→ Generate insights for each column
        │     │     ├─→ column_insights.get_column_insights()
        │     │     │     ├─→ string_insights.get_string_insights()
        │     │     │     ├─→ numeric_insights.get_numeric_insights()
        │     │     │     └─→ datetime_insights.get_datetime_insights()
        │     │
        │     └─→ Return structured results dict
        │
        ├─→ ExporterMixin.export_results_to_html()
        │     └─→ html_export.export_to_html()
        │           ├─→ _generate_header()
        │           ├─→ _generate_metadata_cards()
        │           ├─→ _generate_column_summary_table()
        │           ├─→ _generate_column_insights()
        │           │     ├─→ _render_string_insights()
        │           │     ├─→ _render_numeric_insights()
        │           │     └─→ _render_datetime_insights()
        │           └─→ _generate_issues_section()
        │
        ├─→ ExporterMixin.export_results_to_json()
        │
        └─→ ExporterMixin.export_results_to_dataframe()
```

## Security Architecture

### PII Protection Strategy

**Threat Model**: Prevent accidental exposure of PII in audit reports

**Implementation**: Three-layer defense

1. **Column Name Detection** (`utils/security.py`)
   - Scan column names for PII keywords (email, ssn, phone, etc.)
   - User can add custom keywords in config

2. **Automatic Masking**
   - Mask values in detected PII columns: `"john@example.com"` → `"***MASKED***"`
   - Apply to checks, insights, and exports

3. **No File Export**
   - Query database directly via Ibis
   - Data stays in memory (Polars DataFrame)
   - Never write unaudited data to disk

### Connection Security

**Sanitized Logging**:
```python
# Bad: "bigquery://project-123?credentials=/path/to/secret.json"
# Good: "bigquery://project-123?credentials=***REDACTED***"
```

**Least Privilege**:
- Use read-only database credentials
- Never require write permissions
- Sample in database (no table creation)

## Performance Considerations

### Bottlenecks and Optimizations

**1. Network I/O (Database → Client)**
- **Optimization**: Database-native sampling reduces data transfer
- **Example**: 1B row table × 10% sample = 100M rows transferred instead of 1B

**2. Memory Usage**
- **Challenge**: Large DataFrames in memory
- **Mitigation**: Configurable sample sizes; streaming for future enhancement

**3. Check Execution**
- **Pattern**: Run all checks on same DataFrame (no re-querying)
- **Optimization**: Polars vectorized operations (faster than pandas)

**4. HTML Generation**
- **Pattern**: String concatenation with f-strings (fast)
- **Alternative considered**: Template engines (rejected for simplicity)

### Timing Instrumentation

All major phases are timed:
```python
{
    "Connection": 1.2,
    "Metadata": 0.5,
    "Data Loading": 3.1,
    "Audit Checks": 0.8,
    "Total Table Audit": 5.6
}
```

## Extension Points

### Adding a New Database Backend

1. Ensure Ibis supports it: https://ibis-project.org/backends/
2. Add connection parameters to `audit_config.yaml`
3. Update `DatabaseConnection.connect()` with new backend case
4. Test sampling behavior (some backends differ)

### Adding a New Check

1. Create function in appropriate `checks/` module
2. Add config option to `audit_config.yaml`
3. Call from `SecureTableAuditor.audit_table()`
4. Add HTML rendering in `html_export.py`

### Adding a New Insight Type

1. Create function in appropriate `insights/` module
2. Add config option to `column_insights.defaults`
3. Update `get_column_insights()` to call new function
4. Add HTML rendering in `html_export.py`

### Adding a New Export Format

1. Create new exporter in `exporters/`
2. Add method to `ExporterMixin`
3. Call from `audit.py` if in config.output.formats

## Technology Decisions

### Why Ibis?

**Alternatives Considered**: pandas with SQLAlchemy, direct SQL

**Decision**: Ibis

**Rationale**:
- Lazy evaluation (builds expressions, executes once)
- Database-agnostic (same code for BigQuery/Snowflake)
- Native sampling support (TABLESAMPLE)
- Type safety (harder to create SQL injection)
- Composable expressions (easier to build dynamic queries)

### Why Polars?

**Alternatives Considered**: pandas, DuckDB

**Decision**: Polars

**Rationale**:
- Faster than pandas (2-10x for common operations)
- Better memory efficiency (Apache Arrow)
- Expressive API (similar to pandas but more consistent)
- Native support for complex types (List, Struct)
- Active development and growing ecosystem

### Why Inline CSS in HTML Reports?

**Alternatives Considered**: External CSS file, CSS framework (Bootstrap/Tailwind)

**Decision**: Inline CSS

**Rationale**:
- **Portability**: Single HTML file, email-friendly
- **No dependencies**: Works offline, no CDN required
- **Simple deployment**: No build step
- **Easy debugging**: See all styles in one file
- **Performance**: No extra HTTP requests

---

**Last Updated**: October 21, 2025
