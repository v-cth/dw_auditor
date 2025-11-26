Prefer a modular, extensible architecture that avoids unnecessary coupling between components.
Prefer simplicity and avoid over-engineering.
Communicate clearly and concisely, prioritizing relevant information and avoiding verbosity.
Adhere to industry best practices, including clean code principles, consistent naming conventions, and maintainable design patterns.
I don't need backward compatibility. If you thing you have to do it, first ask me.

# Data Warehouse Table Auditor

High-performance data warehouse auditing tool for BigQuery, Snowflake, and Databricks using Ibis for secure, direct database access.

**Stack**: Python 3.10+ | Ibis | Polars | Pydantic

## Quick Start

```bash
source audit_env/bin/activate && python audit.py                    # Default config
source audit_env/bin/activate && python audit.py custom_config.yaml # Custom config
source audit_env/bin/activate && python audit.py --discover          # Metadata only
```

## Architecture

**Data Flow**: `audit.py` → `AuditConfig` → `SecureTableAuditor` → `DatabaseConnection` (Ibis) → Check Framework + Insights → Exporters (HTML/JSON/CSV)

**File Organization**:
- `audit.py`: Main CLI entry point
- `dw_auditor/core/`: Fundamental classes (auditor, config, base_check, base_insight, type_converter, registries, runners)
- `dw_auditor/checks/`: 11 data quality check classes (auto-registered)
- `dw_auditor/insights/`: Insight classes (atomic + type-specific composites)
- `dw_auditor/exporters/html/`: 5-file modular HTML generator

**Core Principles**:
1. Security first: No data exports, PII masking, secure connections
2. Database-native: Push computation to database via Ibis
3. Configuration-driven: YAML controls behavior
4. Separation of concerns: Checks vs insights vs exporters

## Check Framework (Class-Based)

**Architecture**: Abstract `BaseCheck` + dynamic registry + async-ready runner

**Core Components**:
- `core/base_check.py`: Base class with unified interface (`run()`, `_validate_params()`)
- `core/registry.py`: `@register_check(name)` decorator for auto-registration
- `core/runner.py`: `run_check_sync()` execution API

**Available Checks** (11 total):
- **String** (5): trailing_characters, ending_characters, case_duplicates, regex_pattern, numeric_strings
- **Timestamp** (4): timestamp_patterns, date_range, date_outliers, future_dates
- **Numeric** (1): numeric_range
- **Universal** (1): uniqueness

**Adding a Check**:
```python
from dw_auditor.core.base_check import BaseCheck, CheckResult
from dw_auditor.core.registry import register_check

@register_check("my_check")
class MyCheck(BaseCheck):
    display_name = "My Check"

    def _validate_params(self):
        self.config = MyCheckParams(**self.params)  # Pydantic validation

    async def run(self) -> List[CheckResult]:
        # Check logic
        return results
```

Then import in `checks/__init__.py` and use: `run_check_sync('my_check', df, col, pk_cols, ...)`

## Insights Framework (Class-Based)

**Architecture**: Hybrid approach with atomic insights + type-specific composites (similar to Check Framework)

**Core Components**:
- `core/base_insight.py`: Base class with unified interface (`generate()`, `_validate_params()`)
- `core/insight_registry.py`: `@register_insight(name)` decorator for auto-registration
- `core/insight_runner.py`: `run_insight_sync()` execution API

**Atomic Insights** (Reusable across types):
- `top_values`: Most frequent values (universal - works on all types)
- `quantiles`: Statistical percentiles (numeric only)
- `length_stats`: Min/max/avg string length (string only)

**Type-Specific Composites** (4 total):
- **Numeric** (`numeric_insights`): min, max, mean, std, quantiles, top_values
- **String** (`string_insights`): top_values, length_stats
- **Datetime** (`datetime_insights`): min_date, max_date, date_range_days, most_common_dates/hours/days, timezone
- **Boolean** (`boolean_insights`): boolean_distribution (True/False/Null)

**Adding an Insight**:
```python
from dw_auditor.core.base_insight import BaseInsight, InsightResult
from dw_auditor.core.insight_registry import register_insight

@register_insight("my_insight")
class MyInsight(BaseInsight):
    display_name = "My Insight"
    supported_dtypes = [pl.Int64, pl.Float64]  # Empty = universal

    def _validate_params(self):
        self.config = MyInsightParams(**self.params)  # Pydantic validation

    async def generate(self) -> List[InsightResult]:
        # Insight logic
        return [InsightResult(type='my_metric', value=123)]
```

Then import in `insights/__init__.py` or `column_insights.py` and use: `run_insight_sync('my_insight', df, col, **params)`

**Key Differences from Checks**:
- Returns `List[InsightResult]` (measurements) vs `List[CheckResult]` (violations)
- No primary key context needed (insights are aggregates, not row-level)
- HTML exporter has helper function `_insights_to_dict()` to convert List[InsightResult] to Dict for rendering

## Type Converter (Optimized Two-Phase)

**Architecture**: Class-based converter with intelligent sampling strategy

**Location**: `core/type_converter.py`

**Strategy**:
1. **Phase 1 - Sample Test** (5% random sample):
   - Quick validation on small data subset
   - 90% success threshold required to proceed
   - Avoids expensive full conversions when unlikely to succeed
2. **Phase 2 - Full Conversion** (if sample passes):
   - Convert entire column
   - Verify 95% success threshold
   - Apply conversion only if verification passes

**Conversion Order**: int64 → float64 → date → datetime

**Usage**:
```python
from dw_auditor.core.type_converter import TypeConverter

converter = TypeConverter(
    sample_threshold=0.90,    # Sample must be 90% successful
    full_threshold=0.95,      # Full conversion must be 95% successful
    sample_fraction=0.05      # Test on 5% of data first
)
df, conversion_log = converter.convert_dataframe(df)
```

**Performance**:
- **30-50% faster** than single-pass conversion
- Early exit when sample fails (skips expensive datetime parsing on non-date columns)
- Optimized null counting: `is_not_null().sum()` instead of `drop_nulls().len()`
- Random sampling ensures statistical validity

**Configuration**:
- `sample_threshold`: Minimum success rate for sample test (default: 0.90)
- `full_threshold`: Minimum success rate for full conversion (default: 0.95)
- `sample_fraction`: Fraction of rows to sample (default: 0.05)

**Conversion Log Format**:
```python
{
    'column': str,
    'from_type': 'string',
    'to_type': str,  # 'int64', 'float64', 'datetime', 'date'
    'success_rate': float,
    'converted_values': int
}
```

## Primary Key Detection

**Logic** (`auditor.py:660`): Auto-detects columns where `distinct_count == analyzed_rows` AND `null_count == 0`

**Usage**:
- Console output with indicator
- HTML reports (Summary tab)
- JSON exports (`potential_primary_keys` field)
- Config-defined PKs (`tables[].primary_key`) take precedence

## Key Conventions

**Naming**: `snake_case` functions, `PascalCase` classes, `_prefix` for private
**Docs**: Google-style docstrings with Args/Returns
**HTML**: Inline CSS, 4-tab structure (Summary → Insights → Quality Checks → Metadata), minimalist design
**Colors**: Purple (#6606dc) primary, Green (#10b981) success, Orange (#f59e0b) mean, Red (#ef4444) errors

## Configuration

**Key Sections** in `audit_config.yaml`:
- `database`: Connection settings (backend: bigquery/snowflake/databricks, connection_params)
  - **BigQuery**: default_database (project_id), default_schema (dataset), credentials_path/credentials_json, source_project_id (cross-project)
  - **Snowflake**: default_database, default_schema, account, user, password/authenticator, warehouse, role
  - **Databricks**: default_database (catalog), default_schema, server_hostname, http_path, auth_type/access_token, source_catalog (cross-catalog)
- `tables`: Tables to audit (optional: `primary_key`, `query`, `schema` per table)
- `column_insights`: Profiling settings per data type
- `output.number_format`: Display formatting (thousand_separator, decimal_places)

## Tech Stack

- **Ibis**: Lazy SQL generation, database-native sampling, no data export
- **Polars**: Fast DataFrames post-query (vs Pandas)
- **Pydantic**: Runtime parameter validation
- **PyYAML**: Config parsing

## Important Gotchas

**Ibis**: Lazy evaluation - must call `.execute()` or `.to_polars()` to run
**Sampling**: Use `.sample(fraction=)` not `.limit()` for statistical validity
**HTML**: Use `format_number()` helper for thousand separators, avoid label overlap with vertical stacking

## Recent Changes (October 2025-November 2025)

**Nov 24**: Databricks support added
- Implemented `DatabricksAdapter` in `core/db_connection/databricks.py` following BigQuery/Snowflake patterns
- Unity Catalog support with three-level namespace (catalog.schema.table)
- Multiple authentication methods: OAuth/AAD (primary), Personal Access Token, username/password
- Cross-catalog queries via `source_catalog` parameter (similar to BigQuery's `source_project_id`)
- Metadata fetching from `system.information_schema` (INFORMATION_SCHEMA.TABLES, INFORMATION_SCHEMA.COLUMNS, INFORMATION_SCHEMA.TABLE_CONSTRAINTS)
- Updated config validation to support 'databricks' backend with required params (server_hostname, http_path, auth)
- Added Databricks example to config template with environment variable support
- No cost estimation support (returns None, similar to Snowflake)

**Nov 14**: TypeConverter refactor (optimized two-phase conversion)
- Extracted type conversion logic from `auditor.py` to new `core/type_converter.py` class (126 lines → separate module)
- Two-phase strategy: test on 5% sample (90% threshold) before full conversion (95% threshold)
- 30-50% performance improvement by skipping unlikely conversions early
- Optimized null counting: `is_not_null().sum()` instead of `drop_nulls().len()`
- Class-based architecture with configurable thresholds: `sample_threshold`, `full_threshold`, `sample_fraction`
- `auditor.py` reduced from 1304 lines to 1178 lines

**Nov 6**: Class-based insights framework (hybrid architecture)
- Refactored function-based insights to class-based system matching checks framework
- Created `BaseInsight`, `InsightResult`, registry, and runner infrastructure
- Atomic insights: `top_values` (universal), `quantiles` (numeric), `length_stats` (string)
- Type-specific composites: `NumericInsights`, `StringInsights`, `DatetimeInsights`, `BooleanInsights`
- Pydantic validation for all insight parameters
- Returns `List[InsightResult]` throughout entire pipeline (clean architecture, no backward compatibility)
- HTML exporter converts List[InsightResult] to Dict for rendering

**Nov 6**: Enhanced date_outliers check
- Configurable problematic years list (default: [1900, 1970, 2099, 2999, 9999])
- Added `min_suspicious_count` threshold to filter noise
- Context-aware suggestions (1900 → "missing birthdates", 1970 → "Unix epoch", 9999 → "never expires")
- Smart duplicate prevention (don't report problematic years already caught as too old/future)
- Better error messages showing exact years and distances from thresholds

**Nov 6**: Merged get_table_schema and get_column_descriptions
- Single query now fetches both data types and descriptions
- `get_table_schema()` returns `{col: {'data_type': str, 'description': str}}`
- Removed backward compatibility, cleaner architecture
- Column descriptions now display in HTML and JSON exports

**Nov 4**: Simplified threshold strategy
- Removed percentage-based reporting thresholds (80%, 90%, 95%)
- All checks now report issues regardless of percentage affected
- Kept boundary thresholds (greater_than, less_than, min_year, max_year)
- Zero threshold configuration needed - cleaner UX

**Oct 27**: Class-based check framework (926 lines → 11 modular classes)
- Abstract BaseCheck + registry + runner
- Pydantic validation, async-ready
- `auditor.py` now uses `run_check_sync()` API

**Oct 23**: HTML export modularization (1,558 lines → 5 modules)
- Minimalist redesign, 4-tab structure
- Removed emojis, cleaner UI

---

**Last Updated**: November 24, 2025
