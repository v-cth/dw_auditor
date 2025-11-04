Prefer a modular, extensible architecture that avoids unnecessary coupling between components.
Communicate clearly and concisely, prioritizing relevant information and avoiding verbosity.
Adhere to industry best practices, including clean code principles, consistent naming conventions, and maintainable design patterns.

# Data Warehouse Table Auditor

High-performance data warehouse auditing tool for BigQuery and Snowflake using Ibis for secure, direct database access.

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
- `dw_auditor/core/`: Fundamental classes (auditor, config, base_check, registry, runner)
- `dw_auditor/checks/`: 11 data quality check classes (auto-registered)
- `dw_auditor/insights/`: Profiling functions (numeric, datetime, string)
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
- `database`: Connection settings (backend, project_id, dataset_id)
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

## Recent Changes (October 2025)

**Oct 27**: Class-based check framework (926 lines → 11 modular classes)
- Abstract BaseCheck + registry + runner
- Pydantic validation, async-ready
- `auditor.py` now uses `run_check_sync()` API

**Oct 23**: HTML export modularization (1,558 lines → 5 modules)
- Minimalist redesign, 4-tab structure
- Removed emojis, cleaner UI

---

**Last Updated**: November 2025
