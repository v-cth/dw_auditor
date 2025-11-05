# Code Conventions and Style Guide

## Language and Standards

- **Python Version**: 3.10+
- **Style Guide**: PEP 8 with some project-specific extensions
- **Type Hints**: Required for all public functions and methods
- **Docstrings**: Google style for all public functions, classes, and modules

## Naming Conventions

### Functions and Methods
```python
# snake_case for all functions
def get_column_insights(df: pl.DataFrame, config: Dict) -> Dict:
    """Get profiling insights for a column."""
    pass

# Private functions: prefix with underscore
def _render_numeric_insights(insights: Dict) -> str:
    """Internal helper for HTML generation."""
    pass

# Boolean functions: use is_, has_, should_ prefix
def is_primary_key_candidate(column: pl.Series) -> bool:
    """Check if column could be a primary key."""
    pass
```

### Classes
```python
# PascalCase for all classes
class SecureTableAuditor:
    """Main auditor for data warehouse tables."""
    pass

class DatabaseConnection:
    """Ibis database connection wrapper."""
    pass

# Private classes: prefix with underscore
class _QueryBuilder:
    """Internal query construction helper."""
    pass
```

### Variables
```python
# snake_case for variables
table_name = "customers"
row_count = 1000
is_sampled = True

# SCREAMING_SNAKE_CASE for constants
DEFAULT_SAMPLE_SIZE = 10000
MAX_EXAMPLES = 10
PII_KEYWORDS = ["email", "ssn", "phone"]

# Private variables: prefix with underscore
_cached_connection = None
_temp_results = {}
```

### Configuration Keys (YAML)
```yaml
# snake_case for all keys
database:
  backend: "bigquery"
  connection_params:
    project_id: "my-project"

sampling:
  sample_size: 10000
  sample_threshold: 100000

output:
  number_format:
    thousand_separator: ","
    decimal_places: 1
```

## File Organization

### Module Structure
```
module_name/
├── __init__.py          # Public exports only
├── core_module.py       # Main implementation
├── helpers.py           # Helper functions
└── _internal.py         # Private utilities (prefix with _)
```

**Example - HTML Export Module**:
```
exporters/html/
├── __init__.py          # Re-exports: export_to_html
├── export.py            # Main orchestration (assembles tabs)
├── structure.py         # Header, summary, metadata components
├── insights.py          # Column profiling visualizations
├── checks.py            # Quality check results rendering
└── assets.py            # CSS styles and JavaScript
```

### Import Organization
```python
# 1. Standard library imports
import sys
from pathlib import Path
from typing import Dict, List, Optional

# 2. Third-party imports
import polars as pl
import ibis
from ibis import _

# 3. Local application imports
from ..core.config import AuditConfig
from ..utils.security import mask_pii
from .helpers import format_number
```

**Rules**:
- Group imports into 3 sections with blank lines between
- Sort alphabetically within each group
- Use absolute imports for cross-module references
- Use relative imports within same package
- Avoid wildcard imports (`from module import *`)

## Function Design

### Type Hints
```python
# Always provide type hints for parameters and return values
def format_number(
    value: float,
    separator: str = ",",
    decimals: int = 1
) -> str:
    """Format number with thousand separator.

    Args:
        value: Number to format
        separator: Thousand separator character
        decimals: Number of decimal places

    Returns:
        Formatted number string
    """
    pass

# Use Optional for nullable parameters
def get_primary_key(
    table: ibis.Table,
    user_config: Optional[str] = None
) -> Optional[str]:
    """Detect primary key column."""
    pass

# Use Union for multiple types
from typing import Union

def process_value(value: Union[int, float, str]) -> str:
    """Process various value types."""
    pass
```

### Docstrings (Google Style)
```python
def audit_table(
    table_name: str,
    sample_size: int = 10000,
    run_checks: bool = True
) -> Dict:
    """Audit a single database table.

    Performs data quality checks and generates profiling insights
    for the specified table. Handles sampling automatically if the
    table exceeds the configured threshold.

    Args:
        table_name: Name of the table to audit
        sample_size: Maximum rows to analyze (default: 10000)
        run_checks: Whether to run quality checks (default: True)

    Returns:
        Dictionary containing:
            - columns: Per-column check results
            - column_insights: Per-column profiling data
            - metadata: Table metadata (row count, creation time, etc.)
            - timing: Performance metrics

    Raises:
        ConnectionError: If database connection fails
        ValueError: If table_name is invalid or not found

    Example:
        >>> auditor = SecureTableAuditor()
        >>> results = auditor.audit_table("customers")
        >>> print(results['metadata']['row_count'])
        100000
    """
    pass
```

### Function Length
- **Target**: Keep functions under 50 lines
- **Acceptable**: Up to 100 lines for complex logic
- **Refactor**: Split functions longer than 100 lines

**Example Refactoring**:
```python
# Bad: 200-line function
def generate_html_report(results):
    html = "..."  # 200 lines of HTML generation
    return html

# Good: Split into smaller functions
def generate_html_report(results):
    html = _generate_header(results)
    html += _generate_metadata_cards(results)
    html += _generate_column_insights(results)
    html += _generate_issues_section(results)
    return html
```

## Code Style

### String Formatting
```python
# Prefer f-strings for most cases
name = "John"
age = 30
message = f"Hello {name}, you are {age} years old"

# Use triple-quoted strings for multi-line (especially HTML)
html = f"""
    <div style="color: {color};">
        <h1>{title}</h1>
        <p>{content}</p>
    </div>
"""

# Use .format() only when string is reused multiple times
template = "Value: {value}, Count: {count}"
result1 = template.format(value=10, count=5)
result2 = template.format(value=20, count=3)
```

### Dictionary Access
```python
# Prefer .get() with defaults for optional keys
config = {"timeout": 30}
timeout = config.get("timeout", 60)  # Good
sample_size = config.get("sample_size", 10000)

# Use direct access [] only for required keys
backend = config["backend"]  # Will raise KeyError if missing
```

### List Comprehensions
```python
# Good: Simple transformations
squared = [x**2 for x in numbers]
names = [user["name"] for user in users if user["active"]]

# Bad: Complex logic (use explicit loop)
# Don't do this:
result = [process(x) if check(x) else fallback(x) for x in items if validate(x) and not skip(x)]

# Do this instead:
result = []
for item in items:
    if not validate(item) or skip(item):
        continue
    result.append(process(item) if check(item) else fallback(item))
```

### Error Handling
```python
# Specific exceptions
try:
    connection = create_connection(config)
except ConnectionError as e:
    logger.error(f"Failed to connect: {e}")
    raise

# Avoid bare except
try:
    risky_operation()
except Exception as e:  # OK: At least catch Exception
    logger.error(f"Unexpected error: {e}")
    raise

# Never use bare except:
try:
    risky_operation()
except:  # BAD: Catches everything including KeyboardInterrupt
    pass
```

## HTML Report Conventions

### Design Philosophy
- **Minimalist**: Clean typography, no emojis, focused on data
- **Inline CSS**: All styles inline for single-file portability
- **Four-tab structure**: Summary → Insights → Quality Checks → Metadata
- **Inter font**: Professional, readable sans-serif

### CSS Styling Patterns

**Color Palette**:
```python
# Primary accent
PURPLE_PRIMARY = "#6606dc"   # Main accent color

# Status colors
GREEN = "#10b981"            # Success, high frequency
ORANGE = "#f59e0b"           # Warning, mean/average
RED = "#ef4444"              # Error, issues

# Neutral grays
GRAY_50 = "#f9fafb"
GRAY_600 = "#4b5563"
GRAY_800 = "#1f2937"
```

### Layout Patterns
```python
# Container pattern
html = """
<div style="background: white; padding: 25px; border-radius: 8px;
            margin-bottom: 30px; box-shadow: 0 2px 4px rgba(0,0,0,0.1);">
    <!-- Content -->
</div>
"""

# Card pattern
html = """
<div style="background: #f9fafb; border-left: 4px solid #667eea;
            padding: 20px; margin-bottom: 20px; border-radius: 6px;">
    <!-- Content -->
</div>
"""

# Absolute positioning for charts
html = f"""
<div style="position: relative; height: 70px; margin: 10px 0; overflow: hidden;">
    <div style="position: absolute; top: {y}px; left: {x}%; ...">
        {content}
    </div>
</div>
"""
```

### Gradient Patterns
```python
# 5-stop gradient for smooth transitions
gradient = "linear-gradient(to right, #e0e7ff 0%, #c7d2fe 25%, #a5b4fc 50%, #818cf8 75%, #6366f1 100%)"

# Heatmap gradient
def get_bar_color(percentage: float) -> str:
    """Get color for bar based on percentage."""
    if percentage > 50:
        return "#10b981"  # Green
    elif percentage > 20:
        return "#34d399"  # Light green
    else:
        return "#6ee7b7"  # Very light green
```

### Responsive Text Sizing
```python
# Heading hierarchy
h1 = "font-size: 1.5em; font-weight: bold;"
h2 = "font-size: 1.25em; font-weight: bold;"
h3 = "font-size: 1.1em; font-weight: 600;"
h4 = "font-size: 0.95em; font-weight: 600;"

# Body text
normal = "font-size: 0.9em;"
small = "font-size: 0.7em;"
tiny = "font-size: 0.6em;"
```

## Configuration Patterns

### Default Values Strategy
```python
# Pattern 1: Function parameter defaults
def format_number(
    value: float,
    separator: str = ",",      # American default
    decimals: int = 1
) -> str:
    pass

# Pattern 2: Config with fallbacks
thousand_separator = config.get("output", {}).get("number_format", {}).get("thousand_separator", ",")

# Pattern 3: Constants for defaults
DEFAULT_SAMPLE_SIZE = 10000
DEFAULT_THOUSAND_SEPARATOR = ","
DEFAULT_DECIMAL_PLACES = 1

sample_size = config.get("sample_size", DEFAULT_SAMPLE_SIZE)
```

### Config Access Patterns
```python
# Good: Defensive access with defaults
database_backend = config.get("database", {}).get("backend", "bigquery")

# Better: Validate early, fail fast
if "database" not in config:
    raise ValueError("Missing required config section: database")
if "backend" not in config["database"]:
    raise ValueError("Missing required config key: database.backend")
backend = config["database"]["backend"]

# Best: Use config class with validation
config = AuditConfig.from_yaml("config.yaml")
backend = config.database_backend  # Validated on load
```

## Testing Conventions

### Test File Organization
```
tests/
├── test_auditor.py
├── test_checks.py
├── test_insights.py
├── test_exporters.py
└── fixtures/
    ├── sample_data.csv
    └── test_config.yaml
```

### Test Naming
```python
def test_format_number_with_comma_separator():
    """Test number formatting with comma separator."""
    result = format_number(1234.5, separator=",", decimals=1)
    assert result == "1,234.5"

def test_format_number_with_space_separator():
    """Test number formatting with space separator."""
    result = format_number(1234.5, separator=" ", decimals=1)
    assert result == "1 234.5"

def test_format_number_with_zero_decimals():
    """Test integer formatting (zero decimals)."""
    result = format_number(1234.5, separator=",", decimals=0)
    assert result == "1,235"  # Rounded
```

## Git Commit Conventions

### Commit Message Format
```
<type>: <subject>

<body>

<footer>
```

**Types**:
- `feat`: New feature
- `fix`: Bug fix
- `refactor`: Code refactoring (no functional change)
- `docs`: Documentation only
- `style`: Code style changes (formatting, missing semicolons, etc.)
- `test`: Adding or updating tests
- `chore`: Maintenance tasks

**Examples**:
```
feat: add visual distribution range for numeric columns

- Implemented gradient bar showing min/max/quartiles/mean
- Added smart label combining when values overlap
- Added vertical stacking to prevent label overflow
- Configurable via output.number_format in config

Closes #42
```

```
fix: prevent label overflow in distribution visualizations

Labels were extending outside container boundaries when positioned
at top edge. Added 5px offset and overflow: hidden to container.
```

```
refactor: simplify format_number function

- Removed unnecessary if statement
- Made separator configurable via parameter
- Added decimal_places parameter for flexibility
```

## Code Review Checklist

### Before Submitting PR
- [ ] All functions have type hints
- [ ] All public functions have docstrings
- [ ] No hardcoded values (use config or constants)
- [ ] Error cases handled gracefully
- [ ] HTML changes tested in browser
- [ ] Config changes documented in audit_config.yaml
- [ ] Import statements organized correctly
- [ ] No debug print statements left in code

### What Reviewers Look For
1. **Correctness**: Does it work as intended?
2. **Security**: Any PII exposure risks?
3. **Performance**: Any obvious bottlenecks?
4. **Readability**: Clear variable names, good structure?
5. **Consistency**: Follows project conventions?
6. **Documentation**: Config changes documented?

---

**Last Updated**: October 23, 2025
