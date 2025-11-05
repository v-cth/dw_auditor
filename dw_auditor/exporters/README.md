# Exporters Module

Professional-grade export functionality for database audit results with enterprise-level security, error handling, and best practices.

## 🎯 Overview

This module provides three export formats for audit results:

- **JSON**: Machine-readable format for integration and automation
- **DataFrame**: Polars DataFrame for data analysis and manipulation
- **HTML**: Beautiful, professional reports with XSS protection

## ✨ Key Features

### Security
- ✅ **XSS Protection**: HTML output uses Jinja2 autoescape to prevent injection attacks
- ✅ **Path Validation**: All file paths are validated and sanitized
- ✅ **Input Validation**: Comprehensive validation of audit results structure
- ✅ **Safe File Operations**: Proper encoding (UTF-8) and error handling

### Reliability
- ✅ **Comprehensive Error Handling**: All operations wrapped in try-catch with specific exceptions
- ✅ **Input Validation**: Results validated before processing
- ✅ **Graceful Degradation**: Clear error messages when dependencies are missing
- ✅ **Proper Logging**: All operations logged for debugging and monitoring

### Code Quality
- ✅ **Type Safety**: TypedDict definitions for all data structures
- ✅ **Clean Architecture**: Separation of concerns (utils, types, exceptions)
- ✅ **Dependency Injection**: Optional dependencies gracefully handled
- ✅ **Comprehensive Docstrings**: Full documentation with examples
- ✅ **No Magic Values**: Constants extracted and named
- ✅ **Consistent APIs**: Similar function signatures across exporters

## 📦 Installation

Basic installation (JSON and DataFrame exports):
```bash
pip install polars pyyaml
```

With HTML export support:
```bash
pip install polars pyyaml jinja2
```

Full installation:
```bash
pip install -r requirements.txt
```

## 🚀 Usage

### JSON Export

```python
from dw_auditor.exporters import export_to_json

# Export to string
json_str = export_to_json(audit_results)

# Export to file
json_str = export_to_json(audit_results, file_path="report.json")
```

**Features:**
- UTF-8 encoding by default
- Customizable indentation
- Automatic path validation
- Safe JSON serialization

### DataFrame Export

```python
from dw_auditor.exporters import export_to_dataframe

# Get Polars DataFrame
df = export_to_dataframe(audit_results)

# Analyze issues
df.filter(pl.col('issue_type') == 'missing_values')
df.groupby('column_name').agg(pl.count())
```

**Features:**
- One row per issue for easy analysis
- Consistent schema even with no issues
- Handles missing optional fields gracefully
- Rich metadata in each row

### HTML Export

```python
from dw_auditor.exporters import export_to_html

# Generate beautiful HTML report
path = export_to_html(audit_results, file_path="report.html")

# Use custom template
path = export_to_html(audit_results, template_name="custom_report.html")
```

**Features:**
- Professional styling with gradients and shadows
- Responsive design
- XSS-protected output
- Customizable templates
- Visual distinction between issues and clean data

## 🛡️ Error Handling

All exporters raise specific exceptions for different error scenarios:

```python
from dw_auditor.exporters import (
    export_to_html,
    ExporterError,
    InvalidResultsError,
    FileExportError,
    PathValidationError
)

try:
    export_to_html(results, "report.html")
except InvalidResultsError as e:
    print(f"Invalid audit results: {e}")
except PathValidationError as e:
    print(f"Invalid file path: {e}")
except FileExportError as e:
    print(f"Failed to write file: {e}")
except ExporterError as e:
    print(f"General export error: {e}")
```

### Exception Hierarchy

```
ExporterError (base)
├── InvalidResultsError      # Malformed audit results
├── FileExportError          # File I/O failures
└── PathValidationError      # Invalid/unsafe file paths
```

## 📋 Type Definitions

The module provides TypedDict definitions for type safety:

```python
from dw_auditor.exporters import (
    AuditResultsDict,
    ColumnDataDict,
    IssueDict
)

# Use in your code for type hints
def process_results(results: AuditResultsDict) -> None:
    pass
```

## 🔧 Architecture

```
exporters/
├── __init__.py              # Public API
├── exceptions.py            # Custom exceptions
├── types.py                 # TypedDict definitions
├── utils.py                 # Shared utilities
├── json_export.py           # JSON exporter
├── dataframe_export.py      # DataFrame exporter
├── html_export.py           # HTML exporter
├── templates/
│   └── audit_report.html    # Jinja2 template
└── README.md                # This file
```

## 🎨 Customizing HTML Reports

Create custom templates in `exporters/templates/`:

```html
<!-- custom_report.html -->
<!DOCTYPE html>
<html>
<head>
    <title>{{ table_name }} Audit</title>
</head>
<body>
    <h1>{{ table_name }}</h1>
    {% if has_issues %}
        <p>Found {{ issue_count }} issues</p>
    {% endif %}
</body>
</html>
```

Use it:
```python
export_to_html(results, template_name="custom_report.html")
```

## 🔒 Security Best Practices Implemented

1. **XSS Prevention**: Jinja2 autoescape enabled for all HTML
2. **Path Traversal Protection**: Paths resolved and validated
3. **Reserved Name Checks**: Windows reserved names blocked
4. **Length Validation**: Filename length limits enforced
5. **Encoding Safety**: Explicit UTF-8 encoding everywhere
6. **Input Sanitization**: All user input validated

## 📊 Performance

- **JSON**: O(n) serialization, minimal memory overhead
- **DataFrame**: O(n) row construction, efficient Polars operations
- **HTML**: O(n) template rendering, one-time Jinja2 compilation

All exporters handle large datasets efficiently with streaming where possible.

## 🧪 Testing

Run tests (if available):
```bash
pytest tests/test_exporters.py -v
```

## 📚 API Reference

### export_to_json()

```python
def export_to_json(
    results: AuditResultsDict,
    file_path: Optional[str] = None,
    indent: int = 2,
    ensure_ascii: bool = False
) -> str
```

**Parameters:**
- `results`: Validated audit results dictionary
- `file_path`: Optional output file path
- `indent`: JSON indentation spaces (default: 2)
- `ensure_ascii`: Escape non-ASCII chars (default: False)

**Returns:** JSON string

### export_to_dataframe()

```python
def export_to_dataframe(
    results: AuditResultsDict
) -> pl.DataFrame
```

**Parameters:**
- `results`: Validated audit results dictionary

**Returns:** Polars DataFrame with one row per issue

### export_to_html()

```python
def export_to_html(
    results: AuditResultsDict,
    file_path: str = "audit_report.html",
    template_name: Optional[str] = None
) -> str
```

**Parameters:**
- `results`: Validated audit results dictionary
- `file_path`: Output HTML file path (default: "audit_report.html")
- `template_name`: Custom template name (default: "audit_report.html")

**Returns:** Absolute path to generated HTML file

## 🤝 Contributing

When adding new exporters:
1. Follow the established error handling pattern
2. Add comprehensive docstrings with examples
3. Use TypedDict for input/output types
4. Validate all inputs
5. Log all operations
6. Handle missing dependencies gracefully
7. Add tests

## 📝 Changelog

### Version 1.0.0 (Current)

**Security Improvements:**
- ✅ Fixed XSS vulnerability in HTML export
- ✅ Added path validation and sanitization
- ✅ Implemented input validation for all exporters

**Code Quality:**
- ✅ Added TypedDict definitions
- ✅ Replaced print() with proper logging
- ✅ Extracted HTML to Jinja2 templates
- ✅ Added comprehensive error handling
- ✅ Extracted magic values to constants
- ✅ Added detailed docstrings with examples

**Architecture:**
- ✅ Created exceptions module
- ✅ Created types module
- ✅ Created utils module
- ✅ Improved separation of concerns
- ✅ Made return values consistent

## 📄 License

Part of the Data Warehouse Table Auditor project.

## 🙏 Acknowledgments

Built following enterprise-grade Python best practices including:
- PEP 8 style guide
- Type hints (PEP 484)
- Proper exception handling
- Security-first design
- Clean architecture principles
