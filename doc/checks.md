# 🧮 Column Checks Reference

This document describes all available **column-level data quality checks** supported by the Data Warehouse Auditor.

Each check can be defined **globally per data type** (under `column_checks.defaults`) or **specifically per table/column** (under `column_checks.tables`).

---

## ⚙️ Overview

Column checks verify that data values meet expected quality criteria such as:
- Correct format and structure  
- Valid ranges and thresholds  
- Absence of duplicates  
- Appropriate casing and patterns  

Checks can be **enabled**, **disabled**, or **customized** with specific parameters.  

```yaml
column_checks:
  defaults:
    string:
      trailing_characters: true
      uniqueness: false
    numeric:
      greater_than_or_equal: 0
  tables:
    dim_customer:
      customer_id:
        uniqueness: true
```

---

## 🧷 String Checks

| Check | Type | Description | Example Config |
|-------|------|--------------|----------------|
| **`trailing_characters`** | `bool` \| `string` \| `list` | Detects unwanted suffixes or trailing characters (e.g., whitespace, `_tmp`). | `true` → default whitespace<br>`["_tmp", "_dim"]` → custom patterns |
| **`ending_characters`** | `bool` \| `string` \| `list` | Flags strings ending with punctuation or undesired endings. | `[".", "!"]` |
| **`case_duplicates`** | `bool` | Detects duplicates ignoring case sensitivity (e.g., “Paris” vs “PARIS”). | `true` |
| **`regex_patterns`** | `bool` \| `string` \| `dict` | Validates string format using regex patterns. Can be simple or detailed config. | ```regex_patterns: { pattern: "^[A-Z]{2}\\d{4}$", mode: "match", description: "Station code format" }``` |
| **`numeric_strings`** | `bool` | Flags string columns where most values are numeric (e.g., `"12345"`). | `true` |
| **`uniqueness`** | `bool` | Checks if all string values are unique. | `true` |

### Notes
- Use `false` to disable any check.
- Regex mode supports:
  - `"match"` → full-string match  
  - `"contains"` → substring detection

---

## 🔢 Numeric Checks

| Check | Type | Description | Example Config |
|-------|------|--------------|----------------|
| **`uniqueness`** | `bool` | Ensures numeric values are unique. | `true` |
| **`greater_than`** | `number` | Ensures all values are greater than a threshold (exclusive). | `greater_than: 0` |
| **`greater_than_or_equal`** | `number` | Ensures all values are ≥ a threshold. | `greater_than_or_equal: 1` |
| **`less_than`** | `number` | Ensures all values are less than a threshold (exclusive). | `less_than: 100` |
| **`less_than_or_equal`** | `number` | Ensures all values are ≤ a threshold. | `less_than_or_equal: 5000` |

### Notes
- You can combine multiple conditions for range validation:
  ```yaml
  price:
    greater_than_or_equal: 0
    less_than_or_equal: 1000
  ```
- Ranges are inclusive/exclusive depending on the operator name.

---

## ⏰ Datetime Checks

| Check | Type | Description | Example Config |
|-------|------|--------------|----------------|
| **`timestamp_patterns`** | `bool` | Detects invalid or inconsistent timestamp formats. | `true` |
| **`future_dates`** | `bool` | Flags timestamps that occur in the future. | `true` |
| **`uniqueness`** | `bool` | Ensures all datetime values are unique. | `false` |
| **`after`** | `string (YYYY-MM-DD)` | Ensures all dates are strictly after a given date. | `after: "2020-01-01"` |
| **`after_or_equal`** | `string (YYYY-MM-DD)` | Ensures all dates are on or after a given date. | `after_or_equal: "2020-01-01"` |
| **`before`** | `string (YYYY-MM-DD)` | Ensures all dates are strictly before a given date. | `before: "2026-01-01"` |
| **`before_or_equal`** | `string (YYYY-MM-DD)` | Ensures all dates are on or before a given date. | `before_or_equal: "2025-12-31"` |

### Notes
- You can mix multiple boundaries:
  ```yaml
  registration_date:
    after: "2020-01-01"
    before: "2025-12-31"
  ```
- Date boundaries can include or exclude endpoints as specified.

---

## 🧠 Configuration Levels

There are **two main configuration levels** for checks:

1. **Global Defaults** — define baseline rules per data type.
   ```yaml
   column_checks:
     defaults:
       string:
         trailing_characters: true
         case_duplicates: true
   ```

2. **Per-Table / Per-Column Overrides** — override specific columns.
   ```yaml
   column_checks:
     tables:
       users:
         email:
           regex_patterns:
             pattern: "^[\\w._%+-]+@[\\w.-]+\\.[a-zA-Z]{2,}$"
             mode: "match"
             description: "Valid email format"
   ```

If a column is not explicitly defined, the **default rules** for its data type are applied.

---

## 🚫 Disabling Checks

Set any check to `false` to disable it globally or locally:
```yaml
column_checks:
  defaults:
    string:
      case_duplicates: false
```

---

## 🧩 Tips

- Keep global defaults lightweight, override only where necessary.  
- Combine with `column_insights` to get descriptive statistics alongside validation.  
- Use clear `description` fields for custom regex patterns — they appear in reports.  
- For performance, limit regex-heavy checks to key columns only.  

---

## 📘 Example Summary

```yaml
column_checks:
  defaults:
    string:
      trailing_characters: true
      numeric_strings: true
    datetime:
      future_dates: true
    numeric:
      greater_than_or_equal: 0
  tables:
    sales:
      order_id:
        uniqueness: true
      order_date:
        after: "2020-01-01"
```

---

## ✅ Supported Data Types

- `string`
- `numeric`
- `datetime`

Each type can have both **generic** and **custom** validation rules.

---

**Next:** [Column Insights Reference →](./insights.md)
