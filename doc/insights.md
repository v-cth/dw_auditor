# 📊 Column Insights Reference

This document describes all available **column insights** provided by the Data Warehouse Auditor.

Insights provide **statistical summaries and profiling metrics** to help you understand the shape, variety, and health of your data.  
They complement data quality checks by giving **context**, not pass/fail rules.

---

## ⚙️ Overview

Column insights are defined under the `column_insights` section of the configuration file:

```yaml
column_insights:
  defaults:
    string:
      top_values: 10
      min_length: true
      max_length: true
      avg_length: true
    numeric:
      min: true
      max: true
      mean: true
      quantiles: true
    datetime:
      min_date: true
      max_date: true
      date_range_days: true
  tables:
    orders:
      total_price:
        quantiles: true
```

Each insight returns aggregated metrics used in **audit reports** (CSV, JSON, HTML).

---

## 🧷 String Insights

| Insight | Type | Description | Example Config |
|----------|------|--------------|----------------|
| **`top_values`** | `int` | Returns top N most frequent values with counts and percentages. | `top_values: 10` |
| **`unique_count`** | `bool` | Reports number of distinct values. | `unique_count: true` |
| **`min_length`** | `bool` | Minimum string length found in column. | `min_length: true` |
| **`max_length`** | `bool` | Maximum string length found in column. | `max_length: true` |
| **`avg_length`** | `bool` | Average string length across all rows. | `avg_length: true` |
| **`length_distribution`** | `bool` | Histogram of string lengths (useful for ID codes, names, etc.). | `length_distribution: true` |

### Example

```yaml
column_insights:
  tables:
    users:
      email:
        top_values: 5
        min_length: true
        max_length: true
```

---

## 🔢 Numeric Insights

| Insight | Type | Description | Example Config |
|----------|------|--------------|----------------|
| **`min`** | `bool` | Minimum value in the column. | `min: true` |
| **`max`** | `bool` | Maximum value in the column. | `max: true` |
| **`mean`** | `bool` | Arithmetic average of all values. | `mean: true` |
| **`median`** | `bool` | Median value. | `median: true` |
| **`std`** | `bool` | Standard deviation (useful for variance detection). | `std: true` |
| **`quantiles`** | `bool` \| `list` | Percentiles to report. `true` = [0.25, 0.5, 0.75] or provide custom list. | `quantiles: true`<br>`quantiles: [0.1, 0.25, 0.5, 0.75, 0.9]` |
| **`top_values`** | `int` | Most frequent numeric values (useful for low-cardinality fields). | `top_values: 5` |
| **`histogram`** | `bool` \| `int` \| `dict` | Distribution buckets. `true` = 10 bins, int = custom bins, dict = advanced config. | `histogram: true`<br>`histogram: 20`<br>`histogram: {bins: 15, method: "equal_frequency"}` |

### Histogram Configuration

The `histogram` insight supports multiple binning strategies:

```yaml
# Simple: Use defaults (10 bins, equal_width)
histogram: true

# Custom bin count
histogram: 20

# Advanced configuration
histogram:
  bins: 15
  method: "equal_frequency"  # equal_width, equal_frequency, quartiles, explicit
  edge_handling: "include_left"  # include_left, include_right, include_both

# Explicit bucket boundaries
histogram:
  method: "explicit"
  buckets: [0, 10, 50, 100, 500, 1000]
```

**Binning methods:**
- `equal_width` - Equal-sized ranges (default)
- `equal_frequency` - Equal number of values per bin
- `quartiles` - Use Q1, Q2, Q3 boundaries
- `explicit` - Custom bucket boundaries via `buckets` parameter

### Example

```yaml
column_insights:
  defaults:
    numeric:
      min: true
      max: true
      mean: true
      quantiles: true  # Uses defaults [0.25, 0.5, 0.75]
      histogram: 10    # 10 bins, equal_width
  tables:
    orders:
      total_amount:
        quantiles: [0.1, 0.25, 0.5, 0.75, 0.9, 0.95]
        histogram:
          bins: 20
          method: "equal_frequency"
```

---

## ⏰ Datetime Insights

| Insight | Type | Description | Example Config |
|----------|------|--------------|----------------|
| **`min_date`** | `bool` | Earliest date in the column. | `min_date: true` |
| **`max_date`** | `bool` | Latest date in the column. | `max_date: true` |
| **`date_range_days`** | `bool` | Number of days between min and max dates. | `date_range_days: true` |
| **`most_common_dates`** | `int` | Top N most common date values. | `most_common_dates: 5` |
| **`most_common_days`** | `int` | Top N most common days of week (0=Monday). | `most_common_days: 7` |
| **`most_common_hours`** | `int` | Top N most frequent hours (0–23). | `most_common_hours: 10` |
| **`most_common_timezones`** | `int` | Reports dominant timezone(s) if column includes tz-aware timestamps. | `most_common_timezones: 1` |

### Example

```yaml
column_insights:
  defaults:
    datetime:
      min_date: true
      max_date: true
      most_common_days: 7
  tables:
    sessions:
      login_timestamp:
        most_common_hours: 5
```

---

## 🧩 Configuration Levels

There are two main configuration levels for insights:

1. **Global Defaults**
   ```yaml
   column_insights:
     defaults:
       numeric:
         mean: true
         quantiles: [0.25, 0.5, 0.75]
   ```

2. **Per-Table / Per-Column Overrides**
   ```yaml
   column_insights:
     tables:
       sales:
         price:
           quantiles: [0.1, 0.5, 0.9]
           std: true
   ```

Columns not explicitly configured will use the defaults for their data type.

---

## 🚫 Disabling Insights

Set any insight to `false` or `0` to disable:

```yaml
column_insights:
  tables:
    orders:
      order_id:
        top_values: 0
```

---

## 📘 Example Summary

```yaml
column_insights:
  defaults:
    string:
      top_values: 10
      min_length: true
      max_length: true
      avg_length: true
    numeric:
      min: true
      max: true
      mean: true
      median: true
      quantiles: [0.25, 0.5, 0.75]
    datetime:
      min_date: true
      max_date: true
      most_common_days: 7
  tables:
    orders:
      total_price:
        quantiles: [0.1, 0.25, 0.5, 0.9, 0.95]
      order_date:
        most_common_hours: 10
```

---

## ✅ Supported Data Types

- `string`
- `numeric`
- `datetime`

Each type supports both **descriptive statistics** and **distribution metrics**.

---

**Next:** [Quality Checks Reference →](./checks.md)
