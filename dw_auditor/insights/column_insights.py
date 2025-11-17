"""
Main column insights orchestrator - routes to class-based insights
"""

import polars as pl
from typing import List
from ..core.insight_runner import run_insight_sync
from ..core.base_insight import InsightResult


# Import all insight classes to register them
from .atomic import TopValuesInsight, QuantilesInsight, LengthStatsInsight
from .numeric_insights import NumericInsights
from .string_insights import StringInsights
from .datetime_insights import DatetimeInsights
from .boolean_insights import BooleanInsights


# Complex types that don't support insights
def is_complex_type(dtype) -> bool:
    """Check if dtype is a complex type"""
    dtype_class = type(dtype)
    complex_types = [pl.Struct, pl.List, pl.Array, pl.Binary, pl.Object]

    for complex_type in complex_types:
        if dtype_class == complex_type:
            return True
        try:
            if isinstance(dtype, complex_type):
                return True
        except TypeError:
            pass
    return False


def generate_column_insights(df: pl.DataFrame, col: str, config: dict) -> List[InsightResult]:
    """Generate insights for a column based on its type

    This function routes to the appropriate class-based insight generator.

    Args:
        df: Polars DataFrame
        col: Column name
        config: Insights configuration for this column

    Returns:
        List of InsightResult objects
    """
    # If no config or all disabled, return empty
    if not config:
        return []

    dtype = df[col].dtype

    # Skip complex types
    if is_complex_type(dtype):
        return []

    # Determine which insight class to use based on dtype
    insight_name = None

    if dtype in [pl.Utf8, pl.String]:
        insight_name = 'string_insights'
    elif (dtype in [pl.Int8, pl.Int16, pl.Int32, pl.Int64,
                    pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64,
                    pl.Float32, pl.Float64] or
          isinstance(dtype, pl.Decimal)):  # Decimal is parameterized type
        insight_name = 'numeric_insights'
    elif dtype in [pl.Datetime, pl.Date]:
        insight_name = 'datetime_insights'
    elif dtype == pl.Boolean:
        insight_name = 'boolean_insights'
    else:
        return []

    # Run the insight using the registry
    try:
        return run_insight_sync(insight_name, df, col, **config)
    except Exception as e:
        # If insight fails, return empty list
        # Log error if needed but don't break the audit
        import logging
        logging.getLogger(__name__).error(f"Insight {insight_name} failed for column {col}: {e}")
        return []
