"""
Main column insights orchestrator
"""

import polars as pl
from typing import Dict, Optional
from .string_insights import generate_string_insights
from .numeric_insights import generate_numeric_insights
from .datetime_insights import generate_datetime_insights
from .boolean_insights import generate_boolean_insights


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


def generate_column_insights(df: pl.DataFrame, col: str, config: Dict) -> Dict:
    """
    Generate insights for a column based on its type

    Args:
        df: Polars DataFrame
        col: Column name
        config: Insights configuration for this column

    Returns:
        Dictionary with column insights
    """
    # If no config or all disabled, return empty
    if not config:
        return {}

    dtype = df[col].dtype

    # Skip complex types
    if is_complex_type(dtype):
        return {'note': 'Complex type - insights not supported'}

    # Route to appropriate insights generator based on type
    if dtype in [pl.Utf8, pl.String]:
        return generate_string_insights(df, col, config)
    elif dtype in [pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64, pl.Float32, pl.Float64]:
        return generate_numeric_insights(df, col, config)
    elif dtype in [pl.Datetime, pl.Date]:
        return generate_datetime_insights(df, col, config)
    elif dtype == pl.Boolean:
        return generate_boolean_insights(df, col, config)
    else:
        return {}
