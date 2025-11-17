"""
Insight execution API - runs insights by name via the registry
"""

from typing import List, Optional, Any, Dict
import polars as pl
from .plugin import get_insight, insight_exists, list_insights
from .base_insight import InsightResult


def run_insight_sync(
    insight_name: str,
    df: pl.DataFrame,
    col: str,
    **params
) -> List[InsightResult]:
    """Run a column insight by name

    This is the primary API for executing insights. It looks up the insight
    in the registry, instantiates it with the provided parameters, and
    executes it.

    Args:
        insight_name: Name of the insight to run (e.g., "top_values")
        df: Polars DataFrame to analyze
        col: Column name to analyze
        **params: Insight-specific parameters (e.g., limit=10 for top_values)

    Returns:
        List of InsightResult objects representing computed insights

    Raises:
        ValueError: If insight_name is not registered
        ValidationError: If parameters are invalid for the insight

    Example:
        results = run_insight_sync(
            "top_values",
            df,
            col="category",
            limit=10
        )

        for result in results:
            print(f"{result.type}: {result.value}")
    """
    # Look up insight class in registry
    insight_class = get_insight(insight_name)

    if insight_class is None:
        available = ", ".join(sorted(list_insights()))
        raise ValueError(
            f"Unknown insight: '{insight_name}'. "
            f"Available insights: {available}"
        )

    # Instantiate insight (this validates params via Pydantic)
    insight_instance = insight_class(
        df=df,
        col=col,
        **params
    )

    # Execute insight
    return insight_instance.generate()


def run_multiple_insights(
    insights: List[Dict[str, Any]],
    df: pl.DataFrame,
    col: str
) -> Dict[str, List[InsightResult]]:
    """Run multiple insights on the same column sequentially

    Args:
        insights: List of insight configurations, each with 'name' and optional params
        df: Polars DataFrame to analyze
        col: Column name to analyze

    Returns:
        Dictionary mapping insight names to their results

    Example:
        insights = [
            {'name': 'min'},
            {'name': 'max'},
            {'name': 'top_values', 'limit': 10}
        ]
        results = run_multiple_insights(insights, df, col="price")
    """
    output = {}

    for insight_config in insights:
        insight_name = insight_config['name']
        params = {k: v for k, v in insight_config.items() if k != 'name'}

        try:
            result = run_insight_sync(insight_name, df, col, **params)
            output[insight_name] = result
        except Exception as e:
            # Store exception for error handling
            output[insight_name] = e

    return output


def validate_insight_config(insight_name: str, **params) -> bool:
    """Validate insight parameters without running the insight

    Useful for config file validation.

    Args:
        insight_name: Name of the insight
        **params: Parameters to validate

    Returns:
        True if validation succeeds

    Raises:
        ValueError: If insight doesn't exist
        ValidationError: If parameters are invalid
    """
    if not insight_exists(insight_name):
        raise ValueError(f"Unknown insight: '{insight_name}'")

    insight_class = get_insight(insight_name)

    # Create a dummy instance just to trigger validation
    # Use empty DataFrame since we're only validating params
    dummy_df = pl.DataFrame({})
    try:
        _ = insight_class(dummy_df, col="dummy", **params)
        return True
    except Exception:
        raise
