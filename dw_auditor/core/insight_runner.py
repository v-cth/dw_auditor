"""
Insight execution API - runs insights by name via the registry
"""

import asyncio
from typing import List, Optional, Any, Dict
import polars as pl
from .insight_registry import get_insight, insight_exists
from .base_insight import InsightResult


async def run_insight(
    insight_name: str,
    df: pl.DataFrame,
    col: str,
    **params
) -> List[InsightResult]:
    """Run a column insight by name (async version)

    This is the primary API for executing insights. It looks up the insight
    in the registry, instantiates it with the provided parameters, and
    executes it asynchronously.

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
        results = await run_insight(
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
        available = ", ".join(sorted(get_insight.__globals__['INSIGHT_REGISTRY'].keys()))
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
    return await insight_instance.generate()


def run_insight_sync(
    insight_name: str,
    df: pl.DataFrame,
    col: str,
    **params
) -> List[InsightResult]:
    """Run a column insight by name (synchronous version)

    This is a convenience wrapper around run_insight() that handles the
    asyncio event loop. Use this when calling from synchronous code.

    Args:
        insight_name: Name of the insight to run
        df: Polars DataFrame to analyze
        col: Column name to analyze
        **params: Insight-specific parameters

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
    """
    # Look up insight class in registry
    insight_class = get_insight(insight_name)

    if insight_class is None:
        from .insight_registry import list_insights
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

    # Execute insight (now synchronous)
    return insight_instance.generate()


async def run_multiple_insights(
    insights: List[Dict[str, Any]],
    df: pl.DataFrame,
    col: str
) -> Dict[str, List[InsightResult]]:
    """Run multiple insights on the same column concurrently

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
        results = await run_multiple_insights(insights, df, col="price")
    """
    tasks = []
    insight_names = []

    for insight_config in insights:
        insight_name = insight_config['name']
        params = {k: v for k, v in insight_config.items() if k != 'name'}

        task = run_insight(insight_name, df, col, **params)
        tasks.append(task)
        insight_names.append(insight_name)

    # Run all insights concurrently
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Package results
    output = {}
    for name, result in zip(insight_names, results):
        if isinstance(result, Exception):
            # Store exception for error handling
            output[name] = result
        else:
            output[name] = result

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
