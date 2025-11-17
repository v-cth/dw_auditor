"""
Check execution API - runs checks by name via the registry
"""

from typing import List, Optional, Any, Dict
import polars as pl
from .plugin import get_check, check_exists
from .base_check import CheckResult


def run_check_sync(
    check_name: str,
    df: pl.DataFrame,
    col: str,
    primary_key_columns: Optional[List[str]] = None,
    **params
) -> List[CheckResult]:
    """Run a data quality check by name

    This is the primary API for executing checks. It looks up the check
    in the registry, instantiates it with the provided parameters, and
    executes it.

    Args:
        check_name: Name of the check to run (e.g., "numeric_range")
        df: Polars DataFrame to check
        col: Column name to check
        primary_key_columns: Optional list of primary key columns for context
        **params: Check-specific parameters (e.g., greater_than=0 for numeric_range)

    Returns:
        List of CheckResult objects representing issues found

    Raises:
        ValueError: If check_name is not registered
        ValidationError: If parameters are invalid for the check

    Example:
        results = run_check_sync(
            "numeric_range",
            df,
            col="price",
            greater_than=0,
            less_than=1000
        )

        for result in results:
            print(f"Found {result.count} violations of type {result.type}")
    """
    # Look up check class in registry
    check_class = get_check(check_name)

    if check_class is None:
        available = ", ".join(sorted(get_check.__globals__['CHECK_REGISTRY'].keys()))
        raise ValueError(
            f"Unknown check: '{check_name}'. "
            f"Available checks: {available}"
        )

    # Instantiate check (this validates params via Pydantic)
    check_instance = check_class(
        df=df,
        col=col,
        primary_key_columns=primary_key_columns,
        **params
    )

    # Execute check
    return check_instance.run()


def run_multiple_checks(
    checks: List[Dict[str, Any]],
    df: pl.DataFrame,
    col: str,
    primary_key_columns: Optional[List[str]] = None
) -> Dict[str, List[CheckResult]]:
    """Run multiple checks on the same column sequentially

    Args:
        checks: List of check configurations, each with 'name' and optional params
        df: Polars DataFrame to check
        col: Column name to check
        primary_key_columns: Optional list of primary key columns for context

    Returns:
        Dictionary mapping check names to their results

    Example:
        checks = [
            {'name': 'numeric_range', 'greater_than': 0},
            {'name': 'numeric_range', 'less_than': 100}
        ]
        results = run_multiple_checks(checks, df, col="price")
    """
    output = {}

    for check_config in checks:
        check_name = check_config['name']
        params = {k: v for k, v in check_config.items() if k != 'name'}

        try:
            result = run_check_sync(check_name, df, col, primary_key_columns, **params)
            output[check_name] = result
        except Exception as e:
            # Store exception for error handling
            output[check_name] = e

    return output


def validate_check_config(check_name: str, **params) -> bool:
    """Validate check parameters without running the check

    Useful for config file validation.

    Args:
        check_name: Name of the check
        **params: Parameters to validate

    Returns:
        True if validation succeeds

    Raises:
        ValueError: If check doesn't exist
        ValidationError: If parameters are invalid
    """
    if not check_exists(check_name):
        raise ValueError(f"Unknown check: '{check_name}'")

    check_class = get_check(check_name)

    # Create a dummy instance just to trigger validation
    # Use empty DataFrame since we're only validating params
    dummy_df = pl.DataFrame({})
    try:
        _ = check_class(dummy_df, col="dummy", **params)
        return True
    except Exception:
        raise
