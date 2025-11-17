"""Data quality check functions - New class-based architecture with dynamic registry"""

# Import all check classes to trigger registration
from . import (
    numeric_range_check,
    string_trailing_check,
    string_leading_check,
    string_case_check,
    string_regex_check,
    timestamp_pattern_check,
    date_range_check,
    date_outlier_check,
    date_future_check,
    uniqueness_check
)

# Export registry and runner functions for public API
from ..core.plugin import (
    PLUGIN_REGISTRY as CHECK_REGISTRY,  # Backward compatibility
    register_plugin as register_check,  # Backward compatibility
    get_check,
    list_checks,
    get_plugin_info as get_check_info,
    check_exists
)

from ..core.runner import (
    run_check_sync,
    run_multiple_checks,
    validate_check_config
)

__all__ = [
    # Registry functions
    "CHECK_REGISTRY",
    "register_check",
    "get_check",
    "list_checks",
    "get_check_info",
    "check_exists",
    # Runner functions
    "run_check_sync",
    "run_multiple_checks",
    "validate_check_config"
]
