"""
Check registry system for dynamic check discovery and instantiation
"""

from typing import Dict, Type, List, Optional
from .base_check import BaseCheck


# Global registry mapping check names to check classes
CHECK_REGISTRY: Dict[str, Type[BaseCheck]] = {}


def register_check(name: str):
    """Decorator to register a check class in the global registry

    Usage:
        @register_check("numeric_range")
        class NumericRangeCheck(BaseCheck):
            ...

    Args:
        name: Unique identifier for the check (used in configs and API calls)

    Returns:
        Decorator function that registers the class

    Raises:
        TypeError: If decorated class doesn't inherit from BaseCheck
        ValueError: If check name is already registered
    """
    def decorator(cls: Type[BaseCheck]):
        # Validate that class inherits from BaseCheck
        if not issubclass(cls, BaseCheck):
            raise TypeError(
                f"{cls.__name__} must inherit from BaseCheck to be registered"
            )

        # Check for name collisions
        if name in CHECK_REGISTRY:
            raise ValueError(
                f"Check '{name}' is already registered by {CHECK_REGISTRY[name].__name__}"
            )

        # Set the name attribute on the class
        cls.name = name

        # Register in global dict
        CHECK_REGISTRY[name] = cls

        return cls

    return decorator


def get_check(name: str) -> Optional[Type[BaseCheck]]:
    """Retrieve a check class by name from the registry

    Args:
        name: Check identifier (e.g., "numeric_range")

    Returns:
        Check class if found, None otherwise

    Example:
        check_class = get_check("numeric_range")
        if check_class:
            instance = check_class(df, col="price", greater_than=0)
    """
    return CHECK_REGISTRY.get(name)


def list_checks() -> List[str]:
    """List all registered check names

    Returns:
        Sorted list of check identifiers

    Example:
        available_checks = list_checks()
        # ['case_duplicates', 'date_range', 'numeric_range', ...]
    """
    return sorted(CHECK_REGISTRY.keys())


def get_check_info() -> Dict[str, Dict[str, str]]:
    """Get detailed information about all registered checks

    Returns:
        Dictionary mapping check names to their metadata:
        - name: Registry key
        - display_name: Human-readable name
        - class_name: Python class name
        - module: Module path

    Example:
        info = get_check_info()
        for check_name, details in info.items():
            print(f"{check_name}: {details['display_name']}")
    """
    info = {}
    for name, check_class in CHECK_REGISTRY.items():
        info[name] = {
            'name': name,
            'display_name': check_class.display_name or name,
            'class_name': check_class.__name__,
            'module': check_class.__module__
        }
    return info


def check_exists(name: str) -> bool:
    """Check if a check with given name is registered

    Args:
        name: Check identifier

    Returns:
        True if registered, False otherwise
    """
    return name in CHECK_REGISTRY
