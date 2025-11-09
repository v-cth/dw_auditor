"""
Insight registry system for dynamic insight discovery and instantiation
"""

from typing import Dict, Type, List, Optional
from .base_insight import BaseInsight


# Global registry mapping insight names to insight classes
INSIGHT_REGISTRY: Dict[str, Type[BaseInsight]] = {}


def register_insight(name: str):
    """Decorator to register an insight class in the global registry

    Usage:
        @register_insight("top_values")
        class TopValuesInsight(BaseInsight):
            ...

    Args:
        name: Unique identifier for the insight (used in configs and API calls)

    Returns:
        Decorator function that registers the class

    Raises:
        TypeError: If decorated class doesn't inherit from BaseInsight
        ValueError: If insight name is already registered
    """
    def decorator(cls: Type[BaseInsight]):
        # Validate that class inherits from BaseInsight
        if not issubclass(cls, BaseInsight):
            raise TypeError(
                f"{cls.__name__} must inherit from BaseInsight to be registered"
            )

        # Check for name collisions
        if name in INSIGHT_REGISTRY:
            raise ValueError(
                f"Insight '{name}' is already registered by {INSIGHT_REGISTRY[name].__name__}"
            )

        # Set the name attribute on the class
        cls.name = name

        # Register in global dict
        INSIGHT_REGISTRY[name] = cls

        return cls

    return decorator


def get_insight(name: str) -> Optional[Type[BaseInsight]]:
    """Retrieve an insight class by name from the registry

    Args:
        name: Insight identifier (e.g., "top_values")

    Returns:
        Insight class if found, None otherwise

    Example:
        insight_class = get_insight("top_values")
        if insight_class:
            instance = insight_class(df, col="category", limit=10)
    """
    return INSIGHT_REGISTRY.get(name)


def list_insights() -> List[str]:
    """List all registered insight names

    Returns:
        Sorted list of insight identifiers

    Example:
        available_insights = list_insights()
        # ['max', 'mean', 'min', 'quantiles', 'top_values', ...]
    """
    return sorted(INSIGHT_REGISTRY.keys())


def get_insight_info() -> Dict[str, Dict[str, str]]:
    """Get detailed information about all registered insights

    Returns:
        Dictionary mapping insight names to their metadata:
        - name: Registry key
        - display_name: Human-readable name
        - class_name: Python class name
        - module: Module path

    Example:
        info = get_insight_info()
        for insight_name, details in info.items():
            print(f"{insight_name}: {details['display_name']}")
    """
    info = {}
    for name, insight_class in INSIGHT_REGISTRY.items():
        info[name] = {
            'name': name,
            'display_name': insight_class.display_name or name,
            'class_name': insight_class.__name__,
            'module': insight_class.__module__
        }
    return info


def insight_exists(name: str) -> bool:
    """Check if an insight with given name is registered

    Args:
        name: Insight identifier

    Returns:
        True if registered, False otherwise
    """
    return name in INSIGHT_REGISTRY
