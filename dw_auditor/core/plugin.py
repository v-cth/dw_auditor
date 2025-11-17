"""
Unified plugin registry system for checks and insights

This module provides a single registry mechanism for both data quality checks
and column insights, eliminating code duplication between the two systems.
"""

from typing import Dict, Type, List, Optional, Union, Literal
from .base_check import BaseCheck
from .base_insight import BaseInsight


# Type alias for plugin classes
PluginClass = Union[Type[BaseCheck], Type[BaseInsight]]
PluginCategory = Literal['check', 'insight']


# Global registry mapping plugin names to plugin classes
PLUGIN_REGISTRY: Dict[str, Dict[str, Union[PluginClass, str]]] = {}


def register_plugin(name: str, category: PluginCategory):
    """Decorator to register a plugin (check or insight) in the global registry

    Usage:
        @register_plugin("numeric_range", category="check")
        class NumericRangeCheck(BaseCheck):
            ...

        @register_plugin("top_values", category="insight")
        class TopValuesInsight(BaseInsight):
            ...

    Args:
        name: Unique identifier for the plugin (used in configs and API calls)
        category: Plugin category - either "check" or "insight"

    Returns:
        Decorator function that registers the class

    Raises:
        TypeError: If decorated class doesn't inherit from BaseCheck/BaseInsight
        ValueError: If plugin name is already registered in that category
    """
    def decorator(cls: PluginClass):
        # Validate that class inherits from appropriate base
        if category == 'check' and not issubclass(cls, BaseCheck):
            raise TypeError(
                f"{cls.__name__} must inherit from BaseCheck to be registered as a check"
            )
        elif category == 'insight' and not issubclass(cls, BaseInsight):
            raise TypeError(
                f"{cls.__name__} must inherit from BaseInsight to be registered as an insight"
            )

        # Check for name collisions
        if name in PLUGIN_REGISTRY:
            raise ValueError(
                f"Plugin '{name}' is already registered by {PLUGIN_REGISTRY[name]['class'].__name__}"
            )

        # Set the name attribute on the class
        cls.name = name

        # Register in global dict with metadata
        PLUGIN_REGISTRY[name] = {
            'class': cls,
            'category': category,
            'display_name': cls.display_name or name,
            'module': cls.__module__
        }

        return cls

    return decorator


def get_plugin(name: str) -> Optional[PluginClass]:
    """Retrieve a plugin class by name from the registry

    Args:
        name: Plugin identifier (e.g., "numeric_range", "top_values")

    Returns:
        Plugin class if found, None otherwise

    Example:
        plugin_class = get_plugin("numeric_range")
        if plugin_class:
            instance = plugin_class(df, col="price", greater_than=0)
    """
    if name not in PLUGIN_REGISTRY:
        return None
    return PLUGIN_REGISTRY[name]['class']


def get_check(name: str) -> Optional[Type[BaseCheck]]:
    """Retrieve a check plugin by name (backward compatibility)

    Args:
        name: Check identifier (e.g., "numeric_range")

    Returns:
        Check class if found and is a check, None otherwise
    """
    if name not in PLUGIN_REGISTRY:
        return None

    plugin_info = PLUGIN_REGISTRY[name]
    if plugin_info['category'] == 'check':
        return plugin_info['class']

    return None


def get_insight(name: str) -> Optional[Type[BaseInsight]]:
    """Retrieve an insight plugin by name (backward compatibility)

    Args:
        name: Insight identifier (e.g., "top_values")

    Returns:
        Insight class if found and is an insight, None otherwise
    """
    if name not in PLUGIN_REGISTRY:
        return None

    plugin_info = PLUGIN_REGISTRY[name]
    if plugin_info['category'] == 'insight':
        return plugin_info['class']

    return None


def list_plugins(category: Optional[PluginCategory] = None) -> List[str]:
    """List all registered plugin names, optionally filtered by category

    Args:
        category: Optional category filter ("check" or "insight")

    Returns:
        Sorted list of plugin identifiers

    Example:
        all_plugins = list_plugins()
        checks_only = list_plugins(category="check")
        insights_only = list_plugins(category="insight")
    """
    if category is None:
        return sorted(PLUGIN_REGISTRY.keys())

    return sorted([
        name for name, info in PLUGIN_REGISTRY.items()
        if info['category'] == category
    ])


def list_checks() -> List[str]:
    """List all registered check names (backward compatibility)

    Returns:
        Sorted list of check identifiers
    """
    return list_plugins(category='check')


def list_insights() -> List[str]:
    """List all registered insight names (backward compatibility)

    Returns:
        Sorted list of insight identifiers
    """
    return list_plugins(category='insight')


def get_plugin_info(name: Optional[str] = None) -> Union[Dict, Dict[str, Dict]]:
    """Get detailed information about plugin(s)

    Args:
        name: Optional plugin name. If None, returns info for all plugins

    Returns:
        If name provided: Dictionary with plugin metadata
        If name is None: Dictionary mapping plugin names to their metadata

    Example:
        # Get info for specific plugin
        info = get_plugin_info("numeric_range")
        # {'name': 'numeric_range', 'category': 'check', 'display_name': 'Numeric Range', ...}

        # Get info for all plugins
        all_info = get_plugin_info()
    """
    if name is not None:
        if name not in PLUGIN_REGISTRY:
            return {}

        info = PLUGIN_REGISTRY[name].copy()
        info['name'] = name
        info['class_name'] = info['class'].__name__
        # Remove the class object from the returned info
        del info['class']
        return info

    # Return info for all plugins
    result = {}
    for plugin_name, plugin_data in PLUGIN_REGISTRY.items():
        result[plugin_name] = {
            'name': plugin_name,
            'category': plugin_data['category'],
            'display_name': plugin_data['display_name'],
            'class_name': plugin_data['class'].__name__,
            'module': plugin_data['module']
        }

    return result


def check_exists(name: str) -> bool:
    """Check if a check plugin with given name is registered

    Args:
        name: Check identifier

    Returns:
        True if registered as a check, False otherwise
    """
    return name in PLUGIN_REGISTRY and PLUGIN_REGISTRY[name]['category'] == 'check'


def insight_exists(name: str) -> bool:
    """Check if an insight plugin with given name is registered

    Args:
        name: Insight identifier

    Returns:
        True if registered as an insight, False otherwise
    """
    return name in PLUGIN_REGISTRY and PLUGIN_REGISTRY[name]['category'] == 'insight'


def plugin_exists(name: str) -> bool:
    """Check if a plugin with given name is registered (any category)

    Args:
        name: Plugin identifier

    Returns:
        True if registered, False otherwise
    """
    return name in PLUGIN_REGISTRY
