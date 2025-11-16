"""
Config file discovery logic
"""

from pathlib import Path
from platformdirs import user_config_dir


def discover_config(explicit_path: str = None) -> str:
    """
    Discover configuration file from various locations

    Priority order:
    1. Explicit path provided by user
    2. OS-native config location (~/.config/dw_auditor/config.yaml on Linux/Mac)
    3. Current directory (./audit_config.yaml)

    Args:
        explicit_path: Optional explicit path to config file

    Returns:
        Absolute path to config file as string

    Raises:
        FileNotFoundError: If no config file is found in any location
    """
    # Priority 1: Explicit path from command line
    if explicit_path:
        path = Path(explicit_path).resolve()
        if not path.exists():
            raise FileNotFoundError(f"Config not found: {explicit_path}")
        return str(path)

    # Priority 2: OS-native config location
    os_config_dir = Path(user_config_dir('dw_auditor', appauthor=False))
    os_config = os_config_dir / 'config.yaml'
    if os_config.exists():
        return str(os_config)

    # Priority 3: Current directory (legacy location)
    local_config = Path('audit_config.yaml').resolve()
    if local_config.exists():
        return str(local_config)

    # Not found anywhere
    raise FileNotFoundError(
        "No config file found.\n"
        "Run 'dw_auditor init' to create one, or specify a config file path."
    )


def get_config_location() -> str:
    """
    Get the OS-native config file path (for informational purposes)

    Returns:
        Path to where config file should be created
    """
    config_dir = Path(user_config_dir('dw_auditor', appauthor=False))
    return str(config_dir / 'config.yaml')
