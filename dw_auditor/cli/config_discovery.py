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
    2. Current directory (./audit_config.yaml) - recommended location
    3. OS-native config location (~/.config/dw_auditor/config.yaml on Linux/Mac) - legacy

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

    # Priority 2: Current directory (recommended location)
    local_config = Path('audit_config.yaml').resolve()
    if local_config.exists():
        return str(local_config)

    # Priority 3: OS-native config location (legacy)
    os_config_dir = Path(user_config_dir('dw_auditor', appauthor=False))
    os_config = os_config_dir / 'config.yaml'
    if os_config.exists():
        return str(os_config)

    # Not found anywhere
    raise FileNotFoundError(
        "No config file found.\n"
        "Run 'dw_auditor init' to create one, or specify a config file path."
    )
