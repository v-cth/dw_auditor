"""
CLI command to initialize dw_auditor configuration
"""

import sys
from pathlib import Path
from platformdirs import user_config_dir
from .config_template import MINIMAL_CONFIG_TEMPLATE


def run_init_command(force: bool = False, path: str = None) -> int:
    """
    Generate a configuration file in OS-native location or custom path

    Args:
        force: If True, overwrite existing config file
        path: Custom path for config file. If None, uses OS-native location

    Returns:
        Exit code (0 = success, 1 = error)
    """
    # Determine target path
    if path:
        config_path = Path(path).resolve()
    else:
        # Use OS-native config directory
        config_dir = Path(user_config_dir('dw_auditor', appauthor=False))
        config_path = config_dir / 'config.yaml'

    # Check if file already exists
    if config_path.exists() and not force:
        print(f"❌ Config already exists: {config_path}")
        print("   Use --force to overwrite")
        return 1

    # Create directory if it doesn't exist
    try:
        config_path.parent.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        print(f"❌ Failed to create directory: {config_path.parent}")
        print(f"   Error: {e}")
        return 1

    # Write config template
    try:
        config_path.write_text(MINIMAL_CONFIG_TEMPLATE, encoding='utf-8')
    except Exception as e:
        print(f"❌ Failed to write config file: {config_path}")
        print(f"   Error: {e}")
        return 1

    # Success message
    print(f"✅ Config created: {config_path}")
    print("\nNext steps:")
    print("  1. Edit the config file with your database details:")

    # Platform-specific edit command hint
    import platform
    if platform.system() == "Windows":
        print(f"     notepad {config_path}")
    else:
        print(f"     nano {config_path}")

    print(f"\n  2. Run your first audit:")
    print(f"     dw_auditor run")

    return 0
