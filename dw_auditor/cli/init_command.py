"""
CLI command to initialize dw_auditor configuration
"""

import sys
from pathlib import Path
from .config_template import MINIMAL_CONFIG_TEMPLATE


def run_init_command(force: bool = False, path: str = None) -> int:
    """
    Generate a configuration file in current directory or custom path

    Args:
        force: If True, overwrite existing config file
        path: Custom path for config file. If None, creates ./audit_config.yaml

    Returns:
        Exit code (0 = success, 1 = error)
    """
    # Determine target path
    if path:
        config_path = Path(path).resolve()
    else:
        # Create in current directory
        config_path = Path('./audit_config.yaml').resolve()

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
    print("  1. Create a .env file in this directory with your credentials (optionnal)")
    print("\n  2. Follow exemples in {config_path} and edit the file with your database details :")
    print(f"     nano {config_path}")
    print("\n  3. Run your first audit:")
    print("     dw_auditor run")

    return 0
