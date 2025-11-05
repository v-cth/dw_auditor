"""
Output formatting and printing utilities for CLI
"""


def print_mode_info(audit_mode: str) -> None:
    """
    Print information about the selected audit mode

    Args:
        audit_mode: Audit mode ('discover', 'checks', 'insights', or 'full')
    """
    mode_messages = {
        'discover': "🔍 Discovery mode: Collecting metadata only (skipping quality checks and insights)",
        'checks': "✓ Check mode: Running quality checks only (skipping profiling/insights)",
        'insights': "📊 Insight mode: Running profiling/insights only (skipping quality checks)",
        'full': "🔍 Full audit mode: Running quality checks and profiling/insights"
    }
    print(mode_messages.get(audit_mode, "Unknown audit mode"))


def format_bytes(bytes_val: int) -> str:
    """
    Format byte count into human-readable string

    Args:
        bytes_val: Number of bytes

    Returns:
        Formatted string (e.g., "1.23 GB")
    """
    if bytes_val >= 1_000_000_000_000:  # TB
        return f"{bytes_val / 1_000_000_000_000:.2f} TB"
    elif bytes_val >= 1_000_000_000:  # GB
        return f"{bytes_val / 1_000_000_000:.2f} GB"
    elif bytes_val >= 1_000_000:  # MB
        return f"{bytes_val / 1_000_000:.2f} MB"
    elif bytes_val >= 1_000:  # KB
        return f"{bytes_val / 1_000:.2f} KB"
    else:
        return f"{bytes_val} bytes"


def print_separator(width: int = 70, char: str = '=') -> None:
    """
    Print a separator line

    Args:
        width: Width of the separator
        char: Character to use for separator
    """
    print(char * width)
