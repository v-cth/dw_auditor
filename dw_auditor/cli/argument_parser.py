"""
Command-line argument parser configuration
"""

import argparse


def setup_argument_parser() -> argparse.ArgumentParser:
    """
    Configure and return the argument parser for the audit CLI

    Returns:
        Configured ArgumentParser instance
    """
    parser = argparse.ArgumentParser(
        description='Run database audit with quality checks and insights',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python audit.py                         # Full audit (checks + insights)
  python audit.py my_config.yaml          # Use custom config
  python audit.py --check                 # Check mode (quality checks only)
  python audit.py --insight               # Insight mode (profiling only)
  python audit.py --discover              # Discovery mode (metadata only)
  python audit.py --log-level DEBUG       # Enable debug logging (shows SQL queries)
        """
    )

    parser.add_argument(
        'config_file',
        nargs='?',
        default='audit_config.yaml',
        help='Path to YAML configuration file (default: audit_config.yaml)'
    )

    parser.add_argument(
        '--yes', '-y',
        action='store_true',
        help='Automatically answer yes to prompts (proceed without confirmation)'
    )

    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Set logging level (default: INFO). Use DEBUG to see executed queries.'
    )

    # Create mutually exclusive group for audit modes
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        '--check', '-c',
        action='store_true',
        help='Check mode: run quality checks only (skip profiling/insights)'
    )
    mode_group.add_argument(
        '--insight', '-i',
        action='store_true',
        help='Insight mode: run profiling/insights only (skip quality checks)'
    )
    mode_group.add_argument(
        '--discover',
        action='store_true',
        help='Discovery mode: collect metadata only (skip checks and insights)'
    )

    return parser


def determine_audit_mode(args: argparse.Namespace) -> str:
    """
    Determine audit mode from parsed arguments

    Args:
        args: Parsed command-line arguments

    Returns:
        Audit mode string ('discover', 'checks', 'insights', or 'full')
    """
    if args.discover:
        return 'discover'
    elif args.check:
        return 'checks'
    elif args.insight:
        return 'insights'
    else:
        return 'full'
