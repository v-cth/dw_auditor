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
  dw_auditor                              # Run audit (default: checks + insights)
  dw_auditor run                          # Same as above
  dw_auditor my_config.yaml               # Use custom config
  dw_auditor run my_config.yaml           # Same as above
  dw_auditor --check                      # Quality checks only
  dw_auditor --insight                    # Profiling/insights only
  dw_auditor --discover                   # Metadata discovery only
  dw_auditor --log-level DEBUG            # Enable debug logging (shows SQL queries)
        """
    )

    # Optional "run" subcommand (for convenience)
    parser.add_argument(
        'subcommand',
        nargs='?',
        help=argparse.SUPPRESS  # Hide from help
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
        help='Run quality checks only (skip insights)'
    )
    mode_group.add_argument(
        '--insight', '-i',
        action='store_true',
        help='Run insights/profiling only (skip checks)'
    )
    mode_group.add_argument(
        '--discover',
        action='store_true',
        help='Collect metadata only (skip checks and insights)'
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
