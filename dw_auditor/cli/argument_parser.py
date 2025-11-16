"""
Command-line argument parser configuration with subcommands
"""

import argparse


def setup_argument_parser() -> argparse.ArgumentParser:
    """
    Configure and return the argument parser with subcommands (init, run)

    Returns:
        Configured ArgumentParser instance
    """
    parser = argparse.ArgumentParser(
        prog='dw_auditor',
        description='Data warehouse audit tool for quality checks and profiling',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Initialize configuration
  dw_auditor init                          # Create config in OS-native location
  dw_auditor init --force                  # Overwrite existing config
  dw_auditor init --path ./my_config.yaml  # Create in custom location

  # Run audits
  dw_auditor run                           # Auto-discover config and run audit
  dw_auditor run my_config.yaml            # Use specific config file
  dw_auditor run --check                   # Quality checks only
  dw_auditor run --insight                 # Profiling/insights only
  dw_auditor run --discover                # Metadata discovery only
  dw_auditor run --log-level DEBUG         # Enable debug logging (shows SQL queries)
        """
    )

    # Create subcommands
    subparsers = parser.add_subparsers(
        dest='command',
        required=True,
        help='Command to execute'
    )

    # ========================================================================
    # INIT SUBCOMMAND
    # ========================================================================
    init_parser = subparsers.add_parser(
        'init',
        help='Create a new configuration file',
        description='Initialize dw_auditor by creating a configuration file'
    )

    init_parser.add_argument(
        '--force', '-f',
        action='store_true',
        help='Overwrite existing configuration file'
    )

    init_parser.add_argument(
        '--path', '-p',
        type=str,
        help='Custom path for config file (default: OS-native config directory)'
    )

    # ========================================================================
    # RUN SUBCOMMAND
    # ========================================================================
    run_parser = subparsers.add_parser(
        'run',
        help='Run database audit',
        description='Run audit with quality checks and insights'
    )

    run_parser.add_argument(
        'config_file',
        nargs='?',
        help='Path to YAML configuration file (optional, will auto-discover)'
    )

    run_parser.add_argument(
        '--yes', '-y',
        action='store_true',
        help='Automatically answer yes to prompts (proceed without confirmation)'
    )

    run_parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Set logging level (default: INFO). Use DEBUG to see executed queries.'
    )

    # Create mutually exclusive group for audit modes
    mode_group = run_parser.add_mutually_exclusive_group()
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
