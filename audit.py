#!/usr/bin/env python
"""
Simple CLI for running database audits
Usage: python audit.py [config_file] [--discover]
"""

import sys
import logging
from pathlib import Path
from datetime import datetime

from dw_auditor import AuditConfig, SecureTableAuditor
from dw_auditor.cli import (
    setup_argument_parser,
    determine_audit_mode,
    print_mode_info,
    estimate_bigquery_costs,
    get_user_confirmation,
    discover_tables
)
from dw_auditor.cli.cost_estimation import prefetch_metadata
from dw_auditor.core.db_connection import DatabaseConnection


def setup_logging(log_file: Path, log_level: str = 'INFO') -> None:
    """
    Configure logging to output to both console and file

    Args:
        log_file: Path to log file
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
    """
    formatter = logging.Formatter(
        '%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # File handler
    file_handler = logging.FileHandler(log_file, mode='w', encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, log_level.upper()))
    console_handler.setFormatter(formatter)

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    # Configure module loggers
    for logger_name in ['dw_auditor.core.auditor', 'dw_auditor']:
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.DEBUG)


def initialize_run_directory(config: AuditConfig, timestamp: str) -> Path:
    """
    Create and return the audit run directory

    Args:
        config: Audit configuration
        timestamp: Timestamp string for directory name

    Returns:
        Path to the run directory
    """
    run_dir = config.output_dir / f"audit_run_{timestamp}"
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def get_tables_to_audit(config: AuditConfig) -> list:
    """
    Get list of tables to audit (from config or auto-discovery)

    Args:
        config: Audit configuration

    Returns:
        List of table names to audit
    """
    tables = config.tables

    # Auto-discover if enabled and no explicit list
    if not tables and config.auto_discover:
        tables = discover_tables(config)
        if not tables:
            print(f"No tables match the filter criteria")
            sys.exit(0)

    return tables


def setup_database_connection(
    config: AuditConfig,
    tables_to_audit: list,
    audit_mode: str,
    auto_yes: bool
) -> DatabaseConnection:
    """
    Setup database connection with optional cost estimation for BigQuery

    Args:
        config: Audit configuration
        tables_to_audit: List of tables to audit
        audit_mode: Audit mode string
        auto_yes: Whether to auto-approve prompts

    Returns:
        Database connection ready for auditing
    """
    db_conn = DatabaseConnection(config.backend, **config.connection_params)
    db_conn.connect()

    # Estimate costs for BigQuery
    if config.backend == 'bigquery' and audit_mode != 'discover':
        estimate_result = estimate_bigquery_costs(db_conn, tables_to_audit, config)

        # Get user confirmation if estimates available
        if estimate_result['table_estimates']:
            if not get_user_confirmation(auto_yes):
                sys.exit(0)
    else:
        # For non-BigQuery or discovery mode, just prefetch metadata
        prefetch_metadata(db_conn, tables_to_audit, config)

    return db_conn


def audit_tables(
    auditor: SecureTableAuditor,
    tables_to_audit: list,
    config: AuditConfig,
    audit_mode: str,
    run_dir: Path,
    db_conn: DatabaseConnection,
    logger: logging.Logger
) -> list:
    """
    Audit all tables and generate reports

    Args:
        auditor: SecureTableAuditor instance
        tables_to_audit: List of table names
        config: Audit configuration
        audit_mode: Audit mode string
        run_dir: Run directory path
        db_conn: Database connection
        logger: Logger instance

    Returns:
        List of audit results for each table
    """
    all_table_results = []

    for table in tables_to_audit:
        logger.info(f"Auditing table: {table}")

        try:
            # Get table-specific configuration
            sampling_config = config.get_table_sampling_config(table)
            custom_query = config.table_queries.get(table, None)
            user_defined_primary_key = config.table_primary_keys.get(table, None)
            table_schema = config.get_table_schema(table)

            # Run audit
            results = auditor.audit_from_database(
                table_name=table,
                backend=config.backend,
                connection_params=config.connection_params,
                schema=table_schema,
                mask_pii=config.mask_pii,
                custom_pii_keywords=config.custom_pii_keywords,
                user_primary_key=user_defined_primary_key,
                column_check_config=config,
                sampling_method=sampling_config['method'],
                sampling_key_column=sampling_config['key_column'],
                custom_query=custom_query,
                audit_mode=audit_mode,
                store_dataframe=config.relationship_detection_enabled,
                db_conn=db_conn
            )

            # Add config metadata to results (for display in reports)
            results['config_metadata'] = {
                'version': config.audit_version,
                'project': config.audit_project,
                'description': config.audit_description,
                'last_modified': config.audit_last_modified
            }

            all_table_results.append(results)

            # Log phase timings if available
            if 'phase_timings' in results:
                logger.info("Table Audit Phase Breakdown:")
                for phase, duration in results['phase_timings'].items():
                    logger.info(f"  {phase.replace('_', ' ').title()}: {duration:.3f}s")
                total_table_duration = sum(results['phase_timings'].values())
                logger.info(f"  Total Table Audit: {total_table_duration:.2f}s")

            # Create table-specific directory and export results
            table_dir = run_dir / table
            table_dir.mkdir(parents=True, exist_ok=True)

            # Export to various formats based on config
            if 'html' in config.export_formats:
                output_file = table_dir / 'audit.html'
                auditor.export_results_to_html(results, str(output_file))

            if 'json' in config.export_formats:
                output_file = table_dir / 'audit.json'
                auditor.export_results_to_json(results, str(output_file))

            if 'csv' in config.export_formats:
                # Export issues CSV
                output_file = table_dir / 'audit.csv'
                df = auditor.export_results_to_dataframe(results)
                df.write_csv(str(output_file))
                print(f"CSV saved to: {output_file}")

                # Export column summary CSV
                summary_file = table_dir / 'summary.csv'
                summary_df = auditor.export_column_summary_to_dataframe(results)
                summary_df.write_csv(str(summary_file))
                print(f"Column summary CSV saved to: {summary_file}")

        except Exception as e:
            logger.error(f"Error auditing table {table}: {e}")
            print(f"❌ Error auditing table {table}: {e}")
            import traceback
            traceback.print_exc()
            continue

    return all_table_results


def detect_and_export_relationships(
    all_table_results: list,
    config: AuditConfig,
    run_dir: Path,
    logger: logging.Logger
) -> list:
    """
    Detect relationships between tables and generate exports

    Args:
        all_table_results: List of audit results
        config: Audit configuration
        run_dir: Run directory path
        logger: Logger instance

    Returns:
        List of detected relationships
    """
    from dw_auditor.analysis.relationship_detector import detect_and_display_relationships

    # Detect and display relationships
    detected_relationships = detect_and_display_relationships(
        all_table_results,
        confidence_threshold=config.relationship_confidence_threshold
    )

    return detected_relationships


def generate_summary_reports(
    auditor: SecureTableAuditor,
    all_table_results: list,
    config: AuditConfig,
    run_dir: Path,
    total_duration: float,
    detected_relationships: list,
    logger: logging.Logger
) -> None:
    """
    Generate summary reports across all audited tables

    Args:
        auditor: SecureTableAuditor instance
        all_table_results: List of audit results
        config: Audit configuration
        run_dir: Run directory path
        total_duration: Total audit duration in seconds
        detected_relationships: List of detected relationships
        logger: Logger instance
    """
    import polars as pl

    print(f"\n{'='*70}")
    print(f"📊 Generating run summary reports...")
    print(f"{'='*70}")

    # Export summary in all configured formats
    if 'html' in config.export_formats:
        summary_html = run_dir / 'summary.html'
        auditor.export_run_summary_to_html(all_table_results, str(summary_html), detected_relationships, total_duration)
        print(f"📄 Summary HTML saved to: {summary_html}")
        if detected_relationships:
            print(f"   └─ Includes interactive relationship diagram with {len(detected_relationships)} relationship(s)")

    if 'json' in config.export_formats:
        summary_json = run_dir / 'summary.json'
        auditor.export_run_summary_to_json(all_table_results, str(summary_json), detected_relationships)
        print(f"📄 Summary JSON saved to: {summary_json}")

    if 'csv' in config.export_formats:
        # Export detailed column summary for all tables
        columns_csv = run_dir / 'columns.csv'
        columns_df = auditor.export_combined_column_summary_to_dataframe(all_table_results)
        columns_df.write_csv(str(columns_csv))
        print(f"📄 Detailed column summary CSV saved to: {columns_csv}")

        # Also export high-level table summary
        tables_csv = run_dir / 'tables.csv'
        tables_df = auditor.export_run_summary_to_dataframe(all_table_results)
        tables_df.write_csv(str(tables_csv))
        print(f"📄 Table summary CSV saved to: {tables_csv}")

        # Export relationships CSV if detected
        if detected_relationships:
            relationships_csv = run_dir / 'relationships.csv'
            relationships_df = pl.DataFrame(detected_relationships)
            relationships_df.write_csv(str(relationships_csv))
            print(f"📄 Relationships CSV saved to: {relationships_csv}")


def main():
    """Main entry point for audit CLI"""
    # Parse arguments
    parser = setup_argument_parser()
    args = parser.parse_args()

    # Validate config file exists
    config_file = args.config_file
    if not Path(config_file).exists():
        print(f"❌ Config file not found: {config_file}")
        print(f"Usage: python audit.py [config_file] [--discover]")
        sys.exit(1)

    # Determine audit mode
    audit_mode = determine_audit_mode(args)

    # Start timing
    total_start_time = datetime.now()
    run_timestamp = total_start_time.strftime("%Y%m%d_%H%M%S")

    # Load configuration
    print(f"📋 Loading config from: {config_file}")
    config = AuditConfig.from_yaml(config_file)

    # Initialize run directory
    run_dir = initialize_run_directory(config, run_timestamp)

    # Setup logging
    log_file = run_dir / "audit.log"
    setup_logging(log_file, log_level=args.log_level)
    logger = logging.getLogger(__name__)

    print(f"📁 Audit run directory: {run_dir}")
    print(f"📝 Logs will be saved to: {log_file}")

    # Print mode info
    print_mode_info(audit_mode)

    # Create auditor
    auditor = SecureTableAuditor(
        sample_size=config.sample_size
    )

    # Get tables to audit
    tables_to_audit = get_tables_to_audit(config)

    # Setup database connection (with cost estimation for BigQuery)
    db_conn = setup_database_connection(config, tables_to_audit, audit_mode, args.yes)

    try:
        # Audit all tables
        all_table_results = audit_tables(
            auditor, tables_to_audit, config, audit_mode,
            run_dir, db_conn, logger
        )

        # Detect relationships
        detected_relationships = []
        if all_table_results and audit_mode != 'discover' and config.relationship_detection_enabled and len(all_table_results) >= 2:
            detected_relationships = detect_and_export_relationships(all_table_results, config, run_dir, logger)

        # Generate summary reports
        if all_table_results:
            total_duration = (datetime.now() - total_start_time).total_seconds()
            generate_summary_reports(auditor, all_table_results, config, run_dir, total_duration, detected_relationships, logger)

    finally:
        db_conn.close()

    # Print completion message
    total_duration = (datetime.now() - total_start_time).total_seconds()
    logger.info(f"Audit completed! Results saved to: {run_dir}")
    logger.info(f"Total duration: {total_duration:.2f} seconds")
    logger.info(f"Logs saved to: {log_file}")


if __name__ == '__main__':
    main()
