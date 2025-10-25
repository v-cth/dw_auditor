#!/usr/bin/env python
"""
Simple CLI for running database audits
Usage: python audit.py [config_file] [--discover]
"""

import sys
import argparse
import webbrowser
from pathlib import Path
from dw_auditor import AuditConfig, SecureTableAuditor


class TeeLogger:
    """Write output to both console and file"""
    def __init__(self, log_file):
        self.terminal = sys.stdout
        self.terminal_err = sys.stderr
        self.log = open(log_file, 'w', encoding='utf-8')

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
        self.log.flush()

    def flush(self):
        self.terminal.flush()
        self.log.flush()

    def close(self):
        self.log.close()


def main():
    from datetime import datetime

    # Parse command-line arguments
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
        """
    )
    parser.add_argument(
        'config_file',
        nargs='?',
        default='audit_config.yaml',
        help='Path to YAML configuration file (default: audit_config.yaml)'
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

    args = parser.parse_args()
    config_file = args.config_file

    # Determine audit mode
    if args.discover:
        audit_mode = 'discover'
    elif args.check:
        audit_mode = 'checks'
    elif args.insight:
        audit_mode = 'insights'
    else:
        audit_mode = 'full'

    # Start total timing
    total_start_time = datetime.now()

    if not Path(config_file).exists():
        print(f"❌ Config file not found: {config_file}")
        print(f"Usage: python audit.py [config_file] [--discover]")
        sys.exit(1)

    # Load config
    print(f"📋 Loading config from: {config_file}")
    config = AuditConfig.from_yaml(config_file)

    # Create audit run directory with timestamp
    run_timestamp = total_start_time.strftime("%Y%m%d_%H%M%S")
    run_dir = config.output_dir / f"audit_run_{run_timestamp}"
    run_dir.mkdir(parents=True, exist_ok=True)

    # Set up logging to file
    log_file = run_dir / "audit.log"
    logger = TeeLogger(log_file)
    sys.stdout = logger
    sys.stderr = logger  # Also redirect stderr to capture tracebacks

    print(f"📁 Audit run directory: {run_dir}")
    print(f"📝 Logs will be saved to: {log_file}")

    # Show mode
    if audit_mode == 'discover':
        print(f"🔍 Discovery mode: Collecting metadata only (skipping quality checks and insights)")
    elif audit_mode == 'checks':
        print(f"✓ Check mode: Running quality checks only (skipping profiling/insights)")
    elif audit_mode == 'insights':
        print(f"📊 Insight mode: Running profiling/insights only (skipping quality checks)")
    else:
        print(f"🔍 Full audit mode: Running quality checks and profiling/insights")

    # Create auditor
    auditor = SecureTableAuditor(
        sample_size=config.sample_size,
        sample_threshold=config.sample_threshold,
        min_year=config.min_year,
        max_year=config.max_year,
        outlier_threshold_pct=config.outlier_threshold_pct
    )

    # Determine tables to audit
    tables_to_audit = config.tables

    # Auto-discover tables if enabled and no explicit list provided
    if not tables_to_audit and config.auto_discover:
        print(f"\n🔍 Auto-discovering tables in schema...")

        # Create temporary database connection for discovery
        from dw_auditor.core.database import DatabaseConnection
        db_conn = DatabaseConnection(config.backend, **config.connection_params)
        db_conn.connect()

        try:
            all_tables = db_conn.get_all_tables(config.schema)
            print(f"📋 Found {len(all_tables)} tables in schema")

            # Apply filters
            tables_to_audit = [t for t in all_tables if config.should_include_table(t)]

            # Show filtering results
            excluded_count = len(all_tables) - len(tables_to_audit)
            if excluded_count > 0:
                print(f"🔽 Filtered out {excluded_count} tables based on patterns")
            print(f"✅ Will audit {len(tables_to_audit)} tables")

            # Show excluded tables if there are any
            if excluded_count > 0 and excluded_count <= 10:
                excluded_tables = [t for t in all_tables if not config.should_include_table(t)]
                print(f"   Excluded: {', '.join(excluded_tables)}")

        finally:
            db_conn.close()

        if not tables_to_audit:
            print(f"⚠️  No tables match the filter criteria")
            sys.exit(0)

    # Estimate BigQuery bytes scanned and get user confirmation (BigQuery only)
    if config.backend == 'bigquery' and audit_mode != 'discover':
        print(f"\n{'='*70}")
        print(f"💰 Estimating BigQuery scan costs...")
        print(f"{'='*70}")

        # Create temporary connection for estimation
        from dw_auditor.core.database import DatabaseConnection
        db_conn = DatabaseConnection(config.backend, **config.connection_params)
        db_conn.connect()

        total_bytes = 0
        table_estimates = []

        try:
            for table in tables_to_audit:
                # Get table-specific configuration
                sampling_config = config.get_table_sampling_config(table)
                custom_query = config.table_queries.get(table, None)

                # Determine if we'll sample based on same logic as audit
                row_count = None
                is_cross_project = hasattr(db_conn, 'source_project_id') and db_conn.source_project_id

                if not custom_query and not is_cross_project:
                    try:
                        row_count = db_conn.get_row_count(table, config.schema)
                    except:
                        pass

                # Determine sample size for estimation
                should_sample = False
                sample_size = None

                if custom_query:
                    should_sample = False
                elif is_cross_project:
                    should_sample = (row_count is None or row_count > config.sample_threshold)
                    sample_size = config.sample_size if should_sample else None
                elif row_count and row_count > config.sample_threshold:
                    should_sample = True
                    sample_size = config.sample_size

                # Determine which columns will be loaded (optimization)
                columns_to_load = None
                try:
                    # Get primary key from config
                    user_defined_primary_key = config.table_primary_keys.get(table, None)

                    # Get table schema
                    table_schema = db_conn.get_table_schema(table, config.schema)

                    if table_schema:
                        # Determine columns using same logic as audit
                        columns_to_load = auditor.determine_columns_to_load(
                            table_schema=table_schema,
                            table_name=table,
                            column_check_config=config,
                            primary_key_columns=user_defined_primary_key,
                            include_columns=getattr(config, 'include_columns', None),
                            exclude_columns=getattr(config, 'exclude_columns', None),
                            audit_mode=audit_mode
                        )
                except Exception as e:
                    # If column determination fails, estimate will use all columns
                    pass

                # Estimate bytes for this table
                bytes_estimate = db_conn.estimate_bytes_scanned(
                    table_name=table,
                    schema=config.schema,
                    custom_query=custom_query,
                    sample_size=sample_size,
                    sampling_method=sampling_config['method'],
                    sampling_key_column=sampling_config['key_column'],
                    columns=columns_to_load if columns_to_load else None
                )

                if bytes_estimate is not None:
                    total_bytes += bytes_estimate
                    table_estimates.append({
                        'table': table,
                        'bytes': bytes_estimate,
                        'sampled': should_sample
                    })

        finally:
            db_conn.close()

        # Display estimates
        if table_estimates:
            print(f"\n📊 Estimated bytes to scan per table:")
            for est in table_estimates:
                bytes_val = est['bytes']
                if bytes_val >= 1_000_000_000_000:  # TB
                    size_str = f"{bytes_val / 1_000_000_000_000:.2f} TB"
                elif bytes_val >= 1_000_000_000:  # GB
                    size_str = f"{bytes_val / 1_000_000_000:.2f} GB"
                elif bytes_val >= 1_000_000:  # MB
                    size_str = f"{bytes_val / 1_000_000:.2f} MB"
                else:
                    size_str = f"{bytes_val / 1_000:.2f} KB"

                sample_indicator = " (sampled)" if est['sampled'] else ""
                print(f"   • {est['table']}: {size_str}{sample_indicator}")

            # Calculate total and cost
            total_gb = total_bytes / 1_000_000_000
            total_tb = total_bytes / 1_000_000_000_000

            # BigQuery on-demand pricing: $6.25 per TB (as of 2024)
            estimated_cost = max(0, total_tb * 6.25)

            print(f"\n💾 Total estimated data to scan: {total_gb:.2f} GB ({total_tb:.4f} TB)")
            print(f"💵 Estimated cost (on-demand): ${estimated_cost:.2f} USD")

            # Ask for confirmation
            print(f"\n⚠️  Do you want to proceed with the audit?")
            try:
                response = input(f"   Type 'y' or 'yes' to continue, or 'n' to cancel: ").strip().lower()
            except (EOFError, KeyboardInterrupt):
                # Handle non-interactive environments or Ctrl+C
                print(f"\n⚠️  Running in non-interactive mode, proceeding with audit...")
                print(f"   (Use --yes flag in future to skip this prompt)")
                response = 'yes'

            if response not in ['y', 'yes']:
                print(f"\n❌ Audit cancelled by user")
                sys.exit(0)

            print(f"\n✅ Proceeding with audit...")
        else:
            print(f"⚠️  Could not estimate bytes (will proceed without confirmation)")

    # Audit tables
    all_table_results = []
    try:
        for table in tables_to_audit:
            print(f"\n{'='*70}")
            print(f"🔍 Auditing table: {table}")
            print(f"{'='*70}")

            try:
                # Get primary key from config if specified
                user_defined_primary_key = config.table_primary_keys.get(table, None)

                # Get table-specific sampling configuration
                sampling_config = config.get_table_sampling_config(table)

                # Get custom query from config if specified
                custom_query = config.table_queries.get(table, None)

                # Get table-specific schema (falls back to global schema if not specified)
                table_schema = config.get_table_schema(table)

                results = auditor.audit_from_database(
                    table_name=table,
                    backend=config.backend,
                    connection_params=config.connection_params,
                    schema=table_schema,
                    mask_pii=config.mask_pii,
                    custom_pii_keywords=config.custom_pii_keywords,
                    user_primary_key=user_defined_primary_key,
                    column_check_config=config,  # Pass entire config for column-level checks
                    sampling_method=sampling_config['method'],
                    sampling_key_column=sampling_config['key_column'],
                    custom_query=custom_query,
                    audit_mode=audit_mode,
                    store_dataframe=config.relationship_detection_enabled  # Store DataFrame for relationship detection
                )

                # Add config metadata to results (for display in reports)
                results['config_metadata'] = {
                    'version': config.audit_version,
                    'project': config.audit_project,
                    'description': config.audit_description,
                    'last_modified': config.audit_last_modified
                }

                # Store results for summary generation
                all_table_results.append(results)

                # Print phase timings if available
                if 'phase_timings' in results:
                    print(f"\n⏱️  Table Audit Phase Breakdown:")
                    for phase, duration in results['phase_timings'].items():
                        print(f"   • {phase.replace('_', ' ').title()}: {duration:.3f}s")
                    total_table_duration = results.get('duration_seconds', 0)
                    print(f"   • Total Table Audit: {total_table_duration:.2f}s")

                # Create table-specific directory
                table_dir = run_dir / table
                table_dir.mkdir(parents=True, exist_ok=True)

                # Export results to table directory
                if 'html' in config.export_formats:
                    output_file = table_dir / 'audit.html'
                    auditor.export_results_to_html(results, str(output_file))

                    # Auto-open HTML in browser if configured
                    if config.auto_open_html:
                        try:
                            webbrowser.open(f'file://{output_file.absolute()}')
                            print(f"🌐 Opened report in browser")
                        except Exception as e:
                            print(f"⚠️  Could not open browser: {e}")

                if 'json' in config.export_formats:
                    output_file = table_dir / 'audit.json'
                    auditor.export_results_to_json(results, str(output_file))

                if 'csv' in config.export_formats:
                    # Export issues CSV
                    output_file = table_dir / 'audit.csv'
                    df = auditor.export_results_to_dataframe(results)
                    df.write_csv(str(output_file))
                    print(f"📄 CSV saved to: {output_file}")

                    # Export column summary CSV
                    summary_file = table_dir / 'summary.csv'
                    summary_df = auditor.export_column_summary_to_dataframe(results)
                    summary_df.write_csv(str(summary_file))
                    print(f"📄 Column summary CSV saved to: {summary_file}")

            except Exception as e:
                print(f"❌ Error auditing table '{table}': {e}")
                import traceback
                traceback.print_exc()
                continue

        # Detect table relationships if enabled
        detected_relationships = []
        if config.relationship_detection_enabled and len(all_table_results) >= 2:
            print(f"\n{'='*70}")
            print(f"🔗 Detecting table relationships...")
            print(f"{'='*70}")

            try:
                from dw_auditor.analysis import PolarsRelationshipDetector

                detector = PolarsRelationshipDetector()

                # Add all audited tables with their DataFrames
                for result in all_table_results:
                    if 'data' in result:  # Check if DataFrame was stored
                        detector.add_table(result['table_name'], result['data'])
                        print(f"   Added table: {result['table_name']}")

                # Detect relationships
                detected_relationships = detector.detect_relationships(
                    confidence_threshold=config.relationship_confidence_threshold
                )

                print(f"\n✅ Found {len(detected_relationships)} relationships (confidence >= {config.relationship_confidence_threshold:.0%})")

                # Show detected relationships
                if detected_relationships:
                    print(f"\nDetected Relationships:")
                    for rel in sorted(detected_relationships, key=lambda x: x['confidence'], reverse=True):
                        print(f"   • {rel['table1']}.{rel['column1']} ↔ {rel['table2']}.{rel['column2']}")
                        print(f"     Confidence: {rel['confidence']:.1%} | Type: {rel['relationship_type']} | Matching values: {rel['matching_values']}")

                # Remove DataFrames from results to save memory (no longer needed)
                for result in all_table_results:
                    if 'data' in result:
                        del result['data']

            except Exception as e:
                print(f"⚠️  Error detecting relationships: {e}")
                import traceback
                traceback.print_exc()

        # Generate run-level summary reports
        if all_table_results:
            print(f"\n{'='*70}")
            print(f"📊 Generating run summary reports...")
            print(f"{'='*70}")

            # Export summary in all configured formats
            if 'html' in config.export_formats:
                summary_html = run_dir / 'summary.html'
                auditor.export_run_summary_to_html(all_table_results, str(summary_html), detected_relationships)
                print(f"📄 Summary HTML saved to: {summary_html}")

                # Generate standalone interactive relationships report if relationships were detected
                if detected_relationships:
                    relationships_html = run_dir / 'relationships_interactive.html'
                    # This will be implemented when we create relationships.py
                    print(f"📄 Interactive relationships report: {relationships_html}")

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
                    import polars as pl
                    relationships_csv = run_dir / 'relationships.csv'
                    relationships_df = pl.DataFrame(detected_relationships)
                    relationships_df.write_csv(str(relationships_csv))
                    print(f"📄 Relationships CSV saved to: {relationships_csv}")

        # Calculate total duration
        total_end_time = datetime.now()
        total_duration = (total_end_time - total_start_time).total_seconds()

        print(f"\n{'='*70}")
        print(f"✅ Audit completed! Results saved to: {run_dir}")
        print(f"⏱️  Total duration: {total_duration:.2f} seconds")
        print(f"📝 Logs saved to: {log_file}")
        print(f"{'='*70}")

    finally:
        # Restore stdout/stderr and close log file
        sys.stdout = logger.terminal
        sys.stderr = logger.terminal_err
        logger.close()


if __name__ == "__main__":
    main()
