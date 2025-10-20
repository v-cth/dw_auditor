#!/usr/bin/env python
"""
Simple CLI for running database audits
Usage: python audit.py [config_file]
"""

import sys
from pathlib import Path
from dw_auditor import AuditConfig, SecureTableAuditor


class TeeLogger:
    """Write output to both console and file"""
    def __init__(self, log_file):
        self.terminal = sys.stdout
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

    # Start total timing
    total_start_time = datetime.now()

    # Default config file
    config_file = sys.argv[1] if len(sys.argv) > 1 else 'audit_config.yaml'

    if not Path(config_file).exists():
        print(f"❌ Config file not found: {config_file}")
        print(f"Usage: python audit.py [config_file]")
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

    print(f"📁 Audit run directory: {run_dir}")
    print(f"📝 Logs will be saved to: {log_file}")

    # Create auditor
    auditor = SecureTableAuditor(
        sample_size=config.sample_size,
        sample_threshold=config.sample_threshold,
        min_year=config.min_year,
        max_year=config.max_year,
        outlier_threshold_pct=config.outlier_threshold_pct
    )

    # Audit tables
    try:
        for table in config.tables:
            print(f"\n{'='*70}")
            print(f"🔍 Auditing table: {table}")
            print(f"{'='*70}")

            try:
                # Get primary key from config if specified
                user_defined_primary_key = config.table_primary_keys.get(table, None)

                results = auditor.audit_from_database(
                    table_name=table,
                    backend=config.backend,
                    connection_params=config.connection_params,
                    schema=config.schema,
                    mask_pii=config.mask_pii,
                    custom_pii_keywords=config.custom_pii_keywords,
                    user_primary_key=user_defined_primary_key,
                    column_check_config=config  # Pass entire config for column-level checks
                )

                # Print phase timings if available
                if 'phase_timings' in results:
                    print(f"\n⏱️  Table Audit Phase Breakdown:")
                    for phase, duration in results['phase_timings'].items():
                        print(f"   • {phase.replace('_', ' ').title()}: {duration:.3f}s")
                    total_table_duration = results.get('duration_seconds', 0)
                    print(f"   • Total Table Audit: {total_table_duration:.2f}s")

                # Export results to run directory
                if 'html' in config.export_formats:
                    output_file = run_dir / f'{config.file_prefix}_{table}.html'
                    auditor.export_results_to_html(results, str(output_file))

                if 'json' in config.export_formats:
                    output_file = run_dir / f'{config.file_prefix}_{table}.json'
                    auditor.export_results_to_json(results, str(output_file))

                if 'csv' in config.export_formats:
                    # Export issues CSV
                    output_file = run_dir / f'{config.file_prefix}_{table}.csv'
                    df = auditor.export_results_to_dataframe(results)
                    df.write_csv(str(output_file))
                    print(f"📄 CSV saved to: {output_file}")

                    # Export column summary CSV
                    summary_file = run_dir / f'{config.file_prefix}_{table}_summary.csv'
                    summary_df = auditor.export_column_summary_to_dataframe(results)
                    summary_df.write_csv(str(summary_file))
                    print(f"📄 Column summary CSV saved to: {summary_file}")

            except Exception as e:
                print(f"❌ Error auditing table '{table}': {e}")
                import traceback
                traceback.print_exc()
                continue

        # Calculate total duration
        total_end_time = datetime.now()
        total_duration = (total_end_time - total_start_time).total_seconds()

        print(f"\n{'='*70}")
        print(f"✅ Audit completed! Results saved to: {run_dir}")
        print(f"⏱️  Total duration: {total_duration:.2f} seconds")
        print(f"📝 Logs saved to: {log_file}")
        print(f"{'='*70}")

    finally:
        # Restore stdout and close log file
        sys.stdout = logger.terminal
        logger.close()


if __name__ == "__main__":
    main()
