"""
BigQuery cost estimation utilities
"""

import sys
from typing import List, Dict, Any, TYPE_CHECKING
from collections import defaultdict
from .output import format_bytes, print_separator

if TYPE_CHECKING:
    from dw_auditor.core.config import AuditConfig
    from dw_auditor.core.db_connection import DatabaseConnection


def prefetch_metadata(
    db_conn: 'DatabaseConnection',
    tables_to_audit: List[str],
    config: 'AuditConfig'
) -> None:
    """
    Pre-fetch metadata for all tables to avoid duplicate queries

    Args:
        db_conn: Database connection
        tables_to_audit: List of table names to audit
        config: Audit configuration
    """
    if not tables_to_audit:
        return

    print(f"\nPre-fetching metadata for {len(tables_to_audit)} table(s)...")

    # Group tables by (project_id, schema) to handle multi-project and multi-schema audits
    tables_by_project_schema = defaultdict(list)
    for table in tables_to_audit:
        schema = config.get_table_schema(table)
        # Get table-specific connection params to extract project_id
        table_conn_params = config.get_table_connection_params(table)
        project_id = table_conn_params.get('project_id') if config.backend == 'bigquery' else None
        # Use (project_id, schema) as key
        key = (project_id, schema)
        tables_by_project_schema[key].append(table)

    # Prefetch filtered metadata per (project_id, schema) group
    for (project_id, schema), tables in tables_by_project_schema.items():
        db_conn.prefetch_metadata(schema, tables, project_id)

    print(f"Metadata cached for all tables")


def estimate_bigquery_costs(
    db_conn: 'DatabaseConnection',
    tables_to_audit: List[str],
    config: 'AuditConfig'
) -> Dict[str, Any]:
    """
    Estimate BigQuery bytes scanned and costs for the audit

    Args:
        db_conn: Database connection
        tables_to_audit: List of table names to audit
        config: Audit configuration

    Returns:
        Dictionary with 'total_bytes', 'table_estimates', 'cost'
    """
    print_separator()
    print(f"Estimating BigQuery scan costs...")
    print_separator()

    prefetch_metadata(db_conn, tables_to_audit, config)

    total_bytes = 0
    table_estimates = []

    for table in tables_to_audit:
        # Get table-specific configuration
        sampling_config = config.get_table_sampling_config(table)
        custom_query = config.table_queries.get(table, None)
        table_conn_params = config.get_table_connection_params(table)
        project_id = table_conn_params.get('project_id') if config.backend == 'bigquery' else None

        # Determine sample size for estimation
        # Note: We don't check row_count here to avoid duplicate metadata queries
        # (metadata is already prefetched above)
        should_sample = False
        sample_size = None
        if custom_query:
            should_sample = False

        # Temporarily set source_project_id for cross-project byte estimation
        original_source_project = getattr(db_conn.adapter, 'source_project_id', None)
        if project_id and hasattr(db_conn.adapter, 'source_project_id'):
            db_conn.adapter.source_project_id = project_id

        try:
            # Estimate bytes for this table
            bytes_estimate = db_conn.estimate_bytes_scanned(
                table_name=table,
                schema=config.get_table_schema(table),
                custom_query=custom_query,
                sample_size=sample_size,
                sampling_method=sampling_config['method'],
                sampling_key_column=sampling_config['key_column'],
                columns=None
            )
        finally:
            # Restore original source_project_id
            if hasattr(db_conn.adapter, 'source_project_id'):
                db_conn.adapter.source_project_id = original_source_project

        if bytes_estimate is not None:
            total_bytes += bytes_estimate
            table_estimates.append({
                'table': table,
                'bytes': bytes_estimate,
                'sampled': should_sample
            })

    # Display estimates
    if table_estimates:
        print(f"\nEstimated bytes to scan per table:")
        for est in table_estimates:
            size_str = format_bytes(est['bytes'])
            sample_indicator = " (sampled)" if est['sampled'] else ""
            print(f"   • {est['table']}: {size_str}{sample_indicator}")

        # Calculate total and cost
        total_gb = total_bytes / 1_000_000_000
        total_tb = total_bytes / 1_000_000_000_000

        # BigQuery on-demand pricing: $6.25 per TB (as of 2024)
        estimated_cost = max(0, total_tb * 6.25)

        print(f"\n💾 Total estimated data to scan: {total_gb:.2f} GB ({total_tb:.4f} TB)")
        print(f"💵 Estimated cost (on-demand): ${estimated_cost:.2f} USD")

        return {
            'total_bytes': total_bytes,
            'table_estimates': table_estimates,
            'cost': estimated_cost
        }
    else:
        print(f"⚠️  Could not estimate bytes (will proceed without confirmation)")
        return {
            'total_bytes': 0,
            'table_estimates': [],
            'cost': 0
        }


def get_user_confirmation(auto_yes: bool = False) -> bool:
    """
    Get user confirmation to proceed with audit

    Args:
        auto_yes: If True, automatically approve without prompting

    Returns:
        True if user confirms, False otherwise
    """
    if auto_yes:
        print(f"\n  Proceeding with audit (auto-approved via --yes)")
        return True

    print(f"\n  Do you want to proceed with the audit?")
    try:
        response = input(f"   Type 'y' or 'yes' to continue, or 'n' to cancel: ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        # Handle non-interactive environments or Ctrl+C
        print(f"\n  Running in non-interactive mode, proceeding with audit...")
        print(f"   (Use --yes flag in future to skip this prompt)")
        response = 'yes'

    if response not in ['y', 'yes']:
        print(f"\nAudit cancelled by user")
        return False

    print(f"\n Proceeding with audit...")
    return True
