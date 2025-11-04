"""
Table discovery utilities
"""

from typing import List, TYPE_CHECKING

if TYPE_CHECKING:
    from dw_auditor.core.config import AuditConfig


def discover_tables(config: 'AuditConfig') -> List[str]:
    """
    Auto-discover tables in the schema using filter patterns

    Args:
        config: Audit configuration

    Returns:
        List of table names that match the filter criteria
    """
    from dw_auditor.core.db_connection import DatabaseConnection

    print(f"\nAuto-discovering tables in schema...")

    # Create temporary database connection for discovery
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
            print(f"Filtered out {excluded_count} tables based on patterns")
        print(f"Will audit {len(tables_to_audit)} tables")

        # Show excluded tables if there are any
        if excluded_count > 0 and excluded_count <= 10:
            excluded_tables = [t for t in all_tables if not config.should_include_table(t)]
            print(f"   Excluded: {', '.join(excluded_tables)}")

        return tables_to_audit

    finally:
        db_conn.close()
