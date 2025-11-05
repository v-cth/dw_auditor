"""
Database connection orchestrator that delegates to backend adapters
"""

import ibis
import polars as pl
import logging
from typing import Optional, List, Dict, Any

from .bigquery import BigQueryAdapter
from .snowflake import SnowflakeAdapter

logger = logging.getLogger(__name__)


ADAPTERS = {
    'bigquery': BigQueryAdapter,
    'snowflake': SnowflakeAdapter
}


class DatabaseConnection:
    """Thin orchestrator that delegates to backend-specific adapters"""

    SUPPORTED_BACKENDS = list(ADAPTERS.keys())

    def __init__(self, backend: str, **connection_params):
        """
        Initialize database connection

        Args:
            backend: Database backend ('bigquery' or 'snowflake')
            **connection_params: Backend-specific connection parameters
                For BigQuery cross-project queries:
                    project_id: Your billing project
                    source_project_id: Source project containing the data (optional)
                    schema: Default dataset/schema
        """
        if backend.lower() not in self.SUPPORTED_BACKENDS:
            raise ValueError(
                f"Backend '{backend}' not supported. "
                f"Supported backends: {', '.join(self.SUPPORTED_BACKENDS)}"
            )

        self.backend = backend.lower()
        self.connection_params = connection_params

        # Create adapter instance
        adapter_class = ADAPTERS[self.backend]
        self.adapter = adapter_class(**connection_params)

        # Expose source_project_id for backward compatibility
        if hasattr(self.adapter, 'source_project_id'):
            self.source_project_id = self.adapter.source_project_id

        self.conn = None

    def connect(self) -> ibis.BaseBackend:
        """Establish database connection"""
        self.conn = self.adapter.connect()
        return self.conn

    def get_table(self, table_name: str, schema: Optional[str] = None) -> ibis.expr.types.Table:
        """Get table reference"""
        return self.adapter.get_table(table_name, schema)

    def execute_query(
        self,
        table_name: str,
        schema: Optional[str] = None,
        limit: Optional[int] = None,
        custom_query: Optional[str] = None,
        sample_size: Optional[int] = None,
        sampling_method: str = 'random',
        sampling_key_column: Optional[str] = None,
        columns: Optional[List[str]] = None
    ) -> pl.DataFrame:
        """Execute query and return Polars DataFrame"""
        return self.adapter.execute_query(
            table_name=table_name,
            schema=schema,
            limit=limit,
            custom_query=custom_query,
            sample_size=sample_size,
            sampling_method=sampling_method,
            sampling_key_column=sampling_key_column,
            columns=columns
        )

    def get_all_tables(self, schema: Optional[str] = None) -> List[str]:
        """Get list of all tables in the schema"""
        return self.adapter.get_all_tables(schema)

    def get_table_metadata(self, table_name: str, schema: Optional[str] = None) -> Dict[str, Any]:
        """Get table metadata"""
        return self.adapter.get_table_metadata(table_name, schema)

    def get_primary_key_columns(self, table_name: str, schema: Optional[str] = None) -> List[str]:
        """Get primary key column names"""
        return self.adapter.get_primary_key_columns(table_name, schema)

    def get_table_schema(self, table_name: str, schema: Optional[str] = None) -> Dict[str, str]:
        """Get table column names and their data types"""
        return self.adapter.get_table_schema(table_name, schema)

    def estimate_bytes_scanned(
        self,
        table_name: str,
        schema: Optional[str] = None,
        custom_query: Optional[str] = None,
        sample_size: Optional[int] = None,
        sampling_method: str = 'random',
        sampling_key_column: Optional[str] = None,
        columns: Optional[List[str]] = None
    ) -> Optional[int]:
        """Estimate bytes that will be scanned (BigQuery only)"""
        return self.adapter.estimate_bytes_scanned(
            table_name=table_name,
            schema=schema,
            custom_query=custom_query,
            sample_size=sample_size,
            sampling_method=sampling_method,
            sampling_key_column=sampling_key_column,
            columns=columns
        )

    def get_row_count(self, table_name: str, schema: Optional[str] = None, approximate: bool = True) -> Optional[int]:
        """Get row count for a table"""
        return self.adapter.get_row_count(table_name, schema, approximate)

    def list_tables(self, schema: Optional[str] = None) -> List[str]:
        """List available tables"""
        return self.adapter.list_tables(schema)

    def prefetch_metadata(self, schema: str, table_names: List[str]):
        """
        Pre-fetch metadata for specific tables (recommended for multi-table audits)

        Args:
            schema: Schema/dataset name
            table_names: List of table names to fetch metadata for
        """
        return self.adapter.prefetch_metadata(schema, table_names)

    def close(self):
        """Close database connection"""
        self.adapter.close()
        self.conn = None
        logger.info(f"Closed {self.backend.upper()} connection")

    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()


def create_connection(backend: str, **connection_params) -> DatabaseConnection:
    """
    Factory function to create database connection

    Args:
        backend: Database backend ('bigquery' or 'snowflake')
        **connection_params: Backend-specific connection parameters

    Returns:
        DatabaseConnection instance

    Examples:
        # BigQuery
        conn = create_connection(
            'bigquery',
            project_id='my-project',
            schema='my_dataset',
            credentials_path='/path/to/credentials.json'
        )

        # Snowflake
        conn = create_connection(
            'snowflake',
            account='my-account',
            user='my-user',
            password='my-password',
            database='my_db',
            schema='my_schema',
            warehouse='my_warehouse'
        )
    """
    return DatabaseConnection(backend, **connection_params)
