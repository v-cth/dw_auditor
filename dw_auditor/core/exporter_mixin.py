"""
Mixin for exporting audit results in various formats.
"""

from typing import Dict, Optional, List
import polars as pl

from ..exporters.dataframe_export import export_to_dataframe
from ..exporters.html import export_to_html
from ..exporters.json_export import export_to_json
from ..exporters.summary_export import (
    export_column_summary_to_dataframe,
    export_combined_column_summary_to_dataframe
)
from ..exporters.run_summary_export import (
    export_run_summary_to_dataframe,
    export_run_summary_to_json,
    export_run_summary_to_html
)
from ..utils.output import get_summary_stats


class AuditorExporterMixin:
    """Mixin class providing methods to export audit results."""

    def export_results_to_dataframe(self, results: Dict) -> pl.DataFrame:
        """
        Export audit results to a Polars DataFrame for easy analysis

        Returns:
            DataFrame with one row per issue found
        """
        return export_to_dataframe(results)

    def export_results_to_json(self, results: Dict, file_path: Optional[str] = None) -> str:
        """
        Export audit results to JSON

        Args:
            results: Audit results dictionary
            file_path: Optional path to save JSON file

        Returns:
            JSON string
        """
        return export_to_json(results, file_path)

    def export_results_to_html(self, results: Dict, file_path: str = "audit_report.html", thousand_separator: str = ",", decimal_places: int = 1) -> str:
        """
        Export audit results to a beautiful HTML report

        Args:
            results: Audit results dictionary
            file_path: Path to save HTML file
            thousand_separator: Separator for thousands (default: ",")
            decimal_places: Number of decimal places to display (default: 1)

        Returns:
            Path to saved HTML file
        """
        return export_to_html(results, file_path, thousand_separator, decimal_places)

    def get_summary_stats(self, results: Dict) -> Dict:
        """
        Get high-level summary statistics from audit results

        Returns:
            Dictionary with summary statistics
        """
        return get_summary_stats(results)

    def export_column_summary_to_dataframe(self, results: Dict) -> pl.DataFrame:
        """
        Export column summary to a Polars DataFrame

        Args:
            results: Audit results dictionary

        Returns:
            DataFrame with one row per column with basic metrics (null count, null %, distinct count)
        """
        return export_column_summary_to_dataframe(results)

    def export_run_summary_to_dataframe(self, all_results: List[Dict]) -> pl.DataFrame:
        """
        Export run-level summary to a Polars DataFrame

        Args:
            all_results: List of audit results dictionaries (one per table)

        Returns:
            DataFrame with one row per table showing high-level metrics
        """
        return export_run_summary_to_dataframe(all_results)

    def export_run_summary_to_json(self, all_results: List[Dict], file_path: Optional[str] = None, relationships: List[Dict] = None) -> Dict:
        """
        Export run-level summary to JSON

        Args:
            all_results: List of audit results dictionaries (one per table)
            file_path: Optional path to save JSON file
            relationships: Optional list of detected relationships

        Returns:
            Dictionary with run summary
        """
        return export_run_summary_to_json(all_results, file_path, relationships)

    def export_run_summary_to_html(self, all_results: List[Dict], file_path: str = "summary.html", relationships: List[Dict] = None, total_duration: float = None) -> str:
        """
        Export run-level summary to HTML dashboard

        Args:
            all_results: List of audit results dictionaries (one per table)
            file_path: Path to save HTML file
            relationships: Optional list of detected relationships
            total_duration: Optional total audit duration in seconds

        Returns:
            Path to saved HTML file
        """
        return export_run_summary_to_html(all_results, file_path, relationships, total_duration)

    def export_combined_column_summary_to_dataframe(self, all_results: List[Dict]) -> pl.DataFrame:
        """
        Export combined column summary for all tables to a single Polars DataFrame

        Args:
            all_results: List of audit results dictionaries (one per table)

        Returns:
            DataFrame with one row per column across all tables with detailed metrics
        """
        return export_combined_column_summary_to_dataframe(all_results)
