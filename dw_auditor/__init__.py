"""
Data Warehouse Table Auditor - Secure Polars Edition
High-performance data quality checks with security best practices
"""

from importlib.metadata import version, PackageNotFoundError

from .core.auditor import SecureTableAuditor
from .core.config import AuditConfig

try:
    __version__ = version("dw-auditor")
except PackageNotFoundError:
    # Package not installed (development mode)
    __version__ = "0.0.0.dev"

__all__ = ["SecureTableAuditor", "AuditConfig"]
