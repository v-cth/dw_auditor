"""
Custom exceptions for exporters module
"""


class ExporterError(Exception):
    """Base exception for all exporter errors"""
    pass


class InvalidResultsError(ExporterError):
    """Raised when audit results dictionary has invalid structure"""
    pass


class FileExportError(ExporterError):
    """Raised when file export operations fail"""
    pass


class PathValidationError(ExporterError):
    """Raised when file path is invalid or unsafe"""
    pass
