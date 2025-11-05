"""
Type definitions for audit results structure
"""

from typing import TypedDict, List, Any, NotRequired


class IssueDict(TypedDict):
    """Structure for a single audit issue"""
    type: str
    count: NotRequired[int]
    pct: NotRequired[float]
    suggestion: NotRequired[str]
    examples: NotRequired[List[Any]]
    special_chars: NotRequired[List[str]]


class ColumnDataDict(TypedDict):
    """Structure for column audit data"""
    dtype: str
    null_count: int
    null_pct: float
    issues: List[IssueDict]


class AuditResultsDict(TypedDict):
    """Structure for complete audit results"""
    table_name: str
    total_rows: int
    analyzed_rows: int
    sampled: bool
    columns: dict[str, ColumnDataDict]
    timestamp: NotRequired[str]
