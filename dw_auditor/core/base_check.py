"""
Base class and models for data quality checks
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Optional, Any, Type
from pydantic import BaseModel, Field
import polars as pl


class CheckResult(BaseModel):
    """Structured result from a data quality check

    Attributes:
        type: Issue type identifier (e.g., 'VALUE_NOT_GREATER_THAN')
        count: Number of rows violating the check
        pct: Percentage of rows violating the check (optional)
        examples: List of example values that violated the check
        suggestion: Human-readable suggestion for fixing the issue (optional)
        threshold: Threshold value used in the check (optional)
        operator: Comparison operator used (optional, e.g., '>', '<=')
        pattern: Pattern string for regex/string checks (optional)
        mode: Mode of operation for complex checks (optional)
        description: Custom description of the issue (optional)
    """
    type: str
    count: int
    pct: Optional[float] = None
    examples: List[Any] = Field(default_factory=list)
    suggestion: Optional[str] = None
    threshold: Optional[Any] = None
    operator: Optional[str] = None
    pattern: Optional[str] = None
    mode: Optional[str] = None
    description: Optional[str] = None


class BaseCheck(ABC):
    """Abstract base class for all data quality checks

    All check implementations must inherit from this class and implement:
    - _validate_params(): Validate check-specific parameters using Pydantic
    - run(): Execute the check and return a list of CheckResult objects

    Attributes:
        name: Registry key for the check (set by @register_check decorator)
        display_name: Human-readable name for the check
        supported_dtypes: List of Polars dtypes this check can handle (empty = all types)
        df: DataFrame to check
        col: Column name to check
        primary_key_columns: List of primary key columns for context in examples
        params: Additional check-specific parameters
    """

    name: str = None  # Set by @register_check decorator
    display_name: str = None  # Override in subclasses
    supported_dtypes: List[Type[pl.DataType]] = []  # Override in subclasses (empty = universal)

    def __init__(
        self,
        df: pl.DataFrame,
        col: str,
        primary_key_columns: Optional[List[str]] = None,
        **params
    ):
        """Initialize a check instance

        Args:
            df: Polars DataFrame to check
            col: Column name to check
            primary_key_columns: Optional list of primary key columns for example context
            **params: Check-specific parameters
        """
        self.df = df
        self.col = col
        self.primary_key_columns = primary_key_columns or []
        self.params = params
        self._validate_params()

    @abstractmethod
    def _validate_params(self) -> None:
        """Validate check-specific parameters using Pydantic

        This method should:
        1. Create a Pydantic model for parameter validation
        2. Validate self.params against the model
        3. Store the validated config as self.config

        Raises:
            ValidationError: If parameters are invalid
        """
        pass

    @abstractmethod
    async def run(self) -> List[CheckResult]:
        """Execute the check and return results

        Returns:
            List of CheckResult objects, one per issue type found
            Empty list if no issues were detected
        """
        pass

    # Shared helper methods

    def _get_non_null_df(self) -> pl.DataFrame:
        """Get DataFrame filtered to non-null values in the check column

        Returns:
            Filtered DataFrame with non-null values only
        """
        return self.df.filter(pl.col(self.col).is_not_null())

    def _format_example_with_pk(self, row_data: Dict) -> str:
        """Format an example value with primary key context

        Args:
            row_data: Dictionary of column name -> value for a single row

        Returns:
            Formatted string like "value [pk1=123, pk2=456]" or just "value"
        """
        value = str(row_data[self.col])

        if self.primary_key_columns:
            pk_values = []
            for pk_col in self.primary_key_columns:
                if pk_col in row_data:
                    pk_values.append(f"{pk_col}={row_data[pk_col]}")
            if pk_values:
                return f"{value} [{', '.join(pk_values)}]"

        return value

    def _get_examples(
        self,
        filtered_df: pl.DataFrame,
        limit: int = 5,
        quote: bool = False
    ) -> List[str]:
        """Extract formatted examples from a filtered DataFrame

        Args:
            filtered_df: DataFrame filtered to violating rows
            limit: Maximum number of examples to return
            quote: Whether to wrap examples in quotes

        Returns:
            List of formatted example strings
        """
        select_cols = [self.col] + self.primary_key_columns
        examples = []

        for row in filtered_df.select(select_cols).head(limit).iter_rows(named=True):
            example = self._format_example_with_pk(row)
            if quote:
                example = f"'{example}'"
            examples.append(example)

        return examples

    def _calculate_percentage(self, count: int, total: int) -> float:
        """Calculate percentage for issue reporting

        Args:
            count: Number of violating rows
            total: Total number of non-null rows

        Returns:
            Percentage as float
        """
        if total == 0:
            return 0.0
        return (count / total) * 100.0
