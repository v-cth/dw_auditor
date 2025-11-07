"""
Base class and models for column insights
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Optional, Any, Type, Union
from pydantic import BaseModel, Field
import polars as pl


class InsightResult(BaseModel):
    """Structured result from a column insight

    Attributes:
        type: Insight type identifier (e.g., 'min', 'max', 'top_values', 'quantiles')
        value: The insight value (can be scalar, list, dict, etc.)
        metadata: Additional metadata about the insight (optional)
        display_name: Human-readable name for the insight (optional)
        unit: Unit of measurement if applicable (optional, e.g., 'days', 'characters')
    """
    type: str
    value: Any
    metadata: Optional[Dict[str, Any]] = None
    display_name: Optional[str] = None
    unit: Optional[str] = None


class BaseInsight(ABC):
    """Abstract base class for all column insights

    All insight implementations must inherit from this class and implement:
    - _validate_params(): Validate insight-specific parameters using Pydantic
    - generate(): Execute the insight and return a list of InsightResult objects

    Attributes:
        name: Registry key for the insight (set by @register_insight decorator)
        display_name: Human-readable name for the insight
        supported_dtypes: List of Polars dtypes this insight can handle (empty = all types)
        df: DataFrame to analyze
        col: Column name to analyze
        params: Additional insight-specific parameters
    """

    name: str = None  # Set by @register_insight decorator
    display_name: str = None  # Override in subclasses
    supported_dtypes: List[Type[pl.DataType]] = []  # Override in subclasses (empty = universal)

    def __init__(
        self,
        df: pl.DataFrame,
        col: str,
        **params
    ):
        """Initialize an insight instance

        Args:
            df: Polars DataFrame to analyze
            col: Column name to analyze
            **params: Insight-specific parameters
        """
        self.df = df
        self.col = col
        self.params = params
        self._validate_params()

    @abstractmethod
    def _validate_params(self) -> None:
        """Validate insight-specific parameters using Pydantic

        This method should:
        1. Create a Pydantic model for parameter validation
        2. Validate self.params against the model
        3. Store the validated config as self.config

        Raises:
            ValidationError: If parameters are invalid
        """
        pass

    @abstractmethod
    def generate(self) -> List[InsightResult]:
        """Execute the insight and return results

        Returns:
            List of InsightResult objects
            Empty list if no data or insight cannot be computed
        """
        pass

    # Shared helper methods

    def _get_non_null_series(self) -> pl.Series:
        """Get series filtered to non-null values

        Returns:
            Polars Series with non-null values only
        """
        return self.df[self.col].drop_nulls()

    def _get_non_null_df(self) -> pl.DataFrame:
        """Get DataFrame filtered to non-null values in the insight column

        Returns:
            Filtered DataFrame with non-null values only
        """
        return self.df.filter(pl.col(self.col).is_not_null())

    def _calculate_value_counts(
        self,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Calculate value counts and percentages for the column

        Args:
            limit: Maximum number of values to return (top N by count)

        Returns:
            List of dicts with 'value', 'count', 'percentage' keys
        """
        non_null_df = self._get_non_null_df()
        total_non_null = len(non_null_df)

        if total_non_null == 0:
            return []

        # Calculate value counts and percentages using Polars expressions
        value_counts_query = (
            non_null_df.select(pl.col(self.col))
            .group_by(self.col)
            .agg(pl.count().alias('count'))
            .with_columns(
                (pl.col('count') / total_non_null * 100).alias('percentage')
            )
            .sort('count', descending=True)
        )

        if limit:
            value_counts_query = value_counts_query.head(limit)

        return [
            {
                'value': item[self.col],
                'count': item['count'],
                'percentage': item['percentage']
            }
            for item in value_counts_query.to_dicts()
        ]

    def _format_numeric(self, value: Union[int, float], decimals: int = 4) -> Union[int, float]:
        """Format numeric value with appropriate precision

        Args:
            value: Numeric value to format
            decimals: Number of decimal places for floats

        Returns:
            Int if value is whole number, otherwise float rounded to decimals
        """
        if isinstance(value, int):
            return value

        # Check if float is effectively an integer
        if isinstance(value, float) and value.is_integer():
            return int(value)

        return round(float(value), decimals)
