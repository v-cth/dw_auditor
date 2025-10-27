"""
Numeric strings detection check
"""

from pydantic import BaseModel, Field
from typing import List
from ..core.base_check import BaseCheck, CheckResult
from ..core.registry import register_check
import polars as pl


class NumericStringsParams(BaseModel):
    """Parameters for numeric strings check

    Attributes:
        threshold: Percentage threshold (0.0-1.0) above which to flag column
                  Default: 0.8 (80% of values must be numeric to flag)
    """
    threshold: float = Field(default=0.8, ge=0.0, le=1.0)


@register_check("numeric_strings")
class NumericStringsCheck(BaseCheck):
    """Detect string columns that contain only numeric values

    Identifies columns stored as strings but containing numeric data,
    which might indicate incorrect data typing.

    Only flags if a high percentage of values are numeric (configurable).

    Configuration example:
        {"numeric_strings": {"threshold": 0.9}}
    """

    display_name = "Numeric Strings Detection"

    def _validate_params(self) -> None:
        """Validate numeric strings parameters"""
        self.config = NumericStringsParams(**self.params)

    async def run(self) -> List[CheckResult]:
        """Execute numeric strings check

        Returns:
            List with single CheckResult if threshold exceeded
        """
        results = []

        # Pattern for numeric strings (including decimals and negatives)
        numeric_pattern = r'^-?\d+\.?\d*$'

        non_null_df = self._get_non_null_df()
        non_null_count = len(non_null_df)

        if non_null_count == 0:
            return results

        numeric_strings = non_null_df.filter(
            pl.col(self.col).str.contains(f'^{numeric_pattern}$')
        )

        # Only flag if a high percentage are numeric
        pct_numeric = len(numeric_strings) / non_null_count

        if pct_numeric > self.config.threshold:
            examples = numeric_strings[self.col].head(3).to_list()
            results.append(CheckResult(
                type='NUMERIC_STRINGS',
                count=len(numeric_strings),
                pct=pct_numeric * 100,
                suggestion='Consider converting to numeric type',
                examples=examples
            ))

        return results
