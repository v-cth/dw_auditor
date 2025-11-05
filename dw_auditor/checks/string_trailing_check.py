"""
Trailing/leading characters validation check
"""

from pydantic import BaseModel
from typing import List, Union, Optional
from ..core.base_check import BaseCheck, CheckResult
from ..core.registry import register_check
import polars as pl
import re


class TrailingCharactersParams(BaseModel):
    """Parameters for trailing/leading characters check

    Attributes:
        patterns: Characters or strings to check for.
                 Can be a string (each char checked individually) or list of strings.
                 Default: [" ", "\t", "\n", "\r"] (whitespace)
    """
    patterns: Optional[Union[str, List[str]]] = None


@register_check("trailing_characters")
class TrailingCharactersCheck(BaseCheck):
    """Detect strings with trailing or leading characters/patterns

    Checks for unwanted leading or trailing characters that might indicate
    data quality issues (e.g., extra spaces, tabs, newlines).

    Configuration example:
        {"trailing_characters": {"patterns": [" ", "_tmp"]}}
    """

    display_name = "Trailing/Leading Characters"
    supported_dtypes = [pl.Utf8, pl.String]

    def _validate_params(self) -> None:
        """Validate trailing characters parameters"""
        self.config = TrailingCharactersParams(**self.params)

        # Set default patterns if none provided
        if self.config.patterns is None:
            self.config.patterns = [" ", "\t", "\n", "\r"]
        elif isinstance(self.config.patterns, str):
            # Convert string to list of individual characters
            self.config.patterns = list(self.config.patterns)

    async def run(self) -> List[CheckResult]:
        """Execute trailing/leading characters check

        Returns:
            List of CheckResult objects, one per pattern violation
        """
        results = []
        non_null_count = self._get_non_null_df().height

        if non_null_count == 0:
            return results

        # Check for leading patterns
        for pattern in self.config.patterns:
            escaped_pattern = re.escape(pattern)
            leading_regex = f"^{escaped_pattern}"

            leading = self.df.filter(
                pl.col(self.col).is_not_null() &
                pl.col(self.col).str.contains(leading_regex)
            )

            if len(leading) > 0:
                results.append(CheckResult(
                    type='LEADING_CHARACTERS',
                    pattern=pattern,
                    count=len(leading),
                    pct=self._calculate_percentage(len(leading), non_null_count),
                    examples=self._get_examples(leading, limit=3, quote=True)
                ))

        # Check for trailing patterns
        for pattern in self.config.patterns:
            escaped_pattern = re.escape(pattern)
            trailing_regex = f"{escaped_pattern}$"

            trailing = self.df.filter(
                pl.col(self.col).is_not_null() &
                pl.col(self.col).str.contains(trailing_regex)
            )

            if len(trailing) > 0:
                results.append(CheckResult(
                    type='TRAILING_CHARACTERS',
                    pattern=pattern,
                    count=len(trailing),
                    pct=self._calculate_percentage(len(trailing), non_null_count),
                    examples=self._get_examples(trailing, limit=3, quote=True)
                ))

        return results
