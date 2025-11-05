"""
String leading characters validation check
"""

from pydantic import BaseModel
from typing import List, Union, Optional
from ..core.base_check import BaseCheck, CheckResult
from ..core.registry import register_check
import polars as pl
import re


class LeadingCharactersParams(BaseModel):
    """Parameters for leading characters check

    Attributes:
        patterns: Characters or strings to check for at the beginning.
                 Can be a string (each char checked individually) or list of strings.
                 Default: [".", ",", ";", ":", "!", "?", " "] (punctuation and spaces)
    """
    patterns: Optional[Union[str, List[str]]] = None


@register_check("leading_characters")
class LeadingCharactersCheck(BaseCheck):
    """Detect strings that begin with specific characters or patterns

    Useful for finding strings with unexpected leading punctuation or prefixes.

    Configuration example:
        {"leading_characters": {"patterns": [".", " "]}}
    """

    display_name = "Leading Characters"
    supported_dtypes = [pl.Utf8, pl.String]

    def _validate_params(self) -> None:
        """Validate leading characters parameters"""
        self.config = LeadingCharactersParams(**self.params)

        # Set default patterns if none provided
        if self.config.patterns is None:
            self.config.patterns = [".", ",", ";", ":", "!", "?", " "]
        elif isinstance(self.config.patterns, str):
            # Convert string to list of individual characters
            self.config.patterns = list(self.config.patterns)

    async def run(self) -> List[CheckResult]:
        """Execute leading characters check

        Returns:
            List of CheckResult objects, one per pattern violation
        """
        results = []
        non_null_count = self._get_non_null_df().height

        if non_null_count == 0:
            return results

        # Check for each leading pattern separately
        for pattern in self.config.patterns:
            escaped_pattern = re.escape(pattern)
            leading_regex = f"^{escaped_pattern}"

            leading_with_pattern = self.df.filter(
                pl.col(self.col).is_not_null() &
                pl.col(self.col).str.contains(leading_regex)
            )

            if len(leading_with_pattern) > 0:
                results.append(CheckResult(
                    type='LEADING_CHARACTERS',
                    pattern=pattern,
                    count=len(leading_with_pattern),
                    pct=self._calculate_percentage(len(leading_with_pattern), non_null_count),
                    examples=self._get_examples(leading_with_pattern, limit=3)
                ))

        return results
