"""
String ending characters validation check
"""

from pydantic import BaseModel
from typing import List, Union, Optional
from ..core.base_check import BaseCheck, CheckResult
from ..core.registry import register_check
import polars as pl
import re


class EndingCharactersParams(BaseModel):
    """Parameters for ending characters check

    Attributes:
        patterns: Characters or strings to check for at the end.
                 Can be a string (each char checked individually) or list of strings.
                 Default: [".", ",", ";", ":", "!", "?"] (punctuation)
    """
    patterns: Optional[Union[str, List[str]]] = None


@register_check("ending_characters")
class EndingCharactersCheck(BaseCheck):
    """Detect strings that end with specific characters or patterns

    Useful for finding strings with unexpected punctuation or suffixes.

    Configuration example:
        {"ending_characters": {"patterns": [".", "_tmp"]}}
    """

    display_name = "Ending Characters"

    def _validate_params(self) -> None:
        """Validate ending characters parameters"""
        self.config = EndingCharactersParams(**self.params)

        # Set default patterns if none provided
        if self.config.patterns is None:
            self.config.patterns = [".", ",", ";", ":", "!", "?"]
        elif isinstance(self.config.patterns, str):
            # Convert string to list of individual characters
            self.config.patterns = list(self.config.patterns)

    async def run(self) -> List[CheckResult]:
        """Execute ending characters check

        Returns:
            List of CheckResult objects, one per pattern violation
        """
        results = []
        non_null_count = self._get_non_null_df().height

        if non_null_count == 0:
            return results

        # Check for each ending pattern separately
        for pattern in self.config.patterns:
            escaped_pattern = re.escape(pattern)
            ending_regex = f"{escaped_pattern}$"

            ending_with_pattern = self.df.filter(
                pl.col(self.col).is_not_null() &
                pl.col(self.col).str.contains(ending_regex)
            )

            if len(ending_with_pattern) > 0:
                results.append(CheckResult(
                    type='ENDING_CHARACTERS',
                    pattern=pattern,
                    count=len(ending_with_pattern),
                    pct=self._calculate_percentage(len(ending_with_pattern), non_null_count),
                    examples=self._get_examples(ending_with_pattern, limit=3)
                ))

        return results
