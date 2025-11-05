"""
Regex pattern validation check
"""

from pydantic import BaseModel, validator
from typing import List, Optional
from ..core.base_check import BaseCheck, CheckResult
from ..core.registry import register_check
import polars as pl
import re


class RegexPatternParams(BaseModel):
    """Parameters for regex pattern check

    Attributes:
        pattern: Regex pattern to check against (required)
        mode: Validation mode - "contains" or "match"
              - "contains": Flag rows where pattern IS found (negative validation)
              - "match": Flag rows where pattern is NOT matched (positive validation)
        description: Optional human-readable description for error messages
    """
    pattern: str
    mode: str = "contains"
    description: Optional[str] = None

    @validator('pattern')
    def validate_pattern(cls, v):
        """Ensure pattern is valid regex"""
        if not v:
            raise ValueError("Pattern cannot be empty")
        try:
            re.compile(v)
        except re.error as e:
            raise ValueError(f"Invalid regex pattern: {e}")
        return v

    @validator('mode')
    def validate_mode(cls, v):
        """Ensure mode is valid"""
        if v not in ['contains', 'match']:
            raise ValueError(f"Mode must be 'contains' or 'match', got '{v}'")
        return v


@register_check("regex_pattern")
class RegexPatternCheck(BaseCheck):
    """Check strings against regex patterns with flexible validation modes

    Two modes:
    - "contains": Flag rows where pattern IS found (find unwanted patterns)
    - "match": Flag rows where pattern is NOT matched (enforce required format)

    Configuration examples:
        # Flag values containing special characters
        {"regex_pattern": {"pattern": "[^a-zA-Z0-9]", "mode": "contains"}}

        # Flag values not matching email format
        {"regex_pattern": {"pattern": "^[\\w.-]+@[\\w.-]+\\.\\w+$", "mode": "match"}}
    """

    display_name = "Regex Pattern Validation"
    supported_dtypes = [pl.Utf8, pl.String]

    def _validate_params(self) -> None:
        """Validate regex pattern parameters"""
        self.config = RegexPatternParams(**self.params)

    async def run(self) -> List[CheckResult]:
        """Execute regex pattern check

        Returns:
            List with single CheckResult if violations found
        """
        results = []
        non_null_count = self._get_non_null_df().height

        if non_null_count == 0:
            return results

        if self.config.mode == "contains":
            # Flag rows where pattern IS found (negative validation)
            violating = self.df.filter(
                pl.col(self.col).is_not_null() &
                pl.col(self.col).str.contains(self.config.pattern)
            )
            description = self.config.description or f"Rows containing pattern: {self.config.pattern}"

        elif self.config.mode == "match":
            # Flag rows where pattern is NOT matched (positive validation)
            violating = self.df.filter(
                pl.col(self.col).is_not_null() &
                ~pl.col(self.col).str.contains(f"^{self.config.pattern}$")
            )
            description = self.config.description or f"Rows not matching pattern: {self.config.pattern}"

        if len(violating) > 0:
            results.append(CheckResult(
                type='REGEX_PATTERN',
                pattern=self.config.pattern,
                mode=self.config.mode,
                description=description,
                count=len(violating),
                pct=self._calculate_percentage(len(violating), non_null_count),
                examples=self._get_examples(violating, limit=3)
            ))

        return results
