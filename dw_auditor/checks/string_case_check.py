"""
Case sensitivity duplicates check
"""

from pydantic import BaseModel
from typing import List
from ..core.base_check import BaseCheck, CheckResult
from ..core.plugin import register_plugin
import polars as pl


class CaseDuplicatesParams(BaseModel):
    """Parameters for case duplicates check

    No parameters needed for this check.
    """
    pass


@register_plugin("case_duplicates", category="check")
class CaseDuplicatesCheck(BaseCheck):
    """Detect values that differ only in case

    Identifies groups of values that are identical when case is ignored,
    which might indicate inconsistent data entry.

    Example: "Product", "product", "PRODUCT" would be flagged as case duplicates.

    Configuration example:
        {"case_duplicates": true}
    """

    display_name = "Case Duplicates"
    supported_dtypes = [pl.Utf8, pl.String]

    def _validate_params(self) -> None:
        """Validate case duplicates parameters"""
        self.config = CaseDuplicatesParams(**self.params)

    def run(self) -> List[CheckResult]:
        """Execute case duplicates check

        Returns:
            List with single CheckResult if case duplicates found
        """
        results = []

        # Get non-null values and group by lowercase
        case_analysis = (
            self.df.select(pl.col(self.col))
            .filter(pl.col(self.col).is_not_null())
            .with_columns(pl.col(self.col).str.to_lowercase().alias('lower'))
            .group_by('lower')
            .agg(pl.col(self.col).unique().alias('variations'))
            .filter(pl.col('variations').list.len() > 1)
        )

        if len(case_analysis) > 0:
            # Format examples as readable strings: 'lowercase' → ['Variation1', 'Variation2']
            examples = []
            for row in case_analysis.head(3).iter_rows(named=True):
                lowercase_val = row['lower']
                variations = row['variations']
                # Format as: 'paris' → ['Paris', 'paris']
                formatted_example = f"'{lowercase_val}' → {variations}"
                examples.append(formatted_example)

            results.append(CheckResult(
                type='CASE_DUPLICATES',
                count=len(case_analysis),
                examples=examples
            ))

        return results
