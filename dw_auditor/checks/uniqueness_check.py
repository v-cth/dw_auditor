"""
Uniqueness validation check
"""

from pydantic import BaseModel
from typing import List
from ..core.base_check import BaseCheck, CheckResult
from ..core.registry import register_check
import polars as pl


class UniquenessParams(BaseModel):
    """Parameters for uniqueness check

    No parameters needed for this check.
    """
    pass


@register_check("uniqueness")
class UniquenessCheck(BaseCheck):
    """Check if all values in a column are unique (excluding nulls)

    Identifies duplicate values in columns that should contain only unique values.
    Useful for validating primary keys, unique identifiers, or natural keys.

    Reports:
    - Total number of duplicate rows
    - Number of distinct values that have duplicates
    - Top duplicated values with their counts

    Configuration example:
        {"uniqueness": true}
    """

    display_name = "Uniqueness Validation"

    def _validate_params(self) -> None:
        """Validate uniqueness parameters"""
        self.config = UniquenessParams(**self.params)

    async def run(self) -> List[CheckResult]:
        """Execute uniqueness check

        Returns:
            List with single CheckResult if duplicates found
        """
        results = []

        # Filter out nulls
        non_null_df = self._get_non_null_df()
        non_null_count = len(non_null_df)

        if non_null_count == 0:
            return results

        # Count occurrences of each value
        value_counts = non_null_df.group_by(self.col).agg(
            pl.count().alias('count')
        ).filter(
            pl.col('count') > 1  # Only duplicates
        ).sort('count', descending=True)

        duplicate_values_count = len(value_counts)

        if duplicate_values_count > 0:
            # Calculate total number of duplicate rows (sum of counts - number of unique duplicate values)
            total_duplicate_rows = value_counts['count'].sum() - duplicate_values_count
            pct = (total_duplicate_rows / non_null_count) * 100

            # Get top 5 most frequent duplicates with their counts
            examples = []
            for row in value_counts.head(5).iter_rows(named=True):
                value = row[self.col]
                count = row['count']

                # Try to get primary key context for first occurrence
                if self.primary_key_columns and len(self.primary_key_columns) > 0:
                    # Get one example row with this value
                    select_cols = [self.col] + self.primary_key_columns
                    example_row = non_null_df.filter(pl.col(self.col) == value).select(select_cols).head(1)

                    if len(example_row) > 0:
                        row_data = example_row.row(0, named=True)
                        pk_values = [
                            f"{pk_col}={row_data[pk_col]}"
                            for pk_col in self.primary_key_columns
                            if pk_col in row_data
                        ]
                        if pk_values:
                            examples.append(f"{value} [count={count}, {', '.join(pk_values)}]")
                        else:
                            examples.append(f"{value} [count={count}]")
                    else:
                        examples.append(f"{value} [count={count}]")
                else:
                    examples.append(f"{value} [count={count}]")

            results.append(CheckResult(
                type='DUPLICATE_VALUES',
                count=total_duplicate_rows,
                pct=pct,
                distinct_duplicates=duplicate_values_count,
                suggestion=f'Column should contain only unique values. Found {duplicate_values_count} distinct value(s) with duplicates.',
                examples=examples
            ))

        return results
