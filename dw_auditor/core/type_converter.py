"""
Type conversion module for automatic string-to-typed column conversion

Two-phase strategy:
1. Test conversion on 5% random sample (90% success threshold)
2. If sample passes, convert full column and verify (95% threshold)
"""

import polars as pl
import logging
from typing import Tuple, List, Dict, Optional

logger = logging.getLogger(__name__)


class TypeConverter:
    """
    Automatic type conversion for string columns

    Optimized two-phase approach:
    - Phase 1: Quick test on small sample (5% default)
    - Phase 2: Full conversion only if sample passes

    Args:
        sample_threshold: Minimum success rate for sample test (default: 0.90)
        full_threshold: Minimum success rate for full conversion (default: 0.95)
        sample_fraction: Fraction of data to sample for testing (default: 0.05)
    """

    def __init__(
        self,
        sample_threshold: float = 0.90,
        full_threshold: float = 0.95,
        sample_fraction: float = 0.05
    ):
        self.sample_threshold = sample_threshold
        self.full_threshold = full_threshold
        self.sample_fraction = sample_fraction

    def convert_dataframe(self, df: pl.DataFrame) -> Tuple[pl.DataFrame, List[Dict]]:
        """
        Attempt to convert string columns to more specific types

        Conversion order: int64 → float64 → datetime → date

        Args:
            df: Polars DataFrame

        Returns:
            Tuple of (modified DataFrame, list of conversion info dicts)
        """
        conversion_log = []

        # Identify string columns
        string_columns = [col for col in df.columns if df[col].dtype in [pl.Utf8, pl.String]]

        if not string_columns:
            return df, conversion_log

        logger.info(f"Attempting type conversions on {len(string_columns)} string column(s)...")

        for col in string_columns:
            # Calculate non-null count once and reuse
            total_non_null = df[col].is_not_null().sum()
            if total_non_null == 0:
                continue

            # Try conversions in order: int → float → datetime → date
            conversion_result = self._try_conversion_sequence(df, col, total_non_null)

            if conversion_result:
                df, log_entry = conversion_result
                conversion_log.append(log_entry)

        if conversion_log:
            logger.info(f"Successfully converted {len(conversion_log)} column(s) to more specific types")
        else:
            logger.info(f"No string columns could be converted (sample: {self.sample_threshold:.0%}, full: {self.full_threshold:.0%})")

        return df, conversion_log

    def _try_conversion_sequence(
        self,
        df: pl.DataFrame,
        col: str,
        total_non_null: int
    ) -> Optional[Tuple[pl.DataFrame, Dict]]:
        """
        Try conversion types in sequence: int → float → datetime → date

        Returns:
            Tuple of (modified DataFrame, log entry) if successful, None otherwise
        """
        # 1. Try INTEGER conversion
        result = self._try_type_conversion(
            df, col, total_non_null,
            converter_func=lambda s: s.cast(pl.Int64, strict=False),
            type_name='int64'
        )
        if result:
            return result

        # 2. Try FLOAT conversion
        result = self._try_type_conversion(
            df, col, total_non_null,
            converter_func=lambda s: s.cast(pl.Float64, strict=False),
            type_name='float64'
        )
        if result:
            return result

        # 3. Try DATETIME conversion
        result = self._try_type_conversion(
            df, col, total_non_null,
            converter_func=lambda s: s.str.to_datetime(strict=False),
            type_name='datetime'
        )
        if result:
            return result

        # 4. Try DATE conversion
        result = self._try_type_conversion(
            df, col, total_non_null,
            converter_func=lambda s: s.str.to_date(strict=False),
            type_name='date'
        )
        if result:
            return result

        return None

    def _try_type_conversion(
        self,
        df: pl.DataFrame,
        col: str,
        total_non_null: int,
        converter_func,
        type_name: str
    ) -> Optional[Tuple[pl.DataFrame, Dict]]:
        """
        Two-phase conversion: test on sample, then apply to full column

        Phase 1: Test on 5% random sample (90% threshold)
        Phase 2: Convert full column and verify (95% threshold)

        Args:
            df: DataFrame
            col: Column name
            total_non_null: Pre-calculated non-null count
            converter_func: Function to convert series (e.g., lambda s: s.cast(pl.Int64, strict=False))
            type_name: Type name for logging (e.g., 'int64', 'datetime')

        Returns:
            Tuple of (modified DataFrame, log entry) if successful, None otherwise
        """
        try:
            # Phase 1: Test on sample
            sample_size = max(1, int(len(df) * self.sample_fraction))
            sample_df = df.sample(n=sample_size, seed=42)

            sample_converted = converter_func(sample_df[col])
            sample_non_null = sample_df[col].is_not_null().sum()

            if sample_non_null == 0:
                return None

            sample_success = sample_converted.is_not_null().sum()
            sample_rate = sample_success / sample_non_null

            # If sample fails threshold, skip full conversion
            if sample_rate < self.sample_threshold:
                return None

            # Phase 2: Sample passed, convert full column
            converted = converter_func(df[col])
            successful_conversions = converted.is_not_null().sum()
            success_rate = successful_conversions / total_non_null if total_non_null > 0 else 0

            # Verify full conversion meets threshold
            if success_rate >= self.full_threshold:
                df = df.with_columns(converted.alias(col))

                log_entry = {
                    'column': col,
                    'from_type': 'string',
                    'to_type': type_name,
                    'success_rate': success_rate,
                    'converted_values': successful_conversions
                }

                logger.info(f"  ✓ {col}: string → {type_name} ({success_rate:.1%} success)")
                print(f"⚠️  Auto-converted '{col}' from string to {type_name} ({success_rate:.1%} success)\n")

                return (df, log_entry)

            return None

        except Exception:
            return None
