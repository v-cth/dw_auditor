"""
Automatically detect relationships between database tables based on
column names, data types, and value overlaps using Polars DataFrames
"""

import polars as pl
from typing import Dict, List, Tuple, Set, Optional
from collections import defaultdict


class PolarsRelationshipDetector:
    """
    Detect and analyze relationships between database tables using Polars DataFrames
    that have already been loaded during the audit process
    """

    def __init__(self):
        self.tables = {}
        self.relationships = []
        self.column_metadata = {}

    def add_table(self, name: str, df: pl.DataFrame):
        """
        Add a table to the analysis

        Args:
            name: Table name
            df: Polars DataFrame containing the table data
        """
        self.tables[name] = df
        self._analyze_columns(name, df)

    def _analyze_columns(self, table_name: str, df: pl.DataFrame):
        """
        Analyze column metadata for a table

        Args:
            table_name: Name of the table
            df: Polars DataFrame
        """
        self.column_metadata[table_name] = {}

        for col in df.columns:
            col_series = df.get_column(col)

            # Get sample non-null values
            sample_values = []
            non_null_series = col_series.drop_nulls()
            if len(non_null_series) > 0:
                sample_values = non_null_series.head(5).to_list()

            self.column_metadata[table_name][col] = {
                'dtype': str(col_series.dtype),
                'unique_count': col_series.n_unique(),
                'null_count': col_series.null_count(),
                'sample_values': sample_values,
                'is_potential_key': self._is_potential_key(col_series)
            }

    def _is_potential_key(self, series: pl.Series) -> bool:
        """
        Check if a column could be a key based on uniqueness

        Args:
            series: Polars Series

        Returns:
            True if column has high uniqueness ratio (>95%)
        """
        if series.null_count() == len(series):
            return False

        unique_ratio = series.n_unique() / len(series)
        return unique_ratio > 0.95  # More than 95% unique values

    def detect_relationships(self, confidence_threshold: float = 0.7) -> List[Dict]:
        """
        Detect relationships between tables based on:
        1. Exact column name matches
        2. Similar column names (e.g., order_id, orderid, order_ID)
        3. Data type compatibility
        4. Value overlap analysis

        Args:
            confidence_threshold: Minimum confidence score to report (0.0 to 1.0)

        Returns:
            List of relationship dictionaries
        """
        self.relationships = []
        table_names = list(self.tables.keys())

        for i, table1 in enumerate(table_names):
            for table2 in table_names[i+1:]:
                # Find potential relationships between table1 and table2
                relationships = self._find_table_relationships(
                    table1, table2, confidence_threshold
                )
                self.relationships.extend(relationships)

        return self.relationships

    def _find_table_relationships(self, table1: str, table2: str,
                                 confidence_threshold: float) -> List[Dict]:
        """
        Find all relationships between two tables

        Args:
            table1: First table name
            table2: Second table name
            confidence_threshold: Minimum confidence to report

        Returns:
            List of relationship dictionaries
        """
        relationships = []
        df1 = self.tables[table1]
        df2 = self.tables[table2]

        for col1 in df1.columns:
            for col2 in df2.columns:
                # Check for potential relationship
                confidence = self._calculate_relationship_confidence(
                    table1, col1, table2, col2
                )

                if confidence >= confidence_threshold:
                    relationship = {
                        'table1': table1,
                        'column1': col1,
                        'table2': table2,
                        'column2': col2,
                        'confidence': confidence,
                        'relationship_type': self._determine_relationship_type(
                            df1.get_column(col1), df2.get_column(col2)
                        ),
                        'matching_values': self._get_matching_values_count(
                            df1.get_column(col1), df2.get_column(col2)
                        )
                    }
                    relationships.append(relationship)

        return relationships

    def _calculate_relationship_confidence(self, table1: str, col1: str,
                                          table2: str, col2: str) -> float:
        """
        Calculate confidence score for a potential relationship

        Scoring:
        - 40% from column name similarity
        - 20% from data type compatibility
        - 40% from value overlap

        Args:
            table1: First table name
            col1: First column name
            table2: Second table name
            col2: Second column name

        Returns:
            Confidence score between 0.0 and 1.0
        """
        confidence = 0.0

        # 1. Check column name similarity (40%)
        name_similarity = self._calculate_name_similarity(col1, col2)
        confidence += name_similarity * 0.4

        # 2. Check data type compatibility (20%)
        dtype1 = self.column_metadata[table1][col1]['dtype']
        dtype2 = self.column_metadata[table2][col2]['dtype']
        if self._are_dtypes_compatible(dtype1, dtype2):
            confidence += 0.2

        # 3. Check value overlap (40%)
        df1 = self.tables[table1]
        df2 = self.tables[table2]
        overlap_ratio = self._calculate_value_overlap(
            df1.get_column(col1), df2.get_column(col2)
        )
        confidence += overlap_ratio * 0.4

        return confidence

    def _calculate_name_similarity(self, name1: str, name2: str) -> float:
        """
        Calculate similarity between column names

        Args:
            name1: First column name
            name2: Second column name

        Returns:
            Similarity score between 0.0 and 1.0
        """
        # Normalize names
        norm1 = name1.lower().replace('_', '').replace('-', '').replace(' ', '')
        norm2 = name2.lower().replace('_', '').replace('-', '').replace(' ', '')

        if norm1 == norm2:
            return 1.0

        # Check if one contains the other
        if norm1 in norm2 or norm2 in norm1:
            return 0.8

        # Check for common patterns (id suffix)
        if norm1.endswith('id') and norm2.endswith('id'):
            return 0.5

        return 0.0

    def _are_dtypes_compatible(self, dtype1: str, dtype2: str) -> bool:
        """
        Check if two data types are compatible for joining

        Args:
            dtype1: First data type (Polars dtype string)
            dtype2: Second data type (Polars dtype string)

        Returns:
            True if types are compatible
        """
        # Normalize dtype strings to lowercase
        dtype1_lower = dtype1.lower()
        dtype2_lower = dtype2.lower()

        # Same type
        if dtype1 == dtype2:
            return True

        # Both numeric (Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64, Float32, Float64)
        numeric_types = ['int8', 'int16', 'int32', 'int64', 'uint8', 'uint16', 'uint32', 'uint64', 'float32', 'float64']
        if any(t in dtype1_lower for t in numeric_types) and \
           any(t in dtype2_lower for t in numeric_types):
            return True

        # Both string-like (Utf8, Categorical, String)
        string_types = ['utf8', 'str', 'string', 'categorical']
        if any(t in dtype1_lower for t in string_types) and \
           any(t in dtype2_lower for t in string_types):
            return True

        # Both date/time types
        datetime_types = ['date', 'datetime', 'time', 'duration']
        if any(t in dtype1_lower for t in datetime_types) and \
           any(t in dtype2_lower for t in datetime_types):
            return True

        return False

    def _calculate_value_overlap(self, series1: pl.Series, series2: pl.Series) -> float:
        """
        Calculate the overlap ratio between two series using set intersection

        Args:
            series1: First Polars Series
            series2: Second Polars Series

        Returns:
            Overlap ratio between 0.0 and 1.0 (Jaccard index)
        """
        # Get unique non-null values
        set1 = set(series1.drop_nulls().unique().to_list())
        set2 = set(series2.drop_nulls().unique().to_list())

        if len(set1) == 0 or len(set2) == 0:
            return 0.0

        intersection = set1.intersection(set2)
        union = set1.union(set2)

        if len(union) == 0:
            return 0.0

        # Jaccard index
        return len(intersection) / len(union)

    def _determine_relationship_type(self, series1: pl.Series, series2: pl.Series) -> str:
        """
        Determine the type of relationship (one-to-one, one-to-many, etc.)

        Args:
            series1: First Polars Series
            series2: Second Polars Series

        Returns:
            Relationship type string
        """
        unique_ratio1 = series1.n_unique() / len(series1) if len(series1) > 0 else 0
        unique_ratio2 = series2.n_unique() / len(series2) if len(series2) > 0 else 0

        if unique_ratio1 > 0.95 and unique_ratio2 > 0.95:
            return "one-to-one"
        elif unique_ratio1 > 0.95 or unique_ratio2 > 0.95:
            return "one-to-many"
        else:
            return "many-to-many"

    def _get_matching_values_count(self, series1: pl.Series, series2: pl.Series) -> int:
        """
        Count the number of matching unique values between two series

        Args:
            series1: First Polars Series
            series2: Second Polars Series

        Returns:
            Count of matching values
        """
        set1 = set(series1.drop_nulls().unique().to_list())
        set2 = set(series2.drop_nulls().unique().to_list())
        return len(set1.intersection(set2))
