"""
Atomic insights - reusable insight components that work across data types
"""

from .top_values import TopValuesInsight
from .quantiles import QuantilesInsight
from .length_stats import LengthStatsInsight
from .histogram import HistogramInsight

__all__ = [
    'TopValuesInsight',
    'QuantilesInsight',
    'LengthStatsInsight',
    'HistogramInsight',
]
