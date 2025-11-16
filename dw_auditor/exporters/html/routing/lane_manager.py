"""
Global lane management system for ER diagram routing.

Tracks which lanes (vertical/horizontal rails) are occupied
and prevents visual overlap between parallel relationship lines.
"""

from typing import Dict, Set, Tuple
from dataclasses import dataclass


@dataclass(frozen=True)
class Segment:
    """Represents a line segment between two points."""
    x1: float
    y1: float
    x2: float
    y2: float

    def __hash__(self):
        # Normalize to ensure (A→B) == (B→A)
        if (self.x1, self.y1) <= (self.x2, self.y2):
            return hash((self.x1, self.y1, self.x2, self.y2))
        return hash((self.x2, self.y2, self.x1, self.y1))

    def __eq__(self, other):
        return hash(self) == hash(other)

    def is_vertical(self) -> bool:
        """Check if segment is vertical."""
        return abs(self.x1 - self.x2) < 0.1  # Tolerance for floating point

    def is_horizontal(self) -> bool:
        """Check if segment is horizontal."""
        return abs(self.y1 - self.y2) < 0.1

    def length(self) -> float:
        """Calculate segment length."""
        import math
        return math.sqrt((self.x2 - self.x1)**2 + (self.y2 - self.y1)**2)


class LaneRegistry:
    """
    Global lane reservation system.

    Tracks usage of vertical and horizontal lanes to prevent
    overlapping lines and enable intelligent lane selection.
    """

    def __init__(self):
        """Initialize empty lane registry."""
        # Track usage count per lane position
        self.vertical_lanes: Dict[int, int] = {}    # x-coordinate → count
        self.horizontal_lanes: Dict[int, int] = {}  # y-coordinate → count

        # Remember all used segments
        self.used_segments: Set[Segment] = set()

        # Track segment usage count for overlap detection
        self.segment_usage: Dict[Segment, int] = {}

    def reserve_lane(self, position: int, is_vertical: bool):
        """
        Reserve a lane (increment usage count).

        Args:
            position: x-coordinate (vertical) or y-coordinate (horizontal)
            is_vertical: True for vertical lane, False for horizontal
        """
        if is_vertical:
            self.vertical_lanes[position] = self.vertical_lanes.get(position, 0) + 1
        else:
            self.horizontal_lanes[position] = self.horizontal_lanes.get(position, 0) + 1

    def get_lane_usage(self, position: int, is_vertical: bool) -> int:
        """
        Get current usage count for a lane.

        Args:
            position: x-coordinate (vertical) or y-coordinate (horizontal)
            is_vertical: True for vertical lane, False for horizontal

        Returns:
            Usage count (0 if lane is unused)
        """
        if is_vertical:
            return self.vertical_lanes.get(position, 0)
        else:
            return self.horizontal_lanes.get(position, 0)

    def get_lane_cost(self, position: int, is_vertical: bool) -> int:
        """
        Calculate cost penalty for using a lane.

        Returns higher cost for heavily-used lanes to encourage
        distribution across available lanes.

        Args:
            position: x-coordinate (vertical) or y-coordinate (horizontal)
            is_vertical: True for vertical lane, False for horizontal

        Returns:
            Cost penalty (0 for unused lanes, higher for busy lanes)
        """
        usage = self.get_lane_usage(position, is_vertical)
        # Exponential penalty: 0, 1, 4, 9, 16, ...
        return usage * usage

    def add_segment(self, x1: float, y1: float, x2: float, y2: float):
        """
        Record a segment as used.

        Args:
            x1, y1: Start point
            x2, y2: End point
        """
        seg = Segment(x1, y1, x2, y2)
        self.used_segments.add(seg)
        self.segment_usage[seg] = self.segment_usage.get(seg, 0) + 1

        # Also reserve the lane this segment occupies
        if seg.is_vertical():
            # Round to nearest 10 pixels for lane grouping
            lane_pos = int(round(x1 / 10) * 10)
            self.reserve_lane(lane_pos, is_vertical=True)
        elif seg.is_horizontal():
            lane_pos = int(round(y1 / 10) * 10)
            self.reserve_lane(lane_pos, is_vertical=False)

    def is_segment_used(self, x1: float, y1: float, x2: float, y2: float) -> bool:
        """
        Check if a segment has been used before.

        Args:
            x1, y1: Start point
            x2, y2: End point

        Returns:
            True if segment already exists in registry
        """
        seg = Segment(x1, y1, x2, y2)
        return seg in self.used_segments

    def get_segment_usage_count(self, x1: float, y1: float, x2: float, y2: float) -> int:
        """
        Get the number of times a segment has been used.

        Args:
            x1, y1: Start point
            x2, y2: End point

        Returns:
            Usage count (0 if never used)
        """
        seg = Segment(x1, y1, x2, y2)
        return self.segment_usage.get(seg, 0)

    def add_path(self, waypoints: list[Tuple[float, float]]):
        """
        Add an entire path (sequence of waypoints) to the registry.

        Args:
            waypoints: List of (x, y) coordinate tuples
        """
        for i in range(len(waypoints) - 1):
            x1, y1 = waypoints[i]
            x2, y2 = waypoints[i + 1]
            self.add_segment(x1, y1, x2, y2)

    def get_preferred_offset(self, base_position: int, is_vertical: bool, max_offset: int = 50) -> int:
        """
        Calculate preferred lane offset to avoid busy lanes.

        Scans nearby positions and returns the one with least usage.

        Args:
            base_position: Ideal position (e.g., midpoint between tables)
            is_vertical: True for vertical lane, False for horizontal
            max_offset: Maximum distance to search from base position

        Returns:
            Offset from base position (-max_offset to +max_offset)
        """
        best_offset = 0
        best_cost = self.get_lane_cost(base_position, is_vertical)

        # Try offsets in both directions
        for offset in range(-max_offset, max_offset + 1, 10):  # Step by 10px
            pos = base_position + offset
            cost = self.get_lane_cost(pos, is_vertical)

            if cost < best_cost:
                best_cost = cost
                best_offset = offset

        return best_offset
