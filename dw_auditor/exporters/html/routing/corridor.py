"""
Corridor scanning for fast routing in simple cases.

Provides heuristic-based routing that finds straight corridors
between tables before falling back to expensive A* pathfinding.
"""

from typing import List, Tuple, Optional
from dataclasses import dataclass


@dataclass
class Corridor:
    """Represents a clear corridor (vertical or horizontal gap)."""
    position: float  # x-coordinate (vertical) or y-coordinate (horizontal)
    is_vertical: bool
    start: float  # min y (vertical) or min x (horizontal)
    end: float    # max y (vertical) or max x (horizontal)

    def width(self) -> float:
        """Get corridor width."""
        return self.end - self.start

    def contains(self, point: float) -> bool:
        """Check if a coordinate is within this corridor."""
        return self.start <= point <= self.end


def scan_vertical_corridors(
    start: Tuple[float, float],
    end: Tuple[float, float],
    obstacles: List[Tuple[float, float, float, float]],
    min_width: float = 40
) -> List[Corridor]:
    """
    Find vertical corridors (constant x) between start and end points.

    Args:
        start: (x, y) start point
        end: (x, y) end point
        obstacles: List of (x, y, width, height) boxes
        min_width: Minimum corridor width to consider valid

    Returns:
        List of valid vertical corridors
    """
    start_x, start_y = start
    end_x, end_y = end

    # Determine x-coordinate range to scan
    min_x = min(start_x, end_x)
    max_x = max(start_x, end_x)

    # Collect all x-coordinates where obstacles start and end
    x_positions = set()
    for obs_x, obs_y, obs_w, obs_h in obstacles:
        x_positions.add(obs_x)  # Left edge
        x_positions.add(obs_x + obs_w)  # Right edge

    # Add some candidate positions in the middle
    for x in range(int(min_x), int(max_x) + 10, 10):
        x_positions.add(x)

    # Also add midpoint
    mid_x = (start_x + end_x) / 2
    x_positions.add(mid_x)

    corridors = []

    # Check each x-position
    for x in sorted(x_positions):
        if x < min_x or x > max_x:
            continue

        # Check if this vertical line intersects any obstacles
        y_ranges = []  # Collect blocked y-ranges

        for obs_x, obs_y, obs_w, obs_h in obstacles:
            # Check if obstacle blocks this x-coordinate
            if obs_x <= x <= obs_x + obs_w:
                y_ranges.append((obs_y, obs_y + obs_h))

        # Merge overlapping ranges
        y_ranges.sort()
        merged_ranges = []
        for y_start, y_end in y_ranges:
            if merged_ranges and y_start <= merged_ranges[-1][1]:
                # Overlaps with previous range - merge
                merged_ranges[-1] = (merged_ranges[-1][0], max(merged_ranges[-1][1], y_end))
            else:
                merged_ranges.append((y_start, y_end))

        # Find gaps (clear corridors)
        min_y = min(start_y, end_y)
        max_y = max(start_y, end_y)

        prev_end = min_y - min_width  # Start before first range

        for y_start, y_end in merged_ranges:
            # Check gap between prev_end and y_start
            gap_start = prev_end
            gap_end = y_start

            if gap_end - gap_start >= min_width:
                # Valid corridor found
                if gap_start <= max_y and gap_end >= min_y:
                    corridors.append(Corridor(
                        position=x,
                        is_vertical=True,
                        start=max(gap_start, min_y),
                        end=min(gap_end, max_y)
                    ))

            prev_end = y_end

        # Check gap after last obstacle
        gap_start = prev_end
        gap_end = max_y + min_width

        if gap_end - gap_start >= min_width:
            if gap_start <= max_y:
                corridors.append(Corridor(
                    position=x,
                    is_vertical=True,
                    start=max(gap_start, min_y),
                    end=min(gap_end, max_y)
                ))

    return corridors


def scan_horizontal_corridors(
    start: Tuple[float, float],
    end: Tuple[float, float],
    obstacles: List[Tuple[float, float, float, float]],
    min_width: float = 40
) -> List[Corridor]:
    """
    Find horizontal corridors (constant y) between start and end points.

    Args:
        start: (x, y) start point
        end: (x, y) end point
        obstacles: List of (x, y, width, height) boxes
        min_width: Minimum corridor width to consider valid

    Returns:
        List of valid horizontal corridors
    """
    start_x, start_y = start
    end_x, end_y = end

    # Determine y-coordinate range to scan
    min_y = min(start_y, end_y)
    max_y = max(start_y, end_y)

    # Collect all y-coordinates where obstacles start and end
    y_positions = set()
    for obs_x, obs_y, obs_w, obs_h in obstacles:
        y_positions.add(obs_y)  # Top edge
        y_positions.add(obs_y + obs_h)  # Bottom edge

    # Add some candidate positions in the middle
    for y in range(int(min_y), int(max_y) + 10, 10):
        y_positions.add(y)

    # Also add midpoint
    mid_y = (start_y + end_y) / 2
    y_positions.add(mid_y)

    corridors = []

    # Check each y-position
    for y in sorted(y_positions):
        if y < min_y or y > max_y:
            continue

        # Check if this horizontal line intersects any obstacles
        x_ranges = []  # Collect blocked x-ranges

        for obs_x, obs_y_pos, obs_w, obs_h in obstacles:
            # Check if obstacle blocks this y-coordinate
            if obs_y_pos <= y <= obs_y_pos + obs_h:
                x_ranges.append((obs_x, obs_x + obs_w))

        # Merge overlapping ranges
        x_ranges.sort()
        merged_ranges = []
        for x_start, x_end in x_ranges:
            if merged_ranges and x_start <= merged_ranges[-1][1]:
                # Overlaps with previous range - merge
                merged_ranges[-1] = (merged_ranges[-1][0], max(merged_ranges[-1][1], x_end))
            else:
                merged_ranges.append((x_start, x_end))

        # Find gaps (clear corridors)
        min_x = min(start_x, end_x)
        max_x = max(start_x, end_x)

        prev_end = min_x - min_width  # Start before first range

        for x_start, x_end in merged_ranges:
            # Check gap between prev_end and x_start
            gap_start = prev_end
            gap_end = x_start

            if gap_end - gap_start >= min_width:
                # Valid corridor found
                if gap_start <= max_x and gap_end >= min_x:
                    corridors.append(Corridor(
                        position=y,
                        is_vertical=False,
                        start=max(gap_start, min_x),
                        end=min(gap_end, max_x)
                    ))

            prev_end = x_end

        # Check gap after last obstacle
        gap_start = prev_end
        gap_end = max_x + min_width

        if gap_end - gap_start >= min_width:
            if gap_start <= max_x:
                corridors.append(Corridor(
                    position=y,
                    is_vertical=False,
                    start=max(gap_start, min_x),
                    end=min(gap_end, max_x)
                ))

    return corridors


def scan_corridors(
    start: Tuple[float, float],
    end: Tuple[float, float],
    obstacles: List[Tuple[float, float, float, float]]
) -> Tuple[List[Corridor], List[Corridor]]:
    """
    Scan for both vertical and horizontal corridors.

    Args:
        start: (x, y) start point
        end: (x, y) end point
        obstacles: List of (x, y, width, height) boxes

    Returns:
        Tuple of (vertical_corridors, horizontal_corridors)
    """
    vertical = scan_vertical_corridors(start, end, obstacles)
    horizontal = scan_horizontal_corridors(start, end, obstacles)
    return (vertical, horizontal)


def select_best_corridor(
    corridors: List[Corridor],
    ideal_position: float,
    lane_registry=None
) -> Optional[Corridor]:
    """
    Select the best corridor from candidates.

    Prefers corridors close to ideal_position with low lane usage.

    Args:
        corridors: List of candidate corridors
        ideal_position: Preferred position (e.g., midpoint)
        lane_registry: Optional LaneRegistry for usage-based scoring

    Returns:
        Best corridor, or None if no candidates
    """
    if not corridors:
        return None

    scored = []

    for corridor in corridors:
        # Distance from ideal position
        distance_score = abs(corridor.position - ideal_position)

        # Lane usage penalty (if registry provided)
        usage_score = 0
        if lane_registry:
            usage = lane_registry.get_lane_usage(
                int(corridor.position),
                corridor.is_vertical
            )
            usage_score = usage * 10  # Penalty for busy lanes

        # Total score (lower is better)
        total_score = distance_score + usage_score

        scored.append((total_score, corridor))

    # Return corridor with lowest score
    return min(scored, key=lambda x: x[0])[1]
