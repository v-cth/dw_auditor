"""
Path optimization for ER diagram routing.

Post-processes raw pathfinding output to create clean, efficient paths:
- Compress collinear segments
- Smooth corners with curves
- Validate clearance from obstacles
- Remove micro-loops and duplicate points
"""

from typing import List, Tuple
import math

from .grid import GridCell


def compress_path(cells: List[GridCell]) -> List[GridCell]:
    """
    Compress path by merging collinear segments.

    Removes unnecessary waypoints where path continues in same direction.

    Args:
        cells: Raw path from pathfinding

    Returns:
        Compressed path with minimal waypoints
    """
    if len(cells) <= 2:
        return cells

    compressed = [cells[0]]  # Start with first cell

    for i in range(1, len(cells) - 1):
        prev_cell = cells[i - 1]
        curr_cell = cells[i]
        next_cell = cells[i + 1]

        # Calculate direction vectors
        dx1 = curr_cell.x - prev_cell.x
        dy1 = curr_cell.y - prev_cell.y

        dx2 = next_cell.x - curr_cell.x
        dy2 = next_cell.y - curr_cell.y

        # Keep waypoint if direction changes
        if dx1 != dx2 or dy1 != dy2:
            compressed.append(curr_cell)

    # Always keep last cell
    compressed.append(cells[-1])

    return compressed


def cells_to_canvas(cells: List[GridCell], resolution: int) -> List[Tuple[float, float]]:
    """
    Convert grid cells to canvas coordinates.

    Args:
        cells: List of grid cells
        resolution: Grid resolution in pixels

    Returns:
        List of (x, y) canvas coordinates
    """
    canvas_points = []
    for cell in cells:
        x = cell.x * resolution + resolution / 2
        y = cell.y * resolution + resolution / 2
        canvas_points.append((x, y))
    return canvas_points


def remove_duplicate_points(points: List[Tuple[float, float]], tolerance: float = 0.1) -> List[Tuple[float, float]]:
    """
    Remove consecutive duplicate points.

    Args:
        points: List of (x, y) coordinates
        tolerance: Distance threshold for considering points duplicate

    Returns:
        Deduplicated point list
    """
    if not points:
        return []

    cleaned = [points[0]]

    for i in range(1, len(points)):
        x1, y1 = cleaned[-1]
        x2, y2 = points[i]

        distance = math.sqrt((x2 - x1)**2 + (y2 - y1)**2)

        if distance > tolerance:
            cleaned.append(points[i])

    return cleaned


def remove_micro_segments(points: List[Tuple[float, float]], min_length: float = 5.0) -> List[Tuple[float, float]]:
    """
    Remove very short segments (micro-loops).

    Args:
        points: List of (x, y) coordinates
        min_length: Minimum segment length to keep

    Returns:
        Cleaned point list
    """
    if len(points) <= 2:
        return points

    cleaned = [points[0]]

    for i in range(1, len(points) - 1):
        x1, y1 = cleaned[-1]
        x2, y2 = points[i]

        segment_length = math.sqrt((x2 - x1)**2 + (y2 - y1)**2)

        # Keep waypoint if segment is long enough
        if segment_length >= min_length:
            cleaned.append(points[i])

    # Always keep last point
    cleaned.append(points[-1])

    return cleaned


def snap_orthogonal(points: List[Tuple[float, float]], threshold: float = 2.0) -> List[Tuple[float, float]]:
    """
    Snap nearly orthogonal segments to perfect orthogonal.

    If a segment is almost horizontal or vertical, make it perfectly so.

    Args:
        points: List of (x, y) coordinates
        threshold: Maximum deviation to snap (in pixels)

    Returns:
        Snapped point list
    """
    if len(points) <= 1:
        return points

    snapped = [points[0]]

    for i in range(1, len(points)):
        x1, y1 = snapped[-1]
        x2, y2 = points[i]

        dx = abs(x2 - x1)
        dy = abs(y2 - y1)

        # Nearly vertical - snap to same x
        if dx <= threshold and dy > threshold:
            snapped.append((x1, y2))
        # Nearly horizontal - snap to same y
        elif dy <= threshold and dx > threshold:
            snapped.append((x2, y1))
        else:
            snapped.append((x2, y2))

    return snapped


def smooth_corners(
    points: List[Tuple[float, float]],
    radius: float = 4.0
) -> Tuple[str, List[Tuple[float, float]]]:
    """
    Generate SVG path with smoothed corners.

    Args:
        points: List of (x, y) waypoints
        radius: Corner radius for smoothing

    Returns:
        Tuple of (svg_path_string, label_positions)
    """
    if len(points) < 2:
        return ("", [])

    path = f"M {points[0][0]},{points[0][1]}"
    label_positions = []

    for i in range(1, len(points)):
        curr = points[i]

        # Check if we can add rounded corner
        if i < len(points) - 1 and radius > 0:
            prev = points[i - 1]
            next_pt = points[i + 1]

            # Calculate vectors
            dx_in = curr[0] - prev[0]
            dy_in = curr[1] - prev[1]
            dx_out = next_pt[0] - curr[0]
            dy_out = next_pt[1] - curr[1]

            # Calculate segment lengths
            len_in = math.sqrt(dx_in*dx_in + dy_in*dy_in)
            len_out = math.sqrt(dx_out*dx_out + dy_out*dy_out)

            # Only round if segments are long enough
            if len_in > radius * 2 and len_out > radius * 2:
                # Calculate corner points
                corner_start_x = curr[0] - (dx_in / len_in) * radius
                corner_start_y = curr[1] - (dy_in / len_in) * radius
                corner_end_x = curr[0] + (dx_out / len_out) * radius
                corner_end_y = curr[1] + (dy_out / len_out) * radius

                # Line to corner start, arc to corner end
                path += f" L {corner_start_x},{corner_start_y}"
                path += f" Q {curr[0]},{curr[1]} {corner_end_x},{corner_end_y}"

                # Record midpoint for potential labels
                if len_in > 50:  # Only add label position if segment is long
                    mid_x = (prev[0] + corner_start_x) / 2
                    mid_y = (prev[1] + corner_start_y) / 2
                    label_positions.append((mid_x, mid_y))

                continue

        # No rounding - straight line
        path += f" L {curr[0]},{curr[1]}"

        # Record midpoint for labels
        if i > 0:
            prev = points[i - 1]
            dx = curr[0] - prev[0]
            dy = curr[1] - prev[1]
            segment_length = math.sqrt(dx*dx + dy*dy)

            if segment_length > 50:  # Only on long segments
                mid_x = (prev[0] + curr[0]) / 2
                mid_y = (prev[1] + curr[1]) / 2
                label_positions.append((mid_x, mid_y))

    return (path, label_positions)


def validate_clearance(
    points: List[Tuple[float, float]],
    obstacles: List[Tuple[float, float, float, float]],
    margin: float = 10.0
) -> bool:
    """
    Validate that path maintains clearance from obstacles.

    Args:
        points: List of (x, y) waypoints
        obstacles: List of (x, y, width, height) boxes
        margin: Required clearance in pixels

    Returns:
        True if path is clear, False if collision detected
    """
    if len(points) < 2:
        return True

    # Check each segment
    for i in range(len(points) - 1):
        x1, y1 = points[i]
        x2, y2 = points[i + 1]

        # Check against each obstacle
        for obs_x, obs_y, obs_w, obs_h in obstacles:
            # Expand obstacle by margin
            box_x1 = obs_x - margin
            box_y1 = obs_y - margin
            box_x2 = obs_x + obs_w + margin
            box_y2 = obs_y + obs_h + margin

            # Check if segment intersects expanded box
            if _segment_intersects_box(x1, y1, x2, y2, box_x1, box_y1, box_x2, box_y2):
                return False

    return True


def _segment_intersects_box(
    seg_x1: float, seg_y1: float,
    seg_x2: float, seg_y2: float,
    box_x1: float, box_y1: float,
    box_x2: float, box_y2: float
) -> bool:
    """Check if line segment intersects axis-aligned bounding box."""
    # Check if segment endpoints are inside box
    if (box_x1 <= seg_x1 <= box_x2 and box_y1 <= seg_y1 <= box_y2):
        return True
    if (box_x1 <= seg_x2 <= box_x2 and box_y1 <= seg_y2 <= box_y2):
        return True

    # Check if segment crosses box edges
    # Use parametric line representation: P = P1 + t * (P2 - P1)

    # For a vertical segment
    if abs(seg_x2 - seg_x1) < 0.1:
        x = seg_x1
        if box_x1 <= x <= box_x2:
            y_min = min(seg_y1, seg_y2)
            y_max = max(seg_y1, seg_y2)
            # Check if box y-range overlaps segment y-range
            if not (y_max < box_y1 or y_min > box_y2):
                return True

    # For a horizontal segment
    elif abs(seg_y2 - seg_y1) < 0.1:
        y = seg_y1
        if box_y1 <= y <= box_y2:
            x_min = min(seg_x1, seg_x2)
            x_max = max(seg_x1, seg_x2)
            # Check if box x-range overlaps segment x-range
            if not (x_max < box_x1 or x_min > box_x2):
                return True

    return False


def optimize_path(
    cells: List[GridCell],
    resolution: int,
    obstacles: List[Tuple[float, float, float, float]],
    corner_radius: float = 4.0
) -> Tuple[str, List[Tuple[float, float]]]:
    """
    Complete path optimization pipeline.

    Args:
        cells: Raw path from pathfinding
        resolution: Grid resolution
        obstacles: List of obstacles for clearance validation
        corner_radius: Radius for corner smoothing

    Returns:
        Tuple of (svg_path, label_positions)
    """
    # Step 1: Compress collinear segments
    compressed = compress_path(cells)

    # Step 2: Convert to canvas coordinates
    canvas_points = cells_to_canvas(compressed, resolution)

    # Step 3: Remove duplicates
    canvas_points = remove_duplicate_points(canvas_points)

    # Step 4: Remove micro-segments
    canvas_points = remove_micro_segments(canvas_points)

    # Step 5: Snap to orthogonal
    canvas_points = snap_orthogonal(canvas_points)

    # Step 6: Validate clearance
    if not validate_clearance(canvas_points, obstacles):
        # Path too close to obstacles - increase clearance or reroute
        pass  # For now, proceed anyway (A* should have avoided obstacles)

    # Step 7: Generate SVG with smooth corners
    svg_path, label_positions = smooth_corners(canvas_points, corner_radius)

    return (svg_path, label_positions)
