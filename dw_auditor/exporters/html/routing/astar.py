"""
A* pathfinding for ER diagram routing.

Implements A* algorithm with custom cost function that considers:
- Path length (Manhattan distance)
- Direction changes (prefer straight lines)
- Lane usage (avoid busy lanes)
- Corridor continuation (reward staying in corridors)
"""

from typing import List, Tuple, Optional
import heapq
from dataclasses import dataclass, field

from .grid import Grid, GridCell
from .lane_manager import LaneRegistry


@dataclass(order=True)
class Node:
    """Node in A* search."""
    f_cost: float = field(compare=True)  # f = g + h
    g_cost: float = field(compare=False)  # Cost from start
    h_cost: float = field(compare=False)  # Heuristic to goal
    cell: GridCell = field(compare=False)
    parent: Optional['Node'] = field(default=None, compare=False)

    def __hash__(self):
        return hash(self.cell)


def manhattan_distance(cell1: GridCell, cell2: GridCell) -> int:
    """Calculate Manhattan distance between two cells."""
    return abs(cell1.x - cell2.x) + abs(cell1.y - cell2.y)


def is_straight_move(parent: Optional[Node], current: Node, neighbor: GridCell) -> bool:
    """Check if move continues in same direction (no turn)."""
    if parent is None:
        return True  # First move

    # Get direction vectors
    prev_dx = current.cell.x - parent.cell.x
    prev_dy = current.cell.y - parent.cell.y

    next_dx = neighbor.x - current.cell.x
    next_dy = neighbor.y - current.cell.y

    # Same direction if vectors match
    return prev_dx == next_dx and prev_dy == next_dy


def calculate_cost(
    current: Node,
    neighbor: GridCell,
    end: GridCell,
    grid: Grid,
    lane_registry: Optional[LaneRegistry] = None
) -> Tuple[float, float]:
    """
    Calculate g_cost and h_cost for a neighbor node.

    Args:
        current: Current node
        neighbor: Neighbor cell to evaluate
        end: Goal cell
        grid: Grid system
        lane_registry: Optional lane usage tracker

    Returns:
        Tuple of (g_cost, h_cost)
    """
    # Base cost: distance from start
    g_cost = current.g_cost + 1

    # Direction change penalty
    if not is_straight_move(current.parent, current, neighbor):
        g_cost += 5  # Penalize turns

    # Lane usage penalty
    if lane_registry:
        # Determine if this is a vertical or horizontal move
        is_vertical_move = neighbor.x == current.cell.x
        is_horizontal_move = neighbor.y == current.cell.y

        if is_vertical_move:
            # Moving vertically - check vertical lane usage
            lane_pos = int(neighbor.x * grid.resolution)
            usage = lane_registry.get_lane_usage(lane_pos, is_vertical=True)
            g_cost += usage * 3  # Penalty proportional to usage

        elif is_horizontal_move:
            # Moving horizontally - check horizontal lane usage
            lane_pos = int(neighbor.y * grid.resolution)
            usage = lane_registry.get_lane_usage(lane_pos, is_vertical=False)
            g_cost += usage * 3  # Penalty proportional to usage

    # Straight corridor bonus
    if current.parent and is_straight_move(current.parent, current, neighbor):
        # Check if we're in a long straight corridor
        if current.parent.parent and is_straight_move(current.parent.parent, current.parent, current.cell):
            g_cost -= 2  # Reward continuing straight for 3+ cells

    # Heuristic: Manhattan distance to goal
    h_cost = manhattan_distance(neighbor, end)

    return (g_cost, h_cost)


def reconstruct_path(node: Node) -> List[GridCell]:
    """Reconstruct path from goal node by following parent pointers."""
    path = []
    current = node

    while current is not None:
        path.append(current.cell)
        current = current.parent

    path.reverse()
    return path


def astar_route(
    start: GridCell,
    end: GridCell,
    grid: Grid,
    lane_registry: Optional[LaneRegistry] = None
) -> Optional[List[GridCell]]:
    """
    Find optimal path from start to end using A* algorithm.

    Args:
        start: Starting grid cell
        end: Goal grid cell
        grid: Grid system with obstacles
        lane_registry: Optional lane usage tracker for cost calculation

    Returns:
        List of grid cells forming path, or None if no path exists
    """
    # Early exit if start or end is blocked
    if not grid.is_traversable(start) or not grid.is_traversable(end):
        return None

    # Check if straight line is possible
    if grid.is_line_clear(start, end):
        return [start, end]

    # Initialize search
    open_set = []  # Priority queue (heap)
    closed_set = set()  # Visited nodes

    # Create start node
    start_node = Node(
        f_cost=manhattan_distance(start, end),
        g_cost=0,
        h_cost=manhattan_distance(start, end),
        cell=start,
        parent=None
    )

    heapq.heappush(open_set, start_node)

    # Track best g_cost to each cell
    best_g_cost = {start: 0.0}

    # A* search
    while open_set:
        current = heapq.heappop(open_set)

        # Goal reached
        if current.cell == end:
            return reconstruct_path(current)

        # Already visited
        if current.cell in closed_set:
            continue

        closed_set.add(current.cell)

        # Explore neighbors
        for neighbor_cell in grid.get_neighbors(current.cell):
            # Skip if already visited
            if neighbor_cell in closed_set:
                continue

            # Calculate costs
            g_cost, h_cost = calculate_cost(current, neighbor_cell, end, grid, lane_registry)
            f_cost = g_cost + h_cost

            # Skip if we've found a better path to this cell
            if neighbor_cell in best_g_cost and g_cost >= best_g_cost[neighbor_cell]:
                continue

            # Record best cost
            best_g_cost[neighbor_cell] = g_cost

            # Create neighbor node
            neighbor_node = Node(
                f_cost=f_cost,
                g_cost=g_cost,
                h_cost=h_cost,
                cell=neighbor_cell,
                parent=current
            )

            heapq.heappush(open_set, neighbor_node)

    # No path found
    return None


def route_with_waypoints(
    waypoints: List[Tuple[float, float]],
    grid: Grid,
    lane_registry: Optional[LaneRegistry] = None
) -> Optional[List[GridCell]]:
    """
    Route through multiple waypoints.

    Args:
        waypoints: List of (x, y) canvas coordinates to visit in order
        grid: Grid system
        lane_registry: Optional lane usage tracker

    Returns:
        Complete path visiting all waypoints, or None if impossible
    """
    if len(waypoints) < 2:
        return None

    complete_path = []

    for i in range(len(waypoints) - 1):
        start_x, start_y = waypoints[i]
        end_x, end_y = waypoints[i + 1]

        start_cell = grid.to_grid(start_x, start_y)
        end_cell = grid.to_grid(end_x, end_y)

        # Find path for this segment
        segment_path = astar_route(start_cell, end_cell, grid, lane_registry)

        if segment_path is None:
            return None  # Cannot complete route

        # Add segment (avoid duplicating connection point)
        if complete_path:
            complete_path.extend(segment_path[1:])
        else:
            complete_path.extend(segment_path)

    return complete_path
