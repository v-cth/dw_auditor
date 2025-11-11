"""
Grid system for A* pathfinding in ER diagrams.

Manages a virtual grid overlaying the diagram canvas, marking
obstacles (table boxes) and providing traversable cell lookup.
"""

from typing import List, Tuple, Set
from dataclasses import dataclass


@dataclass(frozen=True)
class GridCell:
    """Represents a cell in the routing grid."""
    x: int
    y: int

    def __hash__(self):
        return hash((self.x, self.y))

    def __eq__(self, other):
        return self.x == other.x and self.y == other.y


class Grid:
    """
    Grid system for pathfinding.

    Converts continuous canvas coordinates to discrete grid cells
    and tracks which cells are blocked by obstacles.
    """

    def __init__(self, width: float, height: float, resolution: int = 30):
        """
        Initialize grid.

        Args:
            width: Canvas width in pixels
            height: Canvas height in pixels
            resolution: Grid cell size in pixels (default: 30)
        """
        self.width = width
        self.height = height
        self.resolution = resolution

        # Calculate grid dimensions
        self.cols = int(width / resolution) + 1
        self.rows = int(height / resolution) + 1

        # Track blocked cells
        self.blocked: Set[GridCell] = set()

    def to_grid(self, x: float, y: float) -> GridCell:
        """Convert canvas coordinates to grid cell."""
        grid_x = int(x / self.resolution)
        grid_y = int(y / self.resolution)
        return GridCell(grid_x, grid_y)

    def from_grid(self, cell: GridCell) -> Tuple[float, float]:
        """Convert grid cell to canvas coordinates (cell center)."""
        x = cell.x * self.resolution + self.resolution / 2
        y = cell.y * self.resolution + self.resolution / 2
        return (x, y)

    def mark_obstacle(self, box: Tuple[float, float, float, float], margin: int = 20):
        """
        Mark a rectangular region as blocked.

        Args:
            box: (x, y, width, height) of obstacle
            margin: Additional clearance around obstacle in pixels
        """
        x, y, w, h = box

        # Expand box by margin
        x1 = x - margin
        y1 = y - margin
        x2 = x + w + margin
        y2 = y + h + margin

        # Convert to grid cells and mark all cells in region
        cell1 = self.to_grid(max(0, x1), max(0, y1))
        cell2 = self.to_grid(min(self.width, x2), min(self.height, y2))

        for gx in range(cell1.x, cell2.x + 1):
            for gy in range(cell1.y, cell2.y + 1):
                if 0 <= gx < self.cols and 0 <= gy < self.rows:
                    self.blocked.add(GridCell(gx, gy))

    def is_blocked(self, cell: GridCell) -> bool:
        """Check if a cell is blocked."""
        return cell in self.blocked

    def is_valid(self, cell: GridCell) -> bool:
        """Check if a cell is within grid bounds."""
        return 0 <= cell.x < self.cols and 0 <= cell.y < self.rows

    def is_traversable(self, cell: GridCell) -> bool:
        """Check if a cell can be traversed (valid and not blocked)."""
        return self.is_valid(cell) and not self.is_blocked(cell)

    def get_neighbors(self, cell: GridCell) -> List[GridCell]:
        """
        Get orthogonal neighbors (no diagonals).

        Returns only traversable neighbors.
        """
        neighbors = []

        # Check 4 orthogonal directions
        for dx, dy in [(0, -1), (1, 0), (0, 1), (-1, 0)]:  # Up, Right, Down, Left
            neighbor = GridCell(cell.x + dx, cell.y + dy)
            if self.is_traversable(neighbor):
                neighbors.append(neighbor)

        return neighbors

    def get_line_cells(self, start: GridCell, end: GridCell) -> List[GridCell]:
        """
        Get all cells along a straight line (Bresenham's algorithm).

        Used for checking if a straight-line path is clear.
        """
        cells = []

        x0, y0 = start.x, start.y
        x1, y1 = end.x, end.y

        dx = abs(x1 - x0)
        dy = abs(y1 - y0)

        sx = 1 if x0 < x1 else -1
        sy = 1 if y0 < y1 else -1

        err = dx - dy

        x, y = x0, y0

        while True:
            cells.append(GridCell(x, y))

            if x == x1 and y == y1:
                break

            e2 = 2 * err

            if e2 > -dy:
                err -= dy
                x += sx

            if e2 < dx:
                err += dx
                y += sy

        return cells

    def is_line_clear(self, start: GridCell, end: GridCell) -> bool:
        """Check if a straight line between two cells is unobstructed."""
        cells = self.get_line_cells(start, end)
        return all(self.is_traversable(cell) for cell in cells)
