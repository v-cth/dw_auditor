"""
Grid-based A* routing system for ER diagram relationship lines.

This package provides sophisticated pathfinding to eliminate line overlap
and box intersections in entity-relationship diagrams.
"""

from .grid import Grid
from .lane_manager import LaneRegistry
from .corridor import scan_corridors, select_best_corridor
from .astar import astar_route
from .path_optimizer import compress_path, smooth_corners, validate_clearance

__all__ = [
    'Grid',
    'LaneRegistry',
    'scan_corridors',
    'select_best_corridor',
    'astar_route',
    'compress_path',
    'smooth_corners',
    'validate_clearance',
]
