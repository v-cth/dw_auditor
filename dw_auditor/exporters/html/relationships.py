"""
HTML generation for table relationship visualizations
"""

from typing import List, Dict, Optional, Tuple, Set
import json
import math

# Import new routing system
from .routing import (
    Grid, LaneRegistry,
    scan_corridors, select_best_corridor,
    astar_route
)


def _get_table_columns_for_diagram(table_name: str, relationships: List[Dict], tables_metadata: Dict[str, Dict]) -> Dict[str, List[str]]:
    """
    Extract columns to display in ER diagram for a table

    Args:
        table_name: Name of the table
        relationships: All relationships
        tables_metadata: Table metadata with primary key info

    Returns:
        Dict with 'pk', 'fk', and 'other' column lists
    """
    # Get primary key columns from metadata
    table_meta = tables_metadata.get(table_name, {})
    pk_columns = table_meta.get('primary_key_columns', [])
    if isinstance(pk_columns, str):
        pk_columns = [pk_columns]

    # Get foreign key columns (columns involved in relationships)
    fk_columns = set()
    for rel in relationships:
        if rel['table1'] == table_name:
            fk_columns.add(rel['column1'])
        elif rel['table2'] == table_name:
            fk_columns.add(rel['column2'])

    # Remove PKs from FK list (they'll be shown as PK)
    fk_columns = fk_columns - set(pk_columns)

    return {
        'pk': pk_columns,
        'fk': sorted(fk_columns),
        'other': []  # Don't show other columns for now to keep diagram clean
    }


def _calculate_table_positions(tables: List[str], num_cols: int = 3) -> Dict[str, Tuple[int, int]]:
    """
    Calculate grid positions for tables

    Args:
        tables: List of table names
        num_cols: Number of columns in grid

    Returns:
        Dict mapping table name to (x, y) position
    """
    positions = {}
    table_width = 280
    table_height = 200
    h_spacing = 150
    v_spacing = 100

    for idx, table in enumerate(tables):
        col = idx % num_cols
        row = idx // num_cols
        x = 50 + col * (table_width + h_spacing)
        y = 50 + row * (table_height + v_spacing)
        positions[table] = (x, y)

    return positions


def _snap_to_grid(value: float, grid_size: int = 10) -> float:
    """Snap a coordinate to the nearest grid point"""
    return round(value / grid_size) * grid_size


def _snap_to_box_edge(box_x: float, box_y: float, box_width: float, box_height: float,
                      target_x: float, target_y: float) -> Tuple[float, float, str]:
    """
    Find the point on the box perimeter closest to a target point

    Args:
        box_x, box_y: Top-left corner of box
        box_width, box_height: Box dimensions
        target_x, target_y: Target point to connect to

    Returns:
        (x, y, side) tuple of the closest point on box edge and which side ('top', 'right', 'bottom', 'left')
    """
    # Calculate box center
    center_x = box_x + box_width / 2
    center_y = box_y + box_height / 2

    # Vector from center to target
    dx = target_x - center_x
    dy = target_y - center_y

    # Normalize if non-zero
    if abs(dx) < 0.001 and abs(dy) < 0.001:
        return (center_x, center_y, 'right')

    # Calculate which edge to snap to based on angle
    # Use parametric approach: find t where ray intersects box
    t_x = float('inf')
    t_y = float('inf')

    if dx > 0:
        t_x = (box_width / 2) / abs(dx)
    elif dx < 0:
        t_x = (box_width / 2) / abs(dx)

    if dy > 0:
        t_y = (box_height / 2) / abs(dy)
    elif dy < 0:
        t_y = (box_height / 2) / abs(dy)

    # Use the smaller t (closer intersection)
    t = min(t_x, t_y)

    # Calculate intersection point
    edge_x = center_x + dx * t
    edge_y = center_y + dy * t

    # Clamp to box bounds and determine side
    edge_x = max(box_x, min(box_x + box_width, edge_x))
    edge_y = max(box_y, min(box_y + box_height, edge_y))

    # Determine which side we're on
    if abs(edge_x - box_x) < 1:
        side = 'left'
    elif abs(edge_x - (box_x + box_width)) < 1:
        side = 'right'
    elif abs(edge_y - box_y) < 1:
        side = 'top'
    else:
        side = 'bottom'

    return (edge_x, edge_y, side)


def _boxes_overlap_segment(box_list: List[Tuple[float, float, float, float]],
                           x1: float, y1: float, x2: float, y2: float, margin: float = 5) -> bool:
    """Check if a line segment intersects any box"""
    for bx, by, bw, bh in box_list:
        # Expand box by margin
        if x1 == x2:  # Vertical line
            if (bx - margin <= x1 <= bx + bw + margin and
                not (max(y1, y2) < by - margin or min(y1, y2) > by + bh + margin)):
                return True
        elif y1 == y2:  # Horizontal line
            if (by - margin <= y1 <= by + bh + margin and
                not (max(x1, x2) < bx - margin or min(x1, x2) > bx + bw + margin)):
                return True
    return False


def _create_orthogonal_path(start_x: float, start_y: float, start_side: str,
                            end_x: float, end_y: float, end_side: str,
                            lane_offset: float, all_boxes: List[Tuple[float, float, float, float]],
                            corner_radius: int = 4,
                            grid: Optional[Grid] = None,
                            lane_registry: Optional[LaneRegistry] = None) -> Tuple[str, List[Tuple[float, float]]]:
    """
    Create an orthogonal path using grid-based A* routing with collision avoidance.

    Args:
        start_x, start_y: Starting point on box edge
        start_side: Side of starting box ('top', 'right', 'bottom', 'left')
        end_x, end_y: Ending point on box edge
        end_side: Side of ending box
        lane_offset: Offset for parallel lines (lane system)
        all_boxes: List of (x, y, w, h) for all table boxes for collision detection
        corner_radius: Radius for rounded corners (3-6px)
        grid: Optional pre-configured Grid (creates new if None)
        lane_registry: Optional LaneRegistry for global lane management

    Returns:
        Tuple of (SVG path string, list of label positions on straight segments)
    """
    # Dynamic clearance based on obstacle density
    obstacle_count_between = sum(
        1 for bx, by, bw, bh in all_boxes
        if (min(start_x, end_x) <= bx + bw and max(start_x, end_x) >= bx and
            min(start_y, end_y) <= by + bh and max(start_y, end_y) >= by)
    )
    base_clearance = 60 + (obstacle_count_between * 60)

    clearance = base_clearance + abs(lane_offset)

    # Calculate exit points (perpendicular exit from edge)
    if start_side == 'right':
        exit_x = start_x + clearance
        exit_y = start_y
    elif start_side == 'left':
        exit_x = start_x - clearance
        exit_y = start_y
    elif start_side == 'top':
        exit_x = start_x
        exit_y = start_y - clearance
    else:  # bottom
        exit_x = start_x
        exit_y = start_y + clearance

    # Calculate entry points (perpendicular approach to target)
    if end_side == 'right':
        entry_x = end_x + clearance
        entry_y = end_y
    elif end_side == 'left':
        entry_x = end_x - clearance
        entry_y = end_y
    elif end_side == 'top':
        entry_x = end_x
        entry_y = end_y - clearance
    else:  # bottom
        entry_x = end_x
        entry_y = end_y + clearance

    # Initialize grid if not provided
    if grid is None:
        # Estimate canvas size from boxes
        max_x = max((bx + bw for bx, by, bw, bh in all_boxes), default=1500)
        max_y = max((by + bh for bx, by, bw, bh in all_boxes), default=1000)
        grid = Grid(max_x + 200, max_y + 200, resolution=30)

        # Mark all boxes as obstacles
        for box in all_boxes:
            grid.mark_obstacle(box, margin=20)

    # Try corridor scan first (fast for simple cases)
    vertical_corridors, horizontal_corridors = scan_corridors(
        (exit_x, exit_y), (entry_x, entry_y), all_boxes
    )

    # Determine routing strategy based on edge orientations
    use_vertical_corridor = start_side in ['left', 'right'] and end_side in ['left', 'right']
    use_horizontal_corridor = start_side in ['top', 'bottom'] and end_side in ['top', 'bottom']

    corridor_route = None

    if use_vertical_corridor and vertical_corridors:
        # Try corridor routing
        ideal_x = (exit_x + entry_x) / 2
        best_corridor = select_best_corridor(vertical_corridors, ideal_x, lane_registry)

        if best_corridor:
            # Build simple 4-waypoint path through corridor
            corridor_route = [
                (start_x, start_y),
                (exit_x, exit_y),
                (best_corridor.position, exit_y),
                (best_corridor.position, entry_y),
                (entry_x, entry_y),
                (end_x, end_y)
            ]

    elif use_horizontal_corridor and horizontal_corridors:
        # Try corridor routing
        ideal_y = (exit_y + entry_y) / 2
        best_corridor = select_best_corridor(horizontal_corridors, ideal_y, lane_registry)

        if best_corridor:
            # Build simple 4-waypoint path through corridor
            corridor_route = [
                (start_x, start_y),
                (exit_x, exit_y),
                (exit_x, best_corridor.position),
                (entry_x, best_corridor.position),
                (entry_x, entry_y),
                (end_x, end_y)
            ]

    # If corridor routing succeeded, use it
    if corridor_route:
        waypoints = corridor_route
    else:
        # Fall back to A* routing
        start_cell = grid.to_grid(exit_x, exit_y)
        end_cell = grid.to_grid(entry_x, entry_y)

        cell_path = astar_route(start_cell, end_cell, grid, lane_registry)

        if cell_path:
            # Convert grid cells to canvas coordinates
            from .routing.path_optimizer import cells_to_canvas
            middle_waypoints = cells_to_canvas(cell_path, grid.resolution)

            # Build complete path: start → exit → A* path → entry → end
            waypoints = [(start_x, start_y), (exit_x, exit_y)]
            waypoints.extend(middle_waypoints)
            waypoints.extend([(entry_x, entry_y), (end_x, end_y)])
        else:
            # A* failed - fallback to simple direct path
            waypoints = [(start_x, start_y), (exit_x, exit_y), (entry_x, entry_y), (end_x, end_y)]

    # Register path segments in lane registry
    if lane_registry:
        lane_registry.add_path(waypoints)

    # Optimize path
    from .routing.path_optimizer import (
        remove_duplicate_points,
        remove_micro_segments,
        snap_orthogonal,
        smooth_corners
    )

    waypoints = remove_duplicate_points(waypoints)
    waypoints = remove_micro_segments(waypoints)
    waypoints = snap_orthogonal(waypoints)

    # Generate SVG with smooth corners
    path, label_positions = smooth_corners(waypoints, corner_radius)

    return path, label_positions


def _get_crow_foot_path(relationship_type: str, direction: str, is_start: bool) -> str:
    """
    Generate SVG path for crow's foot notation (deprecated, kept for compatibility)

    Args:
        relationship_type: Type of relationship (one-to-one, many-to-one, many-to-many)
        direction: Direction of relationship
        is_start: Whether this is the start or end of the line

    Returns:
        SVG path string for the crow's foot symbol
    """
    # Determine cardinality at this end
    if relationship_type == "one-to-one":
        cardinality = "one"
    elif relationship_type == "many-to-one":
        if direction == "table1_to_table2":
            cardinality = "many" if is_start else "one"
        elif direction == "table2_to_table1":
            cardinality = "one" if is_start else "many"
        else:
            cardinality = "one"
    elif relationship_type == "many-to-many":
        cardinality = "many"
    else:
        cardinality = "one"

    if cardinality == "one":
        # Perpendicular line for "one" side
        return "M -10,0 L 10,0 M 0,-10 L 0,10"
    else:
        # Crow's foot for "many" side (three prongs)
        return "M -10,-10 L 0,0 L -10,10 M 0,-10 L 0,10"


def generate_relationships_summary_section(relationships: List[Dict], tables_metadata: Dict[str, Dict], min_confidence: float = 0.5) -> str:
    """
    Generate ER diagram with crow's foot notation for summary.html

    Args:
        relationships: List of relationship dictionaries
        tables_metadata: Metadata about tables (row counts, column counts, primary keys)
        min_confidence: Minimum confidence to display

    Returns:
        HTML string with relationship section including interactive ER diagram
    """
    # Filter relationships by minimum display confidence
    display_relationships = [r for r in relationships if r['confidence'] >= min_confidence]

    if not display_relationships:
        return ""

    # Sort by confidence descending
    display_relationships.sort(key=lambda x: x['confidence'], reverse=True)

    # Get tables involved in relationships
    tables_in_relationships = set()
    for rel in display_relationships:
        tables_in_relationships.add(rel['table1'])
        tables_in_relationships.add(rel['table2'])

    tables_list = sorted(tables_in_relationships)

    # Calculate table positions
    positions = _calculate_table_positions(tables_list, num_cols=3)

    # Calculate SVG canvas size
    max_x = max(pos[0] for pos in positions.values()) + 300
    max_y = max(pos[1] for pos in positions.values()) + 250

    # Build table boxes SVG and track dimensions for endpoint snapping
    table_boxes_svg = ""
    table_dimensions = {}  # Store box dimensions for each table

    # Initialize global routing infrastructure for multi-pass routing
    grid = Grid(max_x, max_y, resolution=30)
    lane_registry = LaneRegistry()

    for table_name in tables_list:
        x, y = positions[table_name]
        columns = _get_table_columns_for_diagram(table_name, display_relationships, tables_metadata)

        # Get metadata
        metadata = tables_metadata.get(table_name, {})
        row_count = metadata.get('total_rows', 'N/A')
        if isinstance(row_count, int):
            row_count_str = f"{row_count:,}"
        else:
            row_count_str = str(row_count)

        # Calculate box dimensions based on number of columns
        total_columns = len(columns['pk']) + len(columns['fk'])
        # Reduce header and padding for tables with few/no columns
        if total_columns == 0:
            header_height = 35
            column_spacing = 0
            box_height = header_height
        elif total_columns <= 2:
            header_height = 38
            column_spacing = 20
            box_height = header_height + (total_columns * column_spacing)
        else:
            header_height = 40
            column_spacing = 22
            box_height = header_height + 20 + (total_columns * column_spacing)

        box_width = 280
        table_dimensions[table_name] = (x, y, box_width, box_height)

        # Create table box
        table_boxes_svg += f"""
        <g class="er-table" data-table="{table_name}">
            <rect x="{x}" y="{y}" width="{box_width}" height="{box_height}"
                  fill="white" stroke="#d1d5db" stroke-width="1.5" rx="6"/>
            <rect x="{x}" y="{y}" width="{box_width}" height="{header_height}"
                  fill="#6606dc" rx="6"/>
            <rect x="{x}" y="{y + header_height}" width="{box_width}" height="{box_height - header_height}"
                  fill="white" rx="0"/>
            <text x="{x + box_width/2}" y="{y + header_height/2 + 2}"
                  font-family="Inter, sans-serif" font-size="14" font-weight="600"
                  fill="white" text-anchor="middle">{table_name}</text>
            <text x="{x + box_width/2}" y="{y + header_height - 5}"
                  font-family="Inter, sans-serif" font-size="10"
                  fill="rgba(255,255,255,0.8)" text-anchor="middle">{row_count_str} rows</text>
        """

        # Add columns
        y_offset = y + header_height + 18
        for pk_col in columns['pk']:
            table_boxes_svg += f"""
            <text x="{x + 10}" y="{y_offset}"
                  font-family="'Courier New', monospace" font-size="12" font-weight="bold"
                  fill="#1f2937">🔑 {pk_col}</text>
        """
            y_offset += column_spacing

        for fk_col in columns['fk']:
            table_boxes_svg += f"""
            <text x="{x + 10}" y="{y_offset}"
                  font-family="'Courier New', monospace" font-size="12"
                  fill="#4b5563">{fk_col}</text>
        """
            y_offset += column_spacing

        table_boxes_svg += "</g>"

    # Mark all table boxes as obstacles in the grid
    for box in table_dimensions.values():
        grid.mark_obstacle(box, margin=20)

    # Group relationships by table pair to handle multiple relationships between same tables
    from collections import defaultdict
    table_pair_rels = defaultdict(list)
    for rel in display_relationships:
        # Create consistent key for table pair (always sort to avoid duplicates)
        pair_key = tuple(sorted([rel['table1'], rel['table2']]))
        table_pair_rels[pair_key].append(rel)

    # Build relationship lines with crow's foot notation
    relationship_lines_svg = ""
    rel_idx = 0
    for pair_key, rels in table_pair_rels.items():
        table1, table2 = pair_key

        if table1 not in positions or table2 not in positions:
            continue

        # Get box dimensions for smart endpoint snapping
        box1_x, box1_y, box1_w, box1_h = table_dimensions[table1]
        box2_x, box2_y, box2_w, box2_h = table_dimensions[table2]

        # Calculate center points for initial direction
        center_x1 = box1_x + box1_w / 2
        center_y1 = box1_y + box1_h / 2
        center_x2 = box2_x + box2_w / 2
        center_y2 = box2_y + box2_h / 2

        # Draw each relationship with offset if multiple
        num_rels = len(rels)
        for i, rel in enumerate(rels):
            # Calculate target points (other box centers) with slight offset for multiple relations
            if num_rels > 1:
                # Offset perpendicular to connection line
                dx_temp = center_x2 - center_x1
                dy_temp = center_y2 - center_y1
                dist_temp = math.sqrt(dx_temp*dx_temp + dy_temp*dy_temp)
                if dist_temp > 0:
                    # Perpendicular offset
                    perp_x = -dy_temp / dist_temp
                    perp_y = dx_temp / dist_temp
                    offset_amount = (i - (num_rels - 1) / 2) * 15
                    target_x2 = center_x2 + perp_x * offset_amount
                    target_y2 = center_y2 + perp_y * offset_amount
                    target_x1 = center_x1 + perp_x * offset_amount
                    target_y1 = center_y1 + perp_y * offset_amount
                else:
                    target_x1, target_y1 = center_x1, center_y1
                    target_x2, target_y2 = center_x2, center_y2
            else:
                target_x1, target_y1 = center_x1, center_y1
                target_x2, target_y2 = center_x2, center_y2

            # Smart snap to closest box edges (now returns side info)
            start_x, start_y, start_side = _snap_to_box_edge(box1_x, box1_y, box1_w, box1_h, target_x2, target_y2)
            end_x, end_y, end_side = _snap_to_box_edge(box2_x, box2_y, box2_w, box2_h, target_x1, target_y1)

            # Line color based on confidence
            confidence = rel['confidence']
            if confidence >= 0.9:
                line_color = "#6606dc"
                line_width = 2.5
            elif confidence >= 0.7:
                line_color = "#9ca3af"
                line_width = 2
            else:
                line_color = "#d1d5db"
                line_width = 1.5

            # Determine cardinality labels (1 or n)
            if rel['relationship_type'] == "one-to-one":
                label_start = "1"
                label_end = "1"
            elif rel['relationship_type'] == "many-to-one":
                if rel.get('direction') == "table1_to_table2":
                    label_start = "n"
                    label_end = "1"
                elif rel.get('direction') == "table2_to_table1":
                    label_start = "1"
                    label_end = "n"
                else:
                    label_start = "1"
                    label_end = "1"
            elif rel['relationship_type'] == "many-to-many":
                label_start = "n"
                label_end = "n"
            else:
                label_start = "1"
                label_end = "1"

            # Build tooltip text
            tooltip_text = f"{rel['column1']} ↔ {rel['column2']}&#10;Confidence: {confidence:.1%}&#10;Type: {rel['relationship_type']}&#10;Overlap: {rel['overlap_ratio']:.1%}&#10;Matching: {rel['matching_values']:,}"

            # Calculate lane offset for multiple parallel relationships
            if num_rels > 1:
                lane_offset = (i - (num_rels - 1) / 2) * 12
            else:
                lane_offset = 0

            # Collect all box dimensions for collision detection
            all_box_dims = list(table_dimensions.values())

            # Create orthogonal path with collision avoidance
            path_d, label_positions = _create_orthogonal_path(
                start_x, start_y, start_side,
                end_x, end_y, end_side,
                lane_offset, all_box_dims,
                corner_radius=4,
                grid=grid,
                lane_registry=lane_registry
            )

            # Position cardinality labels based on edge sides (perpendicular offset from exit/entry)
            # Adjust offset to account for multiple parallel relationships
            base_offset = 22

            # Start label: offset perpendicular to the start edge
            if start_side == 'top' or start_side == 'bottom':
                label_start_x = start_x
                label_start_y = start_y + (base_offset if start_side == 'bottom' else -base_offset) + lane_offset
            else:  # left or right
                label_start_x = start_x + (base_offset if start_side == 'right' else -base_offset)
                label_start_y = start_y + lane_offset

            # End label: offset perpendicular to the end edge
            if end_side == 'top' or end_side == 'bottom':
                label_end_x = end_x
                label_end_y = end_y + (base_offset if end_side == 'bottom' else -base_offset) + lane_offset
            else:  # left or right
                label_end_x = end_x + (base_offset if end_side == 'right' else -base_offset)
                label_end_y = end_y + lane_offset

            # Position column name label on the longest straight segment
            # Add offset to prevent overlap with parallel relationship labels
            if label_positions:
                # Find the longest segment
                max_len = 0
                best_pos = label_positions[0]
                best_seg_start = None
                best_seg_end = None
                for j in range(len(label_positions) - 1):
                    seg_len = math.sqrt((label_positions[j+1][0] - label_positions[j][0])**2 +
                                       (label_positions[j+1][1] - label_positions[j][1])**2)
                    if seg_len > max_len:
                        max_len = seg_len
                        best_seg_start = label_positions[j]
                        best_seg_end = label_positions[j+1]
                        # Place label at midpoint of this segment
                        best_pos = ((label_positions[j][0] + label_positions[j+1][0]) / 2,
                                   (label_positions[j][1] + label_positions[j+1][1]) / 2)

                col_label_x, col_label_y = best_pos

                # Determine if segment is horizontal or vertical and offset accordingly
                if best_seg_start and best_seg_end:
                    dx = abs(best_seg_end[0] - best_seg_start[0])
                    dy = abs(best_seg_end[1] - best_seg_start[1])
                    # Horizontal segment: add vertical offset (base 8px + lane offset)
                    if dx > dy:
                        col_label_y += -8 + (lane_offset * 0.5)  # Offset above the line
                    # Vertical segment: add horizontal offset (base 8px + lane offset)
                    else:
                        col_label_x += 8 + (lane_offset * 0.5)  # Offset to the right of line
            else:
                # Fallback if no label positions
                col_label_x = (start_x + end_x) / 2
                col_label_y = (start_y + end_y) / 2

            relationship_lines_svg += f"""
        <g class="er-relationship" data-table1="{rel['table1']}" data-table2="{rel['table2']}" data-rel-id="rel-{rel_idx}">
            <title>{tooltip_text}</title>
            <path d="{path_d}" stroke="{line_color}" stroke-width="{line_width}"
                  fill="none"/>
            <!-- Cardinality labels with backgrounds -->
            <rect x="{label_start_x - 10}" y="{label_start_y - 10}" width="20" height="20"
                  fill="white" stroke="{line_color}" stroke-width="1" rx="3"/>
            <text x="{label_start_x}" y="{label_start_y}" font-family="Inter, sans-serif" font-size="13" font-weight="700"
                  fill="{line_color}" text-anchor="middle" dominant-baseline="middle">
                {label_start}
            </text>
            <rect x="{label_end_x - 10}" y="{label_end_y - 10}" width="20" height="20"
                  fill="white" stroke="{line_color}" stroke-width="1" rx="3"/>
            <text x="{label_end_x}" y="{label_end_y}" font-family="Inter, sans-serif" font-size="13" font-weight="700"
                  fill="{line_color}" text-anchor="middle" dominant-baseline="middle">
                {label_end}
            </text>
            <!-- Column name label with background (no rotation for orthogonal lines) -->
            <rect x="{col_label_x - len(rel['column1']) * 3.5 - 2}" y="{col_label_y - 10}"
                  width="{len(rel['column1']) * 7 + 4}" height="20"
                  fill="white" rx="3" stroke="{line_color}" stroke-width="0.5"/>
            <text x="{col_label_x}" y="{col_label_y}" font-family="Inter, sans-serif" font-size="10"
                  fill="{line_color}" text-anchor="middle" dominant-baseline="middle" font-weight="500">
                {rel['column1']}
            </text>
        </g>
        """
            rel_idx += 1

    html = f"""
    <section class="relationships-section">
        <h2 class="relationships-title">
            Table Relationships
        </h2>
        <p class="relationships-description">
            Automatically detected relationships between tables based on column names, data types, and value overlaps.
        </p>

        <!-- ER Diagram with Crow's Foot Notation -->
        <div class="alert-info mb-20">
            <strong>Interactive ER Diagram:</strong> Click on a table to highlight its relationships. Hover over connection lines to see details.
        </div>

        <div class="er-diagram-container">
            <svg id="er-diagram" width="{max_x}" height="{max_y}" xmlns="http://www.w3.org/2000/svg">
                <!-- Relationship lines (drawn first, behind tables) -->
                {relationship_lines_svg}

                <!-- Table boxes -->
                {table_boxes_svg}
            </svg>
        </div>

        <style>
            .er-diagram-container {{
                background: #f9fafb;
                border: 1px solid #e5e7eb;
                border-radius: 8px;
                padding: 20px;
                margin: 20px 0;
                overflow-x: auto;
            }}
            .er-table {{
                cursor: pointer;
                transition: all 0.2s;
            }}
            .er-table:hover rect {{
                filter: brightness(0.95);
            }}
            .er-table.highlighted rect:first-child {{
                fill: #5005b8;
                stroke: #4004a0;
                stroke-width: 3;
            }}
            .er-relationship {{
                transition: all 0.2s;
            }}
            .er-relationship.highlighted > path {{
                stroke: #6606dc !important;
                stroke-width: 3.5 !important;
            }}
            .er-relationship.highlighted text {{
                fill: #6606dc !important;
                font-weight: 700 !important;
            }}
            .er-relationship.dimmed {{
                opacity: 0.2;
            }}
            .er-table.dimmed {{
                opacity: 0.3;
            }}
        </style>

        <script>
            (function() {{
                const tables = document.querySelectorAll('.er-table');
                const relationships = document.querySelectorAll('.er-relationship');
                let selectedTable = null;

                tables.forEach(table => {{
                    table.addEventListener('click', function() {{
                        const tableName = this.getAttribute('data-table');

                        // Toggle selection
                        if (selectedTable === tableName) {{
                            // Deselect
                            selectedTable = null;
                            tables.forEach(t => t.classList.remove('highlighted', 'dimmed'));
                            relationships.forEach(r => r.classList.remove('highlighted', 'dimmed'));
                        }} else {{
                            // Select
                            selectedTable = tableName;

                            // Dim all tables and relationships
                            tables.forEach(t => t.classList.add('dimmed'));
                            relationships.forEach(r => r.classList.add('dimmed'));

                            // Highlight selected table
                            this.classList.remove('dimmed');
                            this.classList.add('highlighted');

                            // Highlight related tables and relationships
                            relationships.forEach(rel => {{
                                const table1 = rel.getAttribute('data-table1');
                                const table2 = rel.getAttribute('data-table2');

                                if (table1 === tableName || table2 === tableName) {{
                                    rel.classList.remove('dimmed');
                                    rel.classList.add('highlighted');

                                    // Highlight the connected table
                                    const connectedTable = table1 === tableName ? table2 : table1;
                                    tables.forEach(t => {{
                                        if (t.getAttribute('data-table') === connectedTable) {{
                                            t.classList.remove('dimmed');
                                            t.classList.add('highlighted');
                                        }}
                                    }});
                                }}
                            }});
                        }}
                    }});
                }});
            }})();
        </script>

        <!-- Detailed Table View -->
        <h3 class="relationships-table-title">
            Relationship Details
        </h3>

        <table class="relationship-table">
            <thead>
                <tr>
                    <th>SOURCE</th>
                    <th class="center col-arrow"></th>
                    <th>TARGET</th>
                    <th class="center col-type">TYPE</th>
                    <th class="center col-matching">MATCHING</th>
                    <th class="center col-overlap">OVERLAP</th>
                    <th class="col-confidence">CONFIDENCE</th>
                </tr>
            </thead>
            <tbody>
    """

    for rel in display_relationships:
        confidence_pct = rel['confidence'] * 100
        overlap_pct = rel['overlap_ratio'] * 100

        # Confidence color based on threshold
        if confidence_pct >= 90:
            confidence_color = "#10b981"  # Green
        elif confidence_pct >= 70:
            confidence_color = "#6606dc"  # Purple
        else:
            confidence_color = "#f59e0b"  # Orange

        # Overlap color based on value
        if overlap_pct >= 90:
            overlap_color = "#10b981"  # Green
        elif overlap_pct >= 70:
            overlap_color = "#6606dc"  # Purple
        else:
            overlap_color = "#f59e0b"  # Orange

        # Determine arrow direction
        if rel.get('direction') == 'table1_to_table2':
            arrow = "→"
        elif rel.get('direction') == 'table2_to_table1':
            arrow = "←"
        else:
            arrow = "↔"

        # Relationship type badge styling
        type_styles = {
            "one-to-one": "background: #dbeafe; color: #1e40af;",
            "many-to-one": "background: #e0e7ff; color: #4338ca;",
            "many-to-many": "background: #fce7f3; color: #9f1239;"
        }
        type_style = type_styles.get(rel['relationship_type'], "background: #f3f4f6; color: #4b5563;")

        html += f"""
                <tr>
                    <td>
                        <div class="relationship-cell-table">{rel['table1']}</div>
                        <div class="relationship-cell-column">{rel['column1']}</div>
                    </td>
                    <td class="center arrow">
                        {arrow}
                    </td>
                    <td>
                        <div class="relationship-cell-table">{rel['table2']}</div>
                        <div class="relationship-cell-column">{rel['column2']}</div>
                    </td>
                    <td class="center">
                        <span class="relationship-type-badge" style="{type_style}">
                            {rel['relationship_type']}
                        </span>
                    </td>
                    <td class="center relationship-matching">
                        {rel['matching_values']:,}
                    </td>
                    <td class="center">
                        <span class="confidence-pct" style="color: {overlap_color};">{overlap_pct:.0f}%</span>
                    </td>
                    <td>
                        <div class="confidence-bar-container">
                            <div class="confidence-bar-bg">
                                <div class="confidence-bar-fill" style="width: {confidence_pct:.1f}%; background: {confidence_color};"></div>
                            </div>
                            <span class="confidence-pct" style="color: {confidence_color};">{confidence_pct:.0f}%</span>
                        </div>
                    </td>
                </tr>
        """

    html += """
            </tbody>
        </table>
    </section>
    """

    return html


def generate_standalone_relationships_report(
    relationships: List[Dict],
    tables_metadata: Dict[str, Dict],
    output_path: str,
    min_confidence_display: float = 0.5
) -> None:
    """
    Generate full interactive ER diagram report

    Args:
        relationships: List of relationship dictionaries
        tables_metadata: Metadata about tables (row counts, column counts, primary keys)
        output_path: Path to save HTML file
        min_confidence_display: Minimum confidence to display
    """
    # Filter relationships
    display_relationships = [r for r in relationships if r['confidence'] >= min_confidence_display]
    display_relationships.sort(key=lambda x: x['confidence'], reverse=True)

    # Get tables involved in relationships
    tables_in_relationships = set()
    for rel in display_relationships:
        tables_in_relationships.add(rel['table1'])
        tables_in_relationships.add(rel['table2'])

    tables_list = sorted(tables_in_relationships)

    # Calculate table positions
    positions = _calculate_table_positions(tables_list, num_cols=4)

    # Calculate SVG canvas size
    max_x = max(pos[0] for pos in positions.values()) + 300
    max_y = max(pos[1] for pos in positions.values()) + 250

    # Initialize global routing infrastructure for multi-pass routing
    grid = Grid(max_x, max_y, resolution=30)
    lane_registry = LaneRegistry()

    # Build table boxes SVG
    table_boxes_svg = ""
    table_dimensions = {}  # Track for obstacle marking
    for table_name in tables_list:
        x, y = positions[table_name]
        columns = _get_table_columns_for_diagram(table_name, display_relationships, tables_metadata)

        # Get metadata
        metadata = tables_metadata.get(table_name, {})
        row_count = metadata.get('total_rows', 'N/A')
        if isinstance(row_count, int):
            row_count_str = f"{row_count:,}"
        else:
            row_count_str = str(row_count)

        # Calculate box height based on number of columns
        total_columns = len(columns['pk']) + len(columns['fk'])
        box_height = 60 + (total_columns * 22)
        box_width = 280

        # Track dimensions for routing
        table_dimensions[table_name] = (x, y, box_width, box_height)

        # Create table box
        table_boxes_svg += f"""
        <g class="er-table" data-table="{table_name}">
            <rect x="{x}" y="{y}" width="{box_width}" height="{box_height}"
                  fill="white" stroke="#d1d5db" stroke-width="1.5" rx="6"/>
            <rect x="{x}" y="{y}" width="{box_width}" height="{header_height}"
                  fill="#6606dc" rx="6"/>
            <rect x="{x}" y="{y + header_height}" width="{box_width}" height="{box_height - header_height}"
                  fill="white" rx="0"/>
            <text x="{x + box_width/2}" y="{y + header_height/2 + 2}"
                  font-family="Inter, sans-serif" font-size="14" font-weight="600"
                  fill="white" text-anchor="middle">{table_name}</text>
            <text x="{x + box_width/2}" y="{y + header_height - 5}"
                  font-family="Inter, sans-serif" font-size="10"
                  fill="rgba(255,255,255,0.8)" text-anchor="middle">{row_count_str} rows</text>
        """

        # Add columns
        y_offset = y + header_height + 18
        for pk_col in columns['pk']:
            table_boxes_svg += f"""
            <text x="{x + 10}" y="{y_offset}"
                  font-family="'Courier New', monospace" font-size="12" font-weight="bold"
                  fill="#1f2937">🔑 {pk_col}</text>
        """
            y_offset += column_spacing

        for fk_col in columns['fk']:
            table_boxes_svg += f"""
            <text x="{x + 10}" y="{y_offset}"
                  font-family="'Courier New', monospace" font-size="12"
                  fill="#4b5563">{fk_col}</text>
        """
            y_offset += column_spacing

        table_boxes_svg += "</g>"

    # Mark all table boxes as obstacles in the grid
    for box in table_dimensions.values():
        grid.mark_obstacle(box, margin=20)

    # Group relationships by table pair (same logic as summary section)
    from collections import defaultdict
    table_pair_rels = defaultdict(list)
    for rel in display_relationships:
        pair_key = tuple(sorted([rel['table1'], rel['table2']]))
        table_pair_rels[pair_key].append(rel)

    # Build relationship lines with crow's foot notation
    relationship_lines_svg = ""
    rel_idx = 0
    for pair_key, rels in table_pair_rels.items():
        table1, table2 = pair_key

        if table1 not in positions or table2 not in positions:
            continue

        x1, y1 = positions[table1]
        x2, y2 = positions[table2]

        # Calculate center points of table boxes
        center_x1 = x1 + 140
        center_y1 = y1 + 100
        center_x2 = x2 + 140
        center_y2 = y2 + 100

        # Calculate edge connection points
        dx = center_x2 - center_x1
        dy = center_y2 - center_y1

        # Determine base connection points
        if abs(dx) > abs(dy):
            if dx > 0:
                base_start_x, base_start_y = x1 + 280, center_y1
                base_end_x, base_end_y = x2, center_y2
            else:
                base_start_x, base_start_y = x1, center_y1
                base_end_x, base_end_y = x2 + 280, center_y2
            offset_axis = 'vertical'
        else:
            if dy > 0:
                base_start_x, base_start_y = center_x1, y1 + 150
                base_end_x, base_end_y = center_x2, y2
            else:
                base_start_x, base_start_y = center_x1, y1
                base_end_x, base_end_y = center_x2, y2 + 150
            offset_axis = 'horizontal'

        # Draw each relationship with offset if multiple
        num_rels = len(rels)
        for i, rel in enumerate(rels):
            # Calculate offset for multiple relationships
            if num_rels > 1:
                offset = (i - (num_rels - 1) / 2) * 25
            else:
                offset = 0

            # Apply offset
            if offset_axis == 'vertical':
                start_x, start_y = base_start_x, base_start_y + offset
                end_x, end_y = base_end_x, base_end_y + offset
            else:
                start_x, start_y = base_start_x + offset, base_start_y
                end_x, end_y = base_end_x + offset, base_end_y

            # Line color based on confidence
            confidence = rel['confidence']
            if confidence >= 0.9:
                line_color = "#6606dc"
                line_width = 2.5
            elif confidence >= 0.7:
                line_color = "#9ca3af"
                line_width = 2
            else:
                line_color = "#d1d5db"
                line_width = 1.5

            # Determine cardinality labels (1 or n)
            if rel['relationship_type'] == "one-to-one":
                label_start = "1"
                label_end = "1"
            elif rel['relationship_type'] == "many-to-one":
                if rel.get('direction') == "table1_to_table2":
                    label_start = "n"
                    label_end = "1"
                elif rel.get('direction') == "table2_to_table1":
                    label_start = "1"
                    label_end = "n"
                else:
                    label_start = "1"
                    label_end = "1"
            elif rel['relationship_type'] == "many-to-many":
                label_start = "n"
                label_end = "n"
            else:
                label_start = "1"
                label_end = "1"

            # Build tooltip text
            tooltip_text = f"{rel['column1']} ↔ {rel['column2']}&#10;Confidence: {confidence:.1%}&#10;Type: {rel['relationship_type']}&#10;Overlap: {rel['overlap_ratio']:.1%}&#10;Matching: {rel['matching_values']:,}"

            # Calculate lane offset for multiple parallel relationships
            if num_rels > 1:
                lane_offset = (i - (num_rels - 1) / 2) * 12
            else:
                lane_offset = 0

            # Collect all box dimensions for collision detection
            all_box_dims = list(table_dimensions.values())

            # Create orthogonal path with collision avoidance
            path_d, label_positions = _create_orthogonal_path(
                start_x, start_y, start_side,
                end_x, end_y, end_side,
                lane_offset, all_box_dims,
                corner_radius=4,
                grid=grid,
                lane_registry=lane_registry
            )

            # Position cardinality labels based on edge sides (perpendicular offset from exit/entry)
            # Adjust offset to account for multiple parallel relationships
            base_offset = 22

            # Start label: offset perpendicular to the start edge
            if start_side == 'top' or start_side == 'bottom':
                label_start_x = start_x
                label_start_y = start_y + (base_offset if start_side == 'bottom' else -base_offset) + lane_offset
            else:  # left or right
                label_start_x = start_x + (base_offset if start_side == 'right' else -base_offset)
                label_start_y = start_y + lane_offset

            # End label: offset perpendicular to the end edge
            if end_side == 'top' or end_side == 'bottom':
                label_end_x = end_x
                label_end_y = end_y + (base_offset if end_side == 'bottom' else -base_offset) + lane_offset
            else:  # left or right
                label_end_x = end_x + (base_offset if end_side == 'right' else -base_offset)
                label_end_y = end_y + lane_offset

            # Position column name label on the longest straight segment
            # Add offset to prevent overlap with parallel relationship labels
            if label_positions:
                # Find the longest segment
                max_len = 0
                best_pos = label_positions[0]
                best_seg_start = None
                best_seg_end = None
                for j in range(len(label_positions) - 1):
                    seg_len = math.sqrt((label_positions[j+1][0] - label_positions[j][0])**2 +
                                       (label_positions[j+1][1] - label_positions[j][1])**2)
                    if seg_len > max_len:
                        max_len = seg_len
                        best_seg_start = label_positions[j]
                        best_seg_end = label_positions[j+1]
                        # Place label at midpoint of this segment
                        best_pos = ((label_positions[j][0] + label_positions[j+1][0]) / 2,
                                   (label_positions[j][1] + label_positions[j+1][1]) / 2)

                col_label_x, col_label_y = best_pos

                # Determine if segment is horizontal or vertical and offset accordingly
                if best_seg_start and best_seg_end:
                    dx = abs(best_seg_end[0] - best_seg_start[0])
                    dy = abs(best_seg_end[1] - best_seg_start[1])
                    # Horizontal segment: add vertical offset (base 8px + lane offset)
                    if dx > dy:
                        col_label_y += -8 + (lane_offset * 0.5)  # Offset above the line
                    # Vertical segment: add horizontal offset (base 8px + lane offset)
                    else:
                        col_label_x += 8 + (lane_offset * 0.5)  # Offset to the right of line
            else:
                # Fallback if no label positions
                col_label_x = (start_x + end_x) / 2
                col_label_y = (start_y + end_y) / 2

            relationship_lines_svg += f"""
        <g class="er-relationship" data-table1="{rel['table1']}" data-table2="{rel['table2']}" data-rel-id="rel-{rel_idx}">
            <title>{tooltip_text}</title>
            <path d="{path_d}" stroke="{line_color}" stroke-width="{line_width}"
                  fill="none"/>
            <!-- Cardinality labels with backgrounds -->
            <rect x="{label_start_x - 10}" y="{label_start_y - 10}" width="20" height="20"
                  fill="white" stroke="{line_color}" stroke-width="1" rx="3"/>
            <text x="{label_start_x}" y="{label_start_y}" font-family="Inter, sans-serif" font-size="13" font-weight="700"
                  fill="{line_color}" text-anchor="middle" dominant-baseline="middle">
                {label_start}
            </text>
            <rect x="{label_end_x - 10}" y="{label_end_y - 10}" width="20" height="20"
                  fill="white" stroke="{line_color}" stroke-width="1" rx="3"/>
            <text x="{label_end_x}" y="{label_end_y}" font-family="Inter, sans-serif" font-size="13" font-weight="700"
                  fill="{line_color}" text-anchor="middle" dominant-baseline="middle">
                {label_end}
            </text>
            <!-- Column name label with background (no rotation for orthogonal lines) -->
            <rect x="{col_label_x - len(rel['column1']) * 3.5 - 2}" y="{col_label_y - 10}"
                  width="{len(rel['column1']) * 7 + 4}" height="20"
                  fill="white" rx="3" stroke="{line_color}" stroke-width="0.5"/>
            <text x="{col_label_x}" y="{col_label_y}" font-family="Inter, sans-serif" font-size="10"
                  fill="{line_color}" text-anchor="middle" dominant-baseline="middle" font-weight="500">
                {rel['column1']}
            </text>
        </g>
        """
            rel_idx += 1

    # Generate relationships HTML list
    relationships_html = ""
    for rel in display_relationships:
        confidence_pct = rel['confidence'] * 100

        if confidence_pct >= 80:
            confidence_class = 'confidence-high'
        elif confidence_pct >= 50:
            confidence_class = 'confidence-medium'
        else:
            confidence_class = 'confidence-low'

        # Determine arrow direction for display
        if rel.get('direction') == 'table1_to_table2':
            arrow = "→"
        elif rel.get('direction') == 'table2_to_table1':
            arrow = "←"
        else:
            arrow = "↔"

        relationships_html += f'''
        <div class="relationship-item {confidence_class}">
            <strong>{rel['table1']}.{rel['column1']}</strong> {arrow}
            <strong>{rel['table2']}.{rel['column2']}</strong><br>
            <small>
                Confidence: {confidence_pct:.1f}% |
                Type: {rel['relationship_type']} |
                Matching values: {rel['matching_values']:,} |
                Overlap: {rel['overlap_ratio']:.1%}
            </small>
        </div>
        '''

    # Generate HTML template
    html_template = f'''<!DOCTYPE html>
<html>
<head>
    <title>Table Relationships - ER Diagram</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet">
    <style>
        body {{
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
            margin: 0;
            padding: 0;
            background-color: #f9fafb;
            color: #1f2937;
        }}
        .header {{
            background: linear-gradient(135deg, #6606dc 0%, #8b5cf6 100%);
            color: white;
            padding: 40px;
        }}
        .header h1 {{
            margin: 0 0 10px 0;
            font-size: 32px;
            font-weight: 700;
        }}
        .header p {{
            margin: 0;
            font-size: 16px;
            opacity: 0.9;
        }}
        .container {{
            max-width: 1600px;
            margin: 0 auto;
            padding: 40px 20px;
        }}
        .er-diagram-container {{
            background: white;
            border: 1px solid #e5e7eb;
            border-radius: 8px;
            padding: 30px;
            margin: 20px 0;
            overflow-x: auto;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        }}
        .er-table {{
            cursor: pointer;
            transition: all 0.2s;
        }}
        .er-table:hover rect {{
            filter: brightness(0.95);
        }}
        .er-table.highlighted rect:first-child {{
            fill: #5005b8;
            stroke: #4004a0;
            stroke-width: 3;
        }}
        .er-relationship {{
            transition: all 0.2s;
        }}
        .er-relationship.highlighted > path {{
            stroke: #6606dc !important;
            stroke-width: 3.5 !important;
        }}
        .er-relationship.highlighted text {{
            fill: #6606dc !important;
            font-weight: 700 !important;
        }}
        .er-relationship.dimmed {{
            opacity: 0.2;
        }}
        .er-table.dimmed {{
            opacity: 0.3;
        }}
        .info-panel {{
            background-color: white;
            border: 1px solid #e5e7eb;
            padding: 30px;
            margin-top: 30px;
            border-radius: 8px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        }}
        .info-panel h2 {{
            margin: 0 0 20px 0;
            font-size: 24px;
            font-weight: 600;
            color: #1f2937;
        }}
        .relationship-item {{
            padding: 15px;
            margin: 10px 0;
            background-color: #f9fafb;
            border-left: 4px solid #10b981;
            border-radius: 4px;
            transition: all 0.2s;
        }}
        .relationship-item:hover {{
            background-color: #f3f4f6;
            transform: translateX(4px);
        }}
        .confidence-high {{ border-left-color: #10b981; }}
        .confidence-medium {{ border-left-color: #f59e0b; }}
        .confidence-low {{ border-left-color: #ef4444; }}
        .stats {{
            display: flex;
            gap: 30px;
            margin-bottom: 30px;
            padding: 20px;
            background: #f9fafb;
            border-radius: 8px;
        }}
        .stat {{
            flex: 1;
        }}
        .stat-value {{
            font-size: 32px;
            font-weight: 700;
            color: #6606dc;
        }}
        .stat-label {{
            font-size: 14px;
            color: #6b7280;
            margin-top: 4px;
        }}
        .tip-box {{
            background: #f0f9ff;
            border: 1px solid #bae6fd;
            padding: 12px 16px;
            border-radius: 6px;
            margin-bottom: 20px;
            color: #0c4a6e;
            font-size: 14px;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>Table Relationships - ER Diagram</h1>
        <p>Entity-Relationship diagram with crow's foot notation showing detected relationships</p>
    </div>

    <div class="container">
        <div class="stats">
            <div class="stat">
                <div class="stat-value">{len(tables_in_relationships)}</div>
                <div class="stat-label">Tables</div>
            </div>
            <div class="stat">
                <div class="stat-value">{len(display_relationships)}</div>
                <div class="stat-label">Relationships</div>
            </div>
            <div class="stat">
                <div class="stat-value">{sum(1 for r in display_relationships if r['confidence'] >= 0.8)}</div>
                <div class="stat-label">High Confidence (&ge;80%)</div>
            </div>
        </div>

        <div class="tip-box">
            <strong>Interactive ER Diagram:</strong> Click on a table to highlight its relationships. Hover over connection lines to see details.
        </div>

        <div class="er-diagram-container">
            <svg id="er-diagram" width="{max_x}" height="{max_y}" xmlns="http://www.w3.org/2000/svg">
                <!-- Relationship lines (drawn first, behind tables) -->
                {relationship_lines_svg}

                <!-- Table boxes -->
                {table_boxes_svg}
            </svg>
        </div>

        <div class="info-panel">
            <h2>Detected Relationships</h2>
            <div id="relationships-list">
                {relationships_html}
            </div>
        </div>
    </div>

    <script>
        (function() {{
            const tables = document.querySelectorAll('.er-table');
            const relationships = document.querySelectorAll('.er-relationship');
            let selectedTable = null;

            tables.forEach(table => {{
                table.addEventListener('click', function() {{
                    const tableName = this.getAttribute('data-table');

                    // Toggle selection
                    if (selectedTable === tableName) {{
                        // Deselect
                        selectedTable = null;
                        tables.forEach(t => t.classList.remove('highlighted', 'dimmed'));
                        relationships.forEach(r => r.classList.remove('highlighted', 'dimmed'));
                    }} else {{
                        // Select
                        selectedTable = tableName;

                        // Dim all tables and relationships
                        tables.forEach(t => t.classList.add('dimmed'));
                        relationships.forEach(r => r.classList.add('dimmed'));

                        // Highlight selected table
                        this.classList.remove('dimmed');
                        this.classList.add('highlighted');

                        // Highlight related tables and relationships
                        relationships.forEach(rel => {{
                            const table1 = rel.getAttribute('data-table1');
                            const table2 = rel.getAttribute('data-table2');

                            if (table1 === tableName || table2 === tableName) {{
                                rel.classList.remove('dimmed');
                                rel.classList.add('highlighted');

                                // Highlight the connected table
                                const connectedTable = table1 === tableName ? table2 : table1;
                                tables.forEach(t => {{
                                    if (t.getAttribute('data-table') === connectedTable) {{
                                        t.classList.remove('dimmed');
                                        t.classList.add('highlighted');
                                    }}
                                }});
                            }}
                        }});
                    }}
                }});
            }});
        }})();
    </script>
</body>
</html>'''

    # Write to file
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html_template)
