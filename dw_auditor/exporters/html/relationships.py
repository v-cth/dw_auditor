"""
HTML generation for table relationship visualizations
"""

from typing import List, Dict, Optional, Tuple, Set
import json
import math


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
                            corner_radius: int = 4) -> Tuple[str, List[Tuple[float, float]]]:
    """
    Create an orthogonal path with collision avoidance and corner rounding

    Args:
        start_x, start_y: Starting point on box edge
        start_side: Side of starting box ('top', 'right', 'bottom', 'left')
        end_x, end_y: Ending point on box edge
        end_side: Side of ending box
        lane_offset: Offset for parallel lines (lane system)
        all_boxes: List of (x, y, w, h) for all table boxes for collision detection
        corner_radius: Radius for rounded corners (3-6px)

    Returns:
        Tuple of (SVG path string, list of label positions on straight segments)
    """
    # Base clearance from edges - increased to avoid table collisions
    base_clearance = 100
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

    # Snap to grid for clean routing
    exit_x = _snap_to_grid(exit_x)
    exit_y = _snap_to_grid(exit_y)
    entry_x = _snap_to_grid(entry_x)
    entry_y = _snap_to_grid(entry_y)

    # Build waypoints for the path
    waypoints = [(start_x, start_y)]

    # First segment: exit perpendicular from start box
    waypoints.append((exit_x, exit_y))

    # Middle routing: choose strategy based on edge orientations
    if start_side in ['left', 'right'] and end_side in ['left', 'right']:
        # Both horizontal edges: route with vertical segment in middle
        mid_x = (exit_x + entry_x) / 2
        mid_x = _snap_to_grid(mid_x)
        # Apply lane offset to vertical rail
        exit_y_offset = exit_y + lane_offset
        entry_y_offset = entry_y + lane_offset

        # Check for collision and adjust if needed
        if _boxes_overlap_segment(all_boxes, exit_x, exit_y_offset, mid_x, exit_y_offset, margin=20):
            # Route further out
            exit_y_offset = exit_y + lane_offset + (120 if lane_offset >= 0 else -120)
            entry_y_offset = entry_y + lane_offset + (120 if lane_offset >= 0 else -120)

        waypoints.append((exit_x, exit_y_offset))
        waypoints.append((mid_x, exit_y_offset))
        waypoints.append((mid_x, entry_y_offset))
        waypoints.append((entry_x, entry_y_offset))
    elif start_side in ['top', 'bottom'] and end_side in ['top', 'bottom']:
        # Both vertical edges: route with horizontal segment in middle
        mid_y = (exit_y + entry_y) / 2
        mid_y = _snap_to_grid(mid_y)
        # Apply lane offset to horizontal rail
        exit_x_offset = exit_x + lane_offset
        entry_x_offset = entry_x + lane_offset

        # Check for collision and adjust if needed
        if _boxes_overlap_segment(all_boxes, exit_x_offset, exit_y, exit_x_offset, mid_y, margin=20):
            # Route further out
            exit_x_offset = exit_x + lane_offset + (120 if lane_offset >= 0 else -120)
            entry_x_offset = entry_x + lane_offset + (120 if lane_offset >= 0 else -120)

        waypoints.append((exit_x_offset, exit_y))
        waypoints.append((exit_x_offset, mid_y))
        waypoints.append((entry_x_offset, mid_y))
        waypoints.append((entry_x_offset, entry_y))
    else:
        # Mixed: horizontal and vertical edges
        if start_side in ['left', 'right']:
            # Start horizontal, apply lane offset to vertical segment
            vert_pos = exit_y + lane_offset

            # Check if the vertical segment would collide
            if _boxes_overlap_segment(all_boxes, exit_x, vert_pos, entry_x, vert_pos, margin=20):
                # Route further out vertically
                vert_pos = exit_y + lane_offset + (120 if exit_y < entry_y else -120)

            waypoints.append((entry_x, vert_pos))
        else:
            # Start vertical, apply lane offset to horizontal segment
            horiz_pos = exit_x + lane_offset

            # Check if the horizontal segment would collide
            if _boxes_overlap_segment(all_boxes, horiz_pos, exit_y, horiz_pos, entry_y, margin=20):
                # Route further out horizontally
                horiz_pos = exit_x + lane_offset + (120 if exit_x < entry_x else -120)

            waypoints.append((horiz_pos, entry_y))

    # Last segment: enter perpendicular to target box
    waypoints.append((entry_x, entry_y))
    waypoints.append((end_x, end_y))

    # Remove consecutive duplicate waypoints
    cleaned_waypoints = [waypoints[0]]
    for i in range(1, len(waypoints)):
        if waypoints[i] != waypoints[i-1]:
            cleaned_waypoints.append(waypoints[i])
    waypoints = cleaned_waypoints

    # Build SVG path with rounded corners
    path = f"M {waypoints[0][0]},{waypoints[0][1]}"

    for i in range(1, len(waypoints)):
        curr = waypoints[i]
        if i < len(waypoints) - 1 and corner_radius > 0:
            # Add rounded corner
            prev = waypoints[i - 1]
            next_pt = waypoints[i + 1]

            # Calculate vectors
            dx_in = curr[0] - prev[0]
            dy_in = curr[1] - prev[1]
            dx_out = next_pt[0] - curr[0]
            dy_out = next_pt[1] - curr[1]

            # Normalize and shorten for corner
            len_in = math.sqrt(dx_in*dx_in + dy_in*dy_in)
            len_out = math.sqrt(dx_out*dx_out + dy_out*dy_out)

            if len_in > corner_radius * 2 and len_out > corner_radius * 2:
                # Calculate corner points
                corner_start_x = curr[0] - (dx_in / len_in) * corner_radius
                corner_start_y = curr[1] - (dy_in / len_in) * corner_radius
                corner_end_x = curr[0] + (dx_out / len_out) * corner_radius
                corner_end_y = curr[1] + (dy_out / len_out) * corner_radius

                # Line to corner start, arc to corner end
                path += f" L {corner_start_x},{corner_start_y}"
                path += f" Q {curr[0]},{curr[1]} {corner_end_x},{corner_end_y}"
            else:
                path += f" L {curr[0]},{curr[1]}"
        else:
            path += f" L {curr[0]},{curr[1]}"

    # Calculate good label positions (middle of longest straight segments)
    label_positions = []
    for i in range(1, len(waypoints) - 1):
        prev = waypoints[i - 1]
        curr = waypoints[i]
        seg_len = math.sqrt((curr[0] - prev[0])**2 + (curr[1] - prev[1])**2)
        if seg_len > 50:  # Only on segments long enough for labels
            mid_x = (prev[0] + curr[0]) / 2
            mid_y = (prev[1] + curr[1]) / 2
            label_positions.append((mid_x, mid_y))

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
                lane_offset = (i - (num_rels - 1) / 2) * 30
            else:
                lane_offset = 0

            # Collect all box dimensions for collision detection
            all_box_dims = list(table_dimensions.values())

            # Create orthogonal path with collision avoidance
            path_d, label_positions = _create_orthogonal_path(
                start_x, start_y, start_side,
                end_x, end_y, end_side,
                lane_offset, all_box_dims,
                corner_radius=4
            )

            # Position cardinality labels based on edge sides (perpendicular offset from exit/entry)
            # Start label: offset perpendicular to the start edge
            if start_side == 'top' or start_side == 'bottom':
                label_start_x = start_x
                label_start_y = start_y + (22 if start_side == 'bottom' else -22)
            else:  # left or right
                label_start_x = start_x + (22 if start_side == 'right' else -22)
                label_start_y = start_y

            # End label: offset perpendicular to the end edge
            if end_side == 'top' or end_side == 'bottom':
                label_end_x = end_x
                label_end_y = end_y + (22 if end_side == 'bottom' else -22)
            else:  # left or right
                label_end_x = end_x + (22 if end_side == 'right' else -22)
                label_end_y = end_y

            # Position column name label on the longest straight segment
            if label_positions:
                # Find the longest segment
                max_len = 0
                best_pos = label_positions[0]
                for j in range(len(label_positions) - 1):
                    seg_len = math.sqrt((label_positions[j+1][0] - label_positions[j][0])**2 +
                                       (label_positions[j+1][1] - label_positions[j][1])**2)
                    if seg_len > max_len:
                        max_len = seg_len
                        # Place label at midpoint of this segment
                        best_pos = ((label_positions[j][0] + label_positions[j+1][0]) / 2,
                                   (label_positions[j][1] + label_positions[j+1][1]) / 2)
                col_label_x, col_label_y = best_pos
            else:
                # Fallback if no label positions
                col_label_x = (start_x + end_x) / 2
                col_label_y = (start_y + end_y) / 2

            relationship_lines_svg += f"""
        <g class="er-relationship" data-table1="{rel['table1']}" data-table2="{rel['table2']}" data-rel-id="rel-{rel_idx}">
            <title>{tooltip_text}</title>
            <path d="{path_d}" stroke="{line_color}" stroke-width="{line_width}"
                  fill="none" stroke-dasharray="{5 if i > 0 else 0}"/>
            <!-- Cardinality labels with backgrounds -->
            <rect x="{label_start_x - 9}" y="{label_start_y - 9}" width="18" height="18"
                  fill="white" stroke="{line_color}" stroke-width="0.5" rx="3" opacity="0.95"/>
            <text x="{label_start_x}" y="{label_start_y}" font-family="Inter, sans-serif" font-size="13" font-weight="700"
                  fill="{line_color}" text-anchor="middle" dominant-baseline="middle">
                {label_start}
            </text>
            <rect x="{label_end_x - 9}" y="{label_end_y - 9}" width="18" height="18"
                  fill="white" stroke="{line_color}" stroke-width="0.5" rx="3" opacity="0.95"/>
            <text x="{label_end_x}" y="{label_end_y}" font-family="Inter, sans-serif" font-size="13" font-weight="700"
                  fill="{line_color}" text-anchor="middle" dominant-baseline="middle">
                {label_end}
            </text>
            <!-- Column name label with background (no rotation for orthogonal lines) -->
            <rect x="{col_label_x - len(rel['column1']) * 3.5}" y="{col_label_y - 8}"
                  width="{len(rel['column1']) * 7}" height="16"
                  fill="white" rx="3" opacity="0.9"/>
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

    # Build table boxes SVG
    table_boxes_svg = ""
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

    # Group relationships by table pair (same logic as summary section)
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
                lane_offset = (i - (num_rels - 1) / 2) * 30
            else:
                lane_offset = 0

            # Collect all box dimensions for collision detection
            all_box_dims = list(table_dimensions.values())

            # Create orthogonal path with collision avoidance
            path_d, label_positions = _create_orthogonal_path(
                start_x, start_y, start_side,
                end_x, end_y, end_side,
                lane_offset, all_box_dims,
                corner_radius=4
            )

            # Position cardinality labels based on edge sides (perpendicular offset from exit/entry)
            # Start label: offset perpendicular to the start edge
            if start_side == 'top' or start_side == 'bottom':
                label_start_x = start_x
                label_start_y = start_y + (22 if start_side == 'bottom' else -22)
            else:  # left or right
                label_start_x = start_x + (22 if start_side == 'right' else -22)
                label_start_y = start_y

            # End label: offset perpendicular to the end edge
            if end_side == 'top' or end_side == 'bottom':
                label_end_x = end_x
                label_end_y = end_y + (22 if end_side == 'bottom' else -22)
            else:  # left or right
                label_end_x = end_x + (22 if end_side == 'right' else -22)
                label_end_y = end_y

            # Position column name label on the longest straight segment
            if label_positions:
                # Find the longest segment
                max_len = 0
                best_pos = label_positions[0]
                for j in range(len(label_positions) - 1):
                    seg_len = math.sqrt((label_positions[j+1][0] - label_positions[j][0])**2 +
                                       (label_positions[j+1][1] - label_positions[j][1])**2)
                    if seg_len > max_len:
                        max_len = seg_len
                        # Place label at midpoint of this segment
                        best_pos = ((label_positions[j][0] + label_positions[j+1][0]) / 2,
                                   (label_positions[j][1] + label_positions[j+1][1]) / 2)
                col_label_x, col_label_y = best_pos
            else:
                # Fallback if no label positions
                col_label_x = (start_x + end_x) / 2
                col_label_y = (start_y + end_y) / 2

            relationship_lines_svg += f"""
        <g class="er-relationship" data-table1="{rel['table1']}" data-table2="{rel['table2']}" data-rel-id="rel-{rel_idx}">
            <title>{tooltip_text}</title>
            <path d="{path_d}" stroke="{line_color}" stroke-width="{line_width}"
                  fill="none" stroke-dasharray="{5 if i > 0 else 0}"/>
            <!-- Cardinality labels with backgrounds -->
            <rect x="{label_start_x - 9}" y="{label_start_y - 9}" width="18" height="18"
                  fill="white" stroke="{line_color}" stroke-width="0.5" rx="3" opacity="0.95"/>
            <text x="{label_start_x}" y="{label_start_y}" font-family="Inter, sans-serif" font-size="13" font-weight="700"
                  fill="{line_color}" text-anchor="middle" dominant-baseline="middle">
                {label_start}
            </text>
            <rect x="{label_end_x - 9}" y="{label_end_y - 9}" width="18" height="18"
                  fill="white" stroke="{line_color}" stroke-width="0.5" rx="3" opacity="0.95"/>
            <text x="{label_end_x}" y="{label_end_y}" font-family="Inter, sans-serif" font-size="13" font-weight="700"
                  fill="{line_color}" text-anchor="middle" dominant-baseline="middle">
                {label_end}
            </text>
            <!-- Column name label with background (no rotation for orthogonal lines) -->
            <rect x="{col_label_x - len(rel['column1']) * 3.5}" y="{col_label_y - 8}"
                  width="{len(rel['column1']) * 7}" height="16"
                  fill="white" rx="3" opacity="0.9"/>
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
