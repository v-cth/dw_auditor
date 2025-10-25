"""
HTML generation for table relationship visualizations
"""

from typing import List, Dict, Optional
import json


def generate_relationships_summary_section(relationships: List[Dict], min_confidence: float = 0.5) -> str:
    """
    Generate minimalist inline CSS section for summary.html

    Args:
        relationships: List of relationship dictionaries
        min_confidence: Minimum confidence to display

    Returns:
        HTML string with relationship section
    """
    # Filter relationships by minimum display confidence
    display_relationships = [r for r in relationships if r['confidence'] >= min_confidence]

    if not display_relationships:
        return ""

    # Sort by confidence descending
    display_relationships.sort(key=lambda x: x['confidence'], reverse=True)

    html = """
    <section class="relationships-section" style="margin-top: 40px;">
        <h2 style="font-size: 24px; font-weight: 600; color: #1f2937; margin-bottom: 20px;">
            Table Relationships
        </h2>
        <p style="color: #6b7280; margin-bottom: 20px;">
            Automatically detected relationships between tables based on column names, data types, and value overlaps.
        </p>

        <table style="width: 100%; border-collapse: collapse; background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,0.1);">
            <thead style="background-color: #f9fafb; border-bottom: 2px solid #e5e7eb;">
                <tr>
                    <th style="padding: 12px; text-align: left; font-weight: 600; color: #4b5563; font-size: 13px;">SOURCE</th>
                    <th style="padding: 12px; text-align: center; font-weight: 600; color: #4b5563; font-size: 13px; width: 40px;"></th>
                    <th style="padding: 12px; text-align: left; font-weight: 600; color: #4b5563; font-size: 13px;">TARGET</th>
                    <th style="padding: 12px; text-align: left; font-weight: 600; color: #4b5563; font-size: 13px; width: 200px;">CONFIDENCE</th>
                    <th style="padding: 12px; text-align: center; font-weight: 600; color: #4b5563; font-size: 13px; width: 120px;">TYPE</th>
                    <th style="padding: 12px; text-align: center; font-weight: 600; color: #4b5563; font-size: 13px; width: 100px;">MATCHING</th>
                </tr>
            </thead>
            <tbody>
    """

    for rel in display_relationships:
        confidence_pct = rel['confidence'] * 100

        # Confidence color based on threshold
        if confidence_pct >= 90:
            confidence_color = "#10b981"  # Green
        elif confidence_pct >= 70:
            confidence_color = "#6606dc"  # Purple
        else:
            confidence_color = "#f59e0b"  # Orange

        # Relationship type badge styling
        type_styles = {
            "one-to-one": "background: #dbeafe; color: #1e40af;",
            "one-to-many": "background: #e0e7ff; color: #4338ca;",
            "many-to-many": "background: #fce7f3; color: #9f1239;"
        }
        type_style = type_styles.get(rel['relationship_type'], "background: #f3f4f6; color: #4b5563;")

        html += f"""
                <tr style="border-bottom: 1px solid #f3f4f6;">
                    <td style="padding: 12px;">
                        <div style="font-weight: 500; color: #1f2937;">{rel['table1']}</div>
                        <div style="font-size: 12px; color: #6b7280; font-family: 'Courier New', monospace;">{rel['column1']}</div>
                    </td>
                    <td style="padding: 12px; text-align: center; color: #9ca3af;">
                        ↔
                    </td>
                    <td style="padding: 12px;">
                        <div style="font-weight: 500; color: #1f2937;">{rel['table2']}</div>
                        <div style="font-size: 12px; color: #6b7280; font-family: 'Courier New', monospace;">{rel['column2']}</div>
                    </td>
                    <td style="padding: 12px;">
                        <div style="display: flex; align-items: center; gap: 8px;">
                            <div style="flex: 1; height: 8px; background: #f3f4f6; border-radius: 4px; overflow: hidden;">
                                <div style="height: 100%; width: {confidence_pct:.1f}%; background: {confidence_color};"></div>
                            </div>
                            <span style="font-size: 13px; font-weight: 600; color: {confidence_color}; min-width: 42px;">{confidence_pct:.0f}%</span>
                        </div>
                    </td>
                    <td style="padding: 12px; text-align: center;">
                        <span style="padding: 4px 8px; border-radius: 4px; font-size: 11px; font-weight: 500; {type_style}">
                            {rel['relationship_type']}
                        </span>
                    </td>
                    <td style="padding: 12px; text-align: center; font-weight: 500; color: #4b5563;">
                        {rel['matching_values']:,}
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
    Generate full interactive report with vis.js network diagram

    Args:
        relationships: List of relationship dictionaries
        tables_metadata: Metadata about tables (row counts, column counts)
        output_path: Path to save HTML file
        min_confidence_display: Minimum confidence to display
    """
    # Filter relationships
    display_relationships = [r for r in relationships if r['confidence'] >= min_confidence_display]
    display_relationships.sort(key=lambda x: x['confidence'], reverse=True)

    # Prepare nodes data (tables)
    tables_in_relationships = set()
    for rel in display_relationships:
        tables_in_relationships.add(rel['table1'])
        tables_in_relationships.add(rel['table2'])

    nodes_data = []
    for table_name in sorted(tables_in_relationships):
        metadata = tables_metadata.get(table_name, {})
        row_count = metadata.get('total_rows', 'N/A')
        col_count = metadata.get('column_count', 'N/A')

        # Format row count
        if isinstance(row_count, int):
            row_count_str = f"{row_count:,}"
        else:
            row_count_str = str(row_count)

        nodes_data.append({
            'id': table_name,
            'label': f"{table_name}\\n({row_count_str} rows)",
            'title': f"Table: {table_name}<br>Rows: {row_count_str}<br>Columns: {col_count}"
        })

    # Prepare edges data (relationships)
    edges_data = []
    for rel in display_relationships:
        # Create short label for display
        short_label = f"{rel['column1'][:15]}..." if len(rel['column1']) > 15 else rel['column1']

        edges_data.append({
            'from': rel['table1'],
            'to': rel['table2'],
            'label': '',  # Hide label on edge, show in tooltip only
            'title': f"<b>{rel['column1']} ↔ {rel['column2']}</b><br>Confidence: {rel['confidence']:.1%}<br>Type: {rel['relationship_type']}<br>Matching values: {rel['matching_values']:,}",
            'width': max(2, rel['confidence'] * 6),
            'value': rel['confidence'],
            'color': {
                'color': '#9ca3af' if rel['confidence'] < 0.9 else '#6606dc',
                'highlight': '#6606dc'
            }
        })

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

        relationships_html += f'''
        <div class="relationship-item {confidence_class}">
            <strong>{rel['table1']}.{rel['column1']}</strong> ↔
            <strong>{rel['table2']}.{rel['column2']}</strong><br>
            <small>
                Confidence: {confidence_pct:.1f}% |
                Type: {rel['relationship_type']} |
                Matching values: {rel['matching_values']}
            </small>
        </div>
        '''

    # Generate HTML template
    html_template = f'''<!DOCTYPE html>
<html>
<head>
    <title>Table Relationships Visualization</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet">
    <script type="text/javascript" src="https://unpkg.com/vis-network/standalone/umd/vis-network.min.js"></script>
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
            max-width: 1400px;
            margin: 0 auto;
            padding: 40px 20px;
        }}
        #mynetwork {{
            width: 100%;
            height: 600px;
            border: 1px solid #e5e7eb;
            background-color: white;
            border-radius: 8px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
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
    </style>
</head>
<body>
    <div class="header">
        <h1>Table Relationships Visualization</h1>
        <p>Interactive network diagram showing detected relationships between tables</p>
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

        <div id="mynetwork"></div>

        <div class="info-panel">
            <h2>Detected Relationships</h2>
            <div id="relationships-list">
                {relationships_html}
            </div>
        </div>
    </div>

    <script type="text/javascript">
        // Create nodes and edges
        var nodes = new vis.DataSet({json.dumps(nodes_data)});
        var edges = new vis.DataSet({json.dumps(edges_data)});

        // Create network
        var container = document.getElementById('mynetwork');
        var data = {{
            nodes: nodes,
            edges: edges
        }};

        var options = {{
            nodes: {{
                shape: 'box',
                font: {{
                    size: 16,
                    color: 'white',
                    face: 'Inter'
                }},
                color: {{
                    background: '#6606dc',
                    border: '#5005b8',
                    highlight: {{
                        background: '#5005b8',
                        border: '#4004a0'
                    }}
                }},
                margin: 12,
                borderWidth: 2,
                borderWidthSelected: 3
            }},
            edges: {{
                smooth: {{
                    type: 'cubicBezier',
                    forceDirection: 'vertical',
                    roundness: 0.4
                }},
                font: {{
                    size: 0,  // Hide edge labels
                    face: 'Inter'
                }},
                arrows: {{
                    to: {{
                        enabled: false
                    }}
                }},
                shadow: {{
                    enabled: true,
                    color: 'rgba(0,0,0,0.1)',
                    size: 4,
                    x: 0,
                    y: 2
                }}
            }},
            physics: {{
                enabled: true,
                barnesHut: {{
                    gravitationalConstant: -8000,
                    springConstant: 0.04,
                    springLength: 150
                }},
                stabilization: {{
                    iterations: 200
                }}
            }},
            interaction: {{
                hover: true,
                tooltipDelay: 200,
                navigationButtons: true,
                keyboard: true
            }}
        }};

        var network = new vis.Network(container, data, options);

        // Add instructions
        var instructions = document.createElement('div');
        instructions.style.cssText = 'background: #f0f9ff; border: 1px solid #bae6fd; padding: 12px 16px; border-radius: 6px; margin-bottom: 20px; color: #0c4a6e; font-size: 14px;';
        instructions.innerHTML = '<strong>💡 Tip:</strong> Hover over edges (lines) to see relationship details. Drag nodes to rearrange the diagram.';
        container.parentNode.insertBefore(instructions, container);
    </script>
</body>
</html>'''

    # Write to file
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html_template)
