from graphviz import Digraph
import webbrowser

# --- Define tables and relationships ---
tables = {
    "users": ["id", "name", "email"],
    "orders": ["id", "user_id", "total"],
    "order_items": ["id", "order_id", "product_id", "quantity"],
    "products": ["id", "name", "price"]
}

relationships = [
    ("users.id", "orders.user_id"),          # 1-to-many
    ("orders.id", "order_items.order_id"),   # 1-to-many
    ("products.id", "order_items.product_id") # 1-to-many
]

# --- Extract relevant columns (PK + used in relationships) ---
def relevant_columns(table_name):
    pk = tables[table_name][0]  # assume first column is PK
    rel_cols = {pk}

    for rel in relationships:
        left, right = rel
        l_table, l_col = left.split(".")
        r_table, r_col = right.split(".")

        if l_table == table_name:
            rel_cols.add(l_col)
        if r_table == table_name:
            rel_cols.add(r_col)

    return [col for col in tables[table_name] if col in rel_cols]

# --- Create Graphviz Digraph ---
dot = Digraph("ERD", format="svg")
dot.attr(rankdir="LR", bgcolor="white")
dot.attr("node", shape="plaintext")

for table in tables:
    cols = relevant_columns(table)
    pk = tables[table][0]

    # Build HTML-like table for Graphviz node
    header = f'<tr><td bgcolor="lightgray" colspan="2"><b>{table}</b></td></tr>'
    rows = ""
    for col in cols:
        color = "#d5f5e3" if col == pk else "white"
        rows += f'<tr><td align="left" bgcolor="{color}">{col}</td></tr>'

    html_table = f'''<
    <table border="1" cellborder="0" cellspacing="0">
        {header}
        {rows}
    </table>>'''

    dot.node(table, html_table, tooltip=f"{table}: {', '.join(cols)}")

# --- Add relationships ---
for left, right in relationships:
    l_table, l_col = left.split(".")
    r_table, r_col = right.split(".")

    # Label direction: 1 → n
    dot.edge(l_table, r_table, label="1 → n", tooltip=f"{l_table}.{l_col} → {r_table}.{r_col}")

# --- Render diagram as interactive HTML ---
output_path = "er_diagram"
dot.render(output_path, view=False)

# Convert SVG to HTML wrapper for hover support
html_content = f"""
<html>
<head><title>ER Diagram</title></head>
<body>
<h2 style='font-family:Arial'>Entity Relationship Diagram</h2>
<object type="image/svg+xml" data="{output_path}.svg" width="100%" height="800px"></object>
</body>
</html>
"""

html_file = output_path + ".html"
with open(html_file, "w") as f:
    f.write(html_content)

print(f"✅ ER diagram saved as {html_file}")
webbrowser.open(html_file)
