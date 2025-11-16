import networkx as nx
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch
import mplcursors

# --- Define schema ---
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
    pk = tables[table_name][0]
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


# --- Determine FK columns ---
def foreign_keys():
    fks = set()
    for _, right in relationships:
        fks.add(right)
    return fks


FKs = foreign_keys()

# --- Build graph structure ---
G = nx.DiGraph()
for table in tables:
    G.add_node(table)

for left, right in relationships:
    l_table, l_col = left.split(".")
    r_table, r_col = right.split(".")
    G.add_edge(l_table, r_table, label="1→n", tooltip=f"{l_table}.{l_col} → {r_table}.{r_col}")

# --- Compute layout ---
pos = nx.spring_layout(G, seed=42)

# --- Draw edges first ---
fig, ax = plt.subplots(figsize=(10, 6))
plt.title("Entity Relationship Diagram", fontsize=14, weight="bold")

nx.draw_networkx_edges(G, pos, arrowstyle="-|>", arrowsize=14, edge_color="gray", width=1.2, ax=ax)
nx.draw_networkx_edge_labels(G, pos, edge_labels={(u, v): d['label'] for u, v, d in G.edges(data=True)}, font_size=9, ax=ax)

# --- Draw custom table-like nodes ---
node_artists = []
tooltips = []

for table, (x, y) in pos.items():
    cols = relevant_columns(table)
    pk = tables[table][0]

    # Draw box
    width = 1.8
    height = 0.25 * (len(cols) + 1)
    box = FancyBboxPatch(
        (x - width/2, y - height/2),
        width, height,
        boxstyle="round,pad=0.02",
        fc="#e3f2fd",
        ec="black",
        lw=1.2,
        mutation_aspect=0.8,
    )
    ax.add_patch(box)
    node_artists.append(box)
    tooltips.append(f"{table}: {', '.join(cols)}")

    # Table name (header)
    ax.text(x, y + height/2 - 0.2, table, fontsize=10, fontweight="bold",
            ha="center", va="top", color="black")

    # Columns list
    for i, col in enumerate(cols):
        color = "#2e7d32" if f"{table}.{col}" in FKs else ("#0d47a1" if col == pk else "black")
        style = "italic" if f"{table}.{col}" in FKs else "normal"
        ax.text(x - width/2 + 0.1, y + height/2 - 0.45 - 0.25 * i,
                col, fontsize=9, color=color, style=style, ha="left", va="top", fontfamily="monospace")

plt.axis("off")

# --- Hover tooltips ---
cursor = mplcursors.cursor(node_artists, hover=True)
@cursor.connect("add")
def on_hover(sel):
    sel.annotation.set_text(tooltips[sel.index])
    sel.annotation.get_bbox_patch().set(fc="white", alpha=0.9)

plt.tight_layout()
plt.show()
