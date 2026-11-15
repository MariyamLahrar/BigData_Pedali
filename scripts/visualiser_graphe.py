
from pathlib import Path
import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt
from matplotlib.colors import Normalize

# ==========================================
# 1. CHEMINS DES FICHIERS
# ==========================================

BASE_DIR = Path(__file__).resolve().parent.parent

VERTICES_FILE = BASE_DIR / "vertices_casa.csv"
EDGES_FILE = BASE_DIR / "edges_casa.csv"

OUTPUT_DIR = BASE_DIR / "resultats" / "graphes"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ==========================================
# 2. CHARGEMENT DES DONNEES
# ==========================================

print("Chargement des donnees de Casablanca...")

if not VERTICES_FILE.exists():
    raise FileNotFoundError(
        f"Fichier introuvable : {VERTICES_FILE}"
    )

if not EDGES_FILE.exists():
    raise FileNotFoundError(
        f"Fichier introuvable : {EDGES_FILE}"
    )

vertices = pd.read_csv(VERTICES_FILE)
edges = pd.read_csv(EDGES_FILE)

vertices["id"] = vertices["id"].astype(str)
edges["src"] = edges["src"].astype(str)
edges["dst"] = edges["dst"].astype(str)

# ==========================================
# 3. CONSTRUCTION DU GRAPHE
# ==========================================

G = nx.DiGraph()

for _, station in vertices.iterrows():
    G.add_node(
        station["id"],
        name=str(station.get("name", station["id"])),
        capacity=float(station.get("capacity", 0))
    )

edges_grouped = (
    edges.groupby(["src", "dst"], as_index=False)["trip_count"]
    .sum()
)

for _, edge in edges_grouped.iterrows():
    G.add_edge(
        edge["src"],
        edge["dst"],
        weight=float(edge["trip_count"])
    )

print("Stations :", G.number_of_nodes())
print("Connexions :", G.number_of_edges())

# ==========================================
# 4. CALCUL DU PAGERANK
# ==========================================

pagerank = nx.pagerank(
    G,
    alpha=0.85,
    weight="weight"
)

nx.set_node_attributes(G, pagerank, "pagerank")

# ==========================================
# 5. CONFIGURATION DU DESIGN
# ==========================================

plt.rcParams.update({
    "font.family": "DejaVu Sans",
    "font.size": 11,
    "axes.titlesize": 18,
    "figure.facecolor": "#0B1220",
    "savefig.facecolor": "#0B1220",
})

fig, ax = plt.subplots(figsize=(16, 11))
ax.set_facecolor("#0B1220")

# Position stable des stations
pos = nx.spring_layout(
    G,
    seed=42,
    k=0.45,
    iterations=100,
    weight="weight"
)

values = [pagerank[node] for node in G.nodes()]

min_pr = min(values)
max_pr = max(values)

node_sizes = [
    80 + (
        (pagerank[node] - min_pr)
        / (max_pr - min_pr + 1e-12)
    ) * 950
    for node in G.nodes()
]

edge_widths = [
    0.7 + G[u][v]["weight"] * 0.5
    for u, v in G.edges()
]

# ==========================================
# 6. DESSINER LES CONNEXIONS
# ==========================================

nx.draw_networkx_edges(
    G,
    pos,
    ax=ax,
    width=edge_widths,
    edge_color="#60A5FA",
    alpha=0.35,
    arrows=True,
    arrowsize=12,
    arrowstyle="-|>",
    connectionstyle="arc3,rad=0.08"
)

# ==========================================
# 7. DESSINER LES STATIONS
# ==========================================

nodes = nx.draw_networkx_nodes(
    G,
    pos,
    ax=ax,
    node_size=node_sizes,
    node_color=values,
    cmap=plt.cm.viridis,
    vmin=min_pr,
    vmax=max_pr,
    edgecolors="#E2E8F0",
    linewidths=0.7,
    alpha=0.95
)

# ==========================================
# 8. AFFICHER LES STATIONS IMPORTANTES
# ==========================================

top_stations = sorted(
    pagerank,
    key=pagerank.get,
    reverse=True
)[:8]

labels = {
    node: node
    for node in top_stations
}

nx.draw_networkx_labels(
    G,
    pos,
    labels=labels,
    font_size=8,
    font_color="white",
    font_weight="bold",
    ax=ax
)

# ==========================================
# 9. TITRE ET LEGENDE
# ==========================================

ax.set_title(
    "PEDALI | RESEAU DES STATIONS - CASABLANCA",
    color="white",
    fontsize=19,
    fontweight="bold",
    pad=28
)

ax.text(
    0.5,
    1.015,
    "Taille des noeuds : PageRank NetworkX  |  "
    "Epaisseur des liens : nombre de trajets",
    transform=ax.transAxes,
    ha="center",
    color="#94A3B8",
    fontsize=11
)

colorbar = fig.colorbar(
    nodes,
    ax=ax,
    fraction=0.025,
    pad=0.02
)

colorbar.set_label(
    "Importance PageRank",
    color="white"
)

colorbar.ax.tick_params(colors="white")

ax.axis("off")

# ==========================================
# 10. INDICATEURS DU RESEAU
# ==========================================

nb_stations = G.number_of_nodes()
nb_connexions = G.number_of_edges()
nb_trajets = int(
    sum(data["weight"] for _, _, data in G.edges(data=True))
)

fig.text(
    0.12,
    0.05,
    f"STATIONS : {nb_stations}",
    color="#38BDF8",
    fontsize=13,
    fontweight="bold"
)

fig.text(
    0.40,
    0.05,
    f"CONNEXIONS : {nb_connexions}",
    color="#34D399",
    fontsize=13,
    fontweight="bold"
)

fig.text(
    0.72,
    0.05,
    f"TRAJETS : {nb_trajets}",
    color="#FBBF24",
    fontsize=13,
    fontweight="bold"
)

# ==========================================
# 11. ENREGISTREMENT DU GRAPHIQUE
# ==========================================

output_file = OUTPUT_DIR / "graphe_casablanca.png"

plt.savefig(
    output_file,
    dpi=220,
    bbox_inches="tight"
)

print("\n===================================")
print("VISUALISATION TERMINEE AVEC SUCCES")
print("Stations :", nb_stations)
print("Connexions :", nb_connexions)
print("Trajets :", nb_trajets)
print("Image :", output_file)
print("===================================")

plt.show()
