# interactive_viz_sna.py
import os, math
import pandas as pd
import networkx as nx
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from dotenv import load_dotenv
from pyvis.network import Network

load_dotenv()

HOST = os.getenv("PG_HOST", "127.0.0.1")
PORT = os.getenv("PG_PORT", "5432")
USER = os.getenv("PG_USER", "postgres")
PWD  = os.getenv("PG_PASSWORD", "")
DB   = os.getenv("SOCIALENS_CLONE", "socialens_clone_db")
SCHEMA = os.getenv("SNA_DB", "sna")

DB_URL = f"postgresql+psycopg2://{USER}:{quote_plus(PWD)}@{HOST}:{PORT}/{DB}"
engine = create_engine(DB_URL, connect_args={"options": f"-c search_path={SCHEMA}"}, pool_pre_ping=True)

def load_df(sql: str) -> pd.DataFrame:
    with engine.begin() as conn:
        return pd.read_sql(text(sql), conn)

# --- 1) Load nodes / edges dari MV
nodes = load_df("""
    SELECT node_id, node_type, platform, project_id, project_name, post_id, label
    FROM node_dim_mv
""")
edges = load_df("""
    SELECT platform, interaction, source_node_id, target_node_id,
           project_id, project_name, post_id, created_at, text_raw,
           sentiment_label, sentiment_score, emotion_label, emotion_score,
           hs_is_hate, hs_score, hs_percentage, spam_label, spam_score
    FROM edges_all_mv
""")

# --- 2) Build graf (directed)
G = nx.DiGraph()
for _, r in nodes.iterrows():
    # label ringkas untuk node
    if r["node_type"] == "project":
        label = f"📁 {r['project_name']} (#{r['project_id']})"
        color = "#68CDFF"
        size  = 28
        shape = "box"
    elif r["node_type"] == "post":
        label = f"📝 {r['platform']} • {r['post_id']}\n{r['project_name']}"
        color = "#FF0BAA" if r["platform"]=="instagram" else "#010101" if r["platform"]=="tiktok" else "#9E9E9E"
        size  = 18
        shape = "dot"
    else:  # user
        label = r["label"].replace("user:", "👤 ")
        color = "#8BC34A"
        size  = 10
        shape = "dot"

    title = (
        f"<b>Node:</b> {r['node_id']}<br>"
        f"<b>Type:</b> {r['node_type']}<br>"
        f"<b>Project:</b> {r['project_name']} (#{r['project_id']})<br>"
        f"<b>Platform:</b> {r['platform'] or '-'}<br>"
        f"<b>Post ID:</b> {r['post_id'] or '-'}"
    )

    G.add_node(
        r["node_id"],
        label=label,
        color=color,
        size=size,
        shape=shape,
        title=title
    )

# helper warna edges
def edge_color(inter: str) -> str:
    inter = (inter or "").lower()
    if "reply" in inter:   return "#FF9800"
    if "mention" in inter: return "#9C27B0"
    if "retweet" in inter or "share" in inter: return "#3F51B5"
    if "comment" in inter: return "#4CAF50"
    return "#9E9E9E"

for _, e in edges.iterrows():
    # tooltip kaya informasi + wrapping
    meta = []
    if pd.notna(e["sentiment_label"]): meta.append(f"Sentiment: <b>{e['sentiment_label']}</b> ({e['sentiment_score']})")
    if pd.notna(e["emotion_label"]):   meta.append(f"Emotion: <b>{e['emotion_label']}</b> ({e['emotion_score']})")
    if pd.notna(e["hs_is_hate"]):      meta.append(f"Hate Speech: <b>{'YES' if e['hs_is_hate'] else 'NO'}</b> (score={e['hs_score']}, pct={e['hs_percentage']})")
    if pd.notna(e["spam_label"]):      meta.append(f"Spam: <b>{e['spam_label']}</b> (score={e['spam_score']})")
    meta_str = "<br>".join(meta) if meta else "-"

    text_html = (e["text_raw"] or "")
    # aman untuk tooltips panjang
    text_html = text_html.replace("<", "&lt;").replace(">", "&gt;")

    title = (
        f"<div style='max-width:420px;white-space:normal'>"
        f"<b>Interaction:</b> {e['interaction']} | <b>Platform:</b> {e['platform']}<br>"
        f"<b>Project:</b> {e['project_name']} (#{e['project_id']})<br>"
        f"<b>Post ID:</b> {e['post_id']}<br>"
        f"<b>Created:</b> {e['created_at']}<br>"
        f"<b>Text:</b> {text_html}<br>"
        f"{meta_str}"
        f"</div>"
    )

    # bobot garis pake intensitas skor (jika ada)
    base_w = 1.5
    bonus  = 0.0
    if pd.notna(e["sentiment_score"]):
        bonus += min(3.0, abs(float(e["sentiment_score"])) * 2.0)
    if pd.notna(e["emotion_score"]):
        bonus += min(2.0, abs(float(e["emotion_score"])) * 1.5)
    width = base_w + bonus

    G.add_edge(
        e["source_node_id"],
        e["target_node_id"],
        color=edge_color(e["interaction"]),
        title=title,
        label=e["interaction"],
        width=width,
        arrows="to"
    )

# --- 3) Spring layout (NetworkX), lalu kunci posisi di PyVis
pos = nx.spring_layout(G, seed=42, k=None)  # biarkan otomatis; atur k kalau graf sangat padat
net = Network(height="800px", width="100%", directed=True, bgcolor="#ffffff", font_color="black")
net.barnes_hut(gravity=-2500, central_gravity=0.3, spring_length=120, spring_strength=0.025)  # physics on (tetap mengikuti spring)
# Jika ingin benar-benar fix posisi (tanpa physics), gunakan: net.toggle_physics(False)

# Translate dari NetworkX ke PyVis dengan posisi
for n, data in G.nodes(data=True):
    x, y = pos[n]
    net.add_node(n,
        label=data.get("label"),
        title=data.get("title"),
        color=data.get("color"),
        size=data.get("size", 10),
        shape=data.get("shape", "dot"),
        x=float(x)*1000.0, y=float(y)*1000.0, physics=False  # kunci layout spring
    )

for u, v, data in G.edges(data=True):
    net.add_edge(u, v,
        color=data.get("color"),
        title=data.get("title"),
        label=data.get("label"),
        width=float(data.get("width", 1.5))
    )

# Opsi tampilan (legend sederhana via HTML)
legend_html = """
<div style="position:absolute; right:20px; top:20px; background:#222; color:#fff; padding:10px 12px; border-radius:10px; font: 12px/1.2 sans-serif;">
  <b>Legend</b><br>
  <span style="color:#607D8B">■</span> Project &nbsp;
  <span style="color:#2196F3">●</span> Post (IG) &nbsp;
  <span style="color:#00BCD4">●</span> Post (TikTok) &nbsp;
  <span style="color:#8BC34A">●</span> User
  <hr style="border:0;border-top:1px solid #444;margin:6px 0">
  <span style="color:#4CAF50">━</span> comment &nbsp;
  <span style="color:#FF9800">━</span> reply &nbsp;
  <span style="color:#3F51B5">━</span> share/retweet &nbsp;
  <span style="color:#9C27B0">━</span> mention
</div>
"""
net.set_template("""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>SNA Network</title>
  {{ head }}
</head>
<body>
  {{ body }}
  """ + legend_html + """
</body>
</html>
""")

out_html = "sna_network_v3.html"
# Hindari error encoding di Windows: tulis manual pakai UTF-8
with open(out_html, "w", encoding="utf-8") as f:
    f.write(net.generate_html(notebook=False))

print(f"Exported: {out_html}")