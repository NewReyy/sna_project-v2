import os, math, gzip
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from dotenv import load_dotenv
from pyvis.network import Network

# ====== Konfigurasi ======
MODE = os.getenv("MODE", "per_project")  # "global" atau "per_project"
USE_PAIR_AGG = True
LAYOUT_KEY_GLOBAL = "global|full"
LAYOUT_KEY_FMT    = "proj={pid}|full"

CHUNK_EDGES   = 15000     # split HTML per 15k edge
TOPK_PER_POST = None      # None=semua; atau angka (mis. 100) untuk ringkas
DEGREE_MIN    = 1         # buang user derajat < 1? (1=tidak buang)
TRUNCATE_TEXT = 180       # potong tooltip teks
OUT_DIR       = "pyvis_exports"
HTML_HEIGHT   = "800px"

# ====== DB ======
load_dotenv()
HOST=os.getenv("PG_HOST","127.0.0.1"); PORT=os.getenv("PG_PORT","5432")
USER=os.getenv("PG_USER","postgres"); PWD=os.getenv("PG_PASSWORD","")
DB=os.getenv("SOCIALENS_CLONE","socialens_clone_db"); SCHEMA=os.getenv("SNA_DB","sna")
URL=f"postgresql+psycopg2://{USER}:{quote_plus(PWD)}@{HOST}:{PORT}/{DB}"
engine=create_engine(URL, connect_args={"options": f"-c search_path={SCHEMA}"}, pool_pre_ping=True)
def q(sql, p=None):
    with engine.begin() as c: return pd.read_sql(text(sql), c, params=p or {})

# ====== Palet ======
platform_palette = {
    "instagram": "#E1306C", "tiktok": "#010101", "twitter": "#1DA1F2",
    "x": "#1DA1F2", "youtube": "#FF0000", "facebook": "#1877F2", "unknown": "#888888"
}
emo_palette = {
    "joy": "#FFC107","sadness": "#2196F3","anger": "#E91E63","fear": "#6A1B9A",
    "surprise": "#00ACC1","disgust": "#4E342E","trust": "#388E3C","anticipation": "#F57C00"
}
def interaction_color(inter:str)->str:
    inter=(inter or "").lower()
    if "reply" in inter: return "#FF9800"
    if "mention" in inter: return "#9C27B0"
    if "retweet" in inter or "share" in inter: return "#3F51B5"
    if "comment" in inter: return "#4CAF50"
    return "#9E9E9E"
def score_based_color(row, fallback):
    if (row.get("any_hate") is True) or (pd.notna(row.get("hs_score")) and float(row["hs_score"])>=0.5) \
       or (pd.notna(row.get("hs_percentage")) and float(row["hs_percentage"])>=50.0):
        return "#FF1744"
    if (pd.notna(row.get("spam_score")) and float(row["spam_score"])>=0.5) or ("spam" in str(row.get("spam_label","")).lower()):
        return "#FF9800"
    sent=None
    for k in ("avg_sent_score","sentiment_score","sent_score01","avg_sent"):
        v=row.get(k)
        if pd.notna(v):
            try: sent=float(v); break
            except: pass
    if sent is not None:
        if sent>=0.66: return "#2E7D32"
        if sent>=0.33: return "#FBC02D"
        return "#B71C1C"
    emo=(row.get("emotion_label") or "").strip().lower()
    if emo: return emo_palette.get(emo, "#607D8B")
    return fallback

# ====== Data loaders ======
def get_edges_global():
    if USE_PAIR_AGG:
        sql = """
        SELECT platform, interaction, source_node_id AS s, target_node_id AS t,
               project_id, project_name, post_id,
               weight, last_created,
               avg_sent_score, any_hate, hs_score, hs_percentage,
               spam_label, spam_score, sentiment_label, emotion_label, text_raw
        FROM edges_pair_agg_mv
        ORDER BY last_created DESC, weight DESC
        """
    else:
        sql = """
        SELECT platform, interaction, source_node_id AS s, target_node_id AS t,
               project_id, project_name, post_id,
               1::int AS weight, created_at AS last_created,
               sentiment_score AS avg_sent_score, hs_is_hate AS any_hate,
               hs_score, hs_percentage, spam_label, spam_score,
               sentiment_label, emotion_label, text_raw
        FROM edges_all_mv
        ORDER BY created_at DESC
        """
    return q(sql)

def get_edges_by_project(pid:int):
    if USE_PAIR_AGG:
        sql = """
        SELECT platform, interaction, source_node_id AS s, target_node_id AS t,
               project_id, project_name, post_id,
               weight, last_created,
               avg_sent_score, any_hate, hs_score, hs_percentage,
               spam_label, spam_score, sentiment_label, emotion_label, text_raw
        FROM edges_pair_agg_mv WHERE project_id = :pid
        ORDER BY last_created DESC, weight DESC
        """
    else:
        sql = """
        SELECT platform, interaction, source_node_id AS s, target_node_id AS t,
               project_id, project_name, post_id,
               1::int AS weight, created_at AS last_created,
               sentiment_score AS avg_sent_score, hs_is_hate AS any_hate,
               hs_score, hs_percentage, spam_label, spam_score,
               sentiment_label, emotion_label, text_raw
        FROM edges_all_mv WHERE project_id = :pid
        ORDER BY created_at DESC
        """
    return q(sql, {"pid": pid})

def get_project_list(min_edges=1, limit=None):
    base = "FROM " + ("edges_pair_agg_mv" if USE_PAIR_AGG else "edges_all_mv")
    sql = f"""
    SELECT project_id, project_name, COUNT(*) AS n
    {base}
    GROUP BY 1,2
    HAVING COUNT(*) >= :m
    ORDER BY n DESC
    """
    if limit:
        return q(sql+" LIMIT :lim", {"m":min_edges,"lim":limit})
    return q(sql, {"m":min_edges})

def nodes_for_edges(edges: pd.DataFrame) -> pd.DataFrame:
    node_ids = pd.unique(pd.concat([edges["s"], edges["t"]], ignore_index=True))
    df = q("""
      SELECT node_id, node_type, platform, project_id, project_name, post_id, label
      FROM node_dim_mv WHERE node_id = ANY(:ids)
    """, {"ids": node_ids.tolist()})
    missing = set(node_ids) - set(df["node_id"])
    if missing:
        add = pd.DataFrame({
            "node_id": list(missing),
            "node_type": ["user"]*len(missing),
            "platform": [None]*len(missing),
            "project_id": [None]*len(missing),
            "project_name": [None]*len(missing),
            "post_id": [None]*len(missing),
            "label": list(missing)
        })
        df = pd.concat([df, add], ignore_index=True)
    return df

def load_positions(layout_key:str, node_ids:list[str]) -> dict:
    if not node_ids: return {}
    df = q("""
      SELECT node_id, x_norm, y_norm FROM node_positions
      WHERE layout_key = :k AND node_id = ANY(:ids)
    """, {"k": layout_key, "ids": node_ids})
    return {r.node_id: (float(r.x_norm), float(r.y_norm)) for _,r in df.iterrows()}

# ====== Helpers ======
def ensure_dir(p): os.makedirs(p, exist_ok=True)
def gzip_write(path, html_text:str):
    with gzip.open(path + ".gz", "wt", encoding="utf-8") as f: f.write(html_text)

def build_net(nodes_df, edges_df, pos_map, title_suffix=""):
    net = Network(height=HTML_HEIGHT, width="100%", directed=True,
                  bgcolor="#111111", font_color="white", cdn_resources="remote")
    net.toggle_physics(False)

    # Nodes
    for _, r in nodes_df.iterrows():
        ntype = r["node_type"]
        if ntype == "project":
            label = f"📁 {r['project_name']} (#{r['project_id']})"; color="#607D8B"; size=26; shape="box"
        elif ntype == "post":
            label = f"📝 {r['platform']} • {r['post_id']}"; color=platform_palette.get((r["platform"] or "unknown").lower(), "#9E9E9E"); size=16; shape="dot"
        else:
            label = ""; color="#8BC34A"; size=8; shape="dot"  # user: tanpa label (hemat ukuran)
        nid = r["node_id"]
        xn, yn = pos_map.get(nid, (None,None))
        xpx = None if xn is None else int(xn*1200); ypx = None if yn is None else int(yn*1200)
        title = (f"<b>Node:</b> {nid}<br><b>Type:</b> {r['node_type']}<br>"
                 f"<b>Project:</b> {r['project_name']} (#{r['project_id']})<br>"
                 f"<b>Platform:</b> {r['platform'] or '-'}<br><b>Post ID:</b> {r['post_id'] or '-'}")
        net.add_node(nid, label=label, title=title, color=color, size=size, shape=shape,
                     x=xpx, y=ypx, physics=False)

    # Edges
    for _, e in edges_df.iterrows():
        txt = (e.get("text_raw") or "")
        if TRUNCATE_TEXT and len(txt) > TRUNCATE_TEXT: txt = txt[:TRUNCATE_TEXT] + "…"
        txt = txt.replace("<","&lt;").replace(">","&gt;")
        meta=[]
        if pd.notna(e.get("sentiment_label")) or pd.notna(e.get("avg_sent_score")): meta.append(f"Sentiment: <b>{e.get('sentiment_label','')}</b> ({e.get('avg_sent_score','')})")
        if pd.notna(e.get("emotion_label")): meta.append(f"Emotion: <b>{e.get('emotion_label','')}</b>")
        if (e.get("any_hate") is True) or pd.notna(e.get("hs_score")) or pd.notna(e.get("hs_percentage")): meta.append(f"Hate: <b>{'YES' if e.get('any_hate') else 'NO'}</b> (score={e.get('hs_score','')}, pct={e.get('hs_percentage','')})")
        if pd.notna(e.get("spam_label")) or pd.notna(e.get("spam_score")): meta.append(f"Spam: <b>{e.get('spam_label','')}</b> (score={e.get('spam_score','')})")
        title = f"<div style='max-width:420px;white-space:normal'><b>{e['interaction']}</b> · {e['platform']} · Post {e['post_id']}<br>{'<br>'.join(meta) if meta else '-'}<hr style='border:0;border-top:1px solid #444'>{txt}</div>"
        width = 1.3 + min(2.5, math.log1p(float(e.get("weight",1))))
        edge_col = score_based_color(e, interaction_color(e["interaction"]))
        net.add_edge(e["s"], e["t"], color=edge_col, label="", title=title, width=width, arrows="to")

    legend = """
    <div style="position:absolute; right:20px; top:20px; background:#222; color:#fff; padding:8px 10px; border-radius:10px; font:12px/1.25 sans-serif;">
      Edge priority: <span style="color:#FF1744">Hate</span> → <span style="color:#FF9800">Spam</span> →
      Sentiment <span style="color:#2E7D32">Hijau</span>/<span style="color:#FBC02D">Kuning</span>/<span style="color:#B71C1C">Merah</span>
    </div>
    """
    net.set_template(f"<!DOCTYPE html><html><head><meta charset='utf-8'/><title>SNA {title_suffix}</title>{{{{ head }}}}</head><body>{{{{ body }}}}{legend}</body></html>")
    return net

def export_one(html_path, edges_df, layout_key, title_suffix):
    # opsional topK & degree filter (hanya mempengaruhi snapshot, data asli tetap)
    if TOPK_PER_POST:
        edges_df = (edges_df.sort_values(["post_id","weight","last_created"], ascending=[True,False,False])
                            .groupby("post_id", as_index=False, group_keys=False).head(TOPK_PER_POST))
    if DEGREE_MIN and DEGREE_MIN>1:
        deg = pd.concat([edges_df["s"], edges_df["t"]]).to_frame("n").assign(c=1).groupby("n", as_index=False)["c"].sum()
        keep = set(deg.loc[deg.c>=DEGREE_MIN,"n"])
        edges_df = edges_df[edges_df["s"].isin(keep) & edges_df["t"].isin(keep)]

    # split per CHUNK_EDGES agar HTML ringan
    chunks = [edges_df] if (not CHUNK_EDGES or len(edges_df)<=CHUNK_EDGES) else \
             [edges_df.iloc[i:i+CHUNK_EDGES] for i in range(0, len(edges_df), CHUNK_EDGES)]

    os.makedirs(os.path.dirname(html_path) or ".", exist_ok=True)
    for idx, chunk in enumerate(chunks, start=1):
        nodes_df = nodes_for_edges(chunk)
        pos_map  = load_positions(layout_key, nodes_df["node_id"].tolist())
        net = build_net(nodes_df, chunk, pos_map, title_suffix=title_suffix)
        html = net.generate_html(notebook=False)

        part = f"_part{idx}" if len(chunks)>1 else ""
        out_file = html_path.replace(".html", f"{part}.html")
        with open(out_file, "w", encoding="utf-8") as f: f.write(html)
        with gzip.open(out_file + ".gz", "wt", encoding="utf-8") as gz: gz.write(html)
        print(f"Exported: {out_file} (+.gz) | nodes≈{len(nodes_df)} edges={len(chunk)}")

def run_global():
    edges = get_edges_global()
    export_one(os.path.join(OUT_DIR, "sna_global.html"), edges, LAYOUT_KEY_GLOBAL, "Global")

def run_per_project():
    projs = get_project_list()
    for _, row in projs.iterrows():
        pid = int(row.project_id)
        edges = get_edges_by_project(pid)
        export_one(os.path.join(OUT_DIR, f"sna_project_{pid}.html"),
                   LAYOUT_KEY_FMT.format(pid=pid), f"Project #{pid}")

if __name__ == "__main__":
    if MODE == "global":
        run_global()
    else:
        run_per_project()