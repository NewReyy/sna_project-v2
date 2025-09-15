# pip install sqlalchemy psycopg2-binary pandas pyvis python-dotenv
import os
import pandas as pd
import json
from sqlalchemy import create_engine, text
from pyvis.network import Network
from collections import defaultdict
from getpass import getpass
from urllib.parse import quote_plus
from dotenv import load_dotenv

load_dotenv()

DB_USER = os.getenv("PG_USER", "postgres")
DB_PASS = os.getenv("PG_PASSWORD") or getpass(f"Password PostgreSQL untuk user '{DB_USER}': ")
DB_HOST = os.getenv("PG_HOST", "127.0.0.1")
DB_PORT = os.getenv("PG_PORT", "5432")
DB_NAME = os.getenv("SNA_DB", "sna_db")
SCHEMA  = "sna"

# urlencode password jika ada karakter spesial seperti @:/?
DB_URL = (
    f"postgresql+psycopg2://{DB_USER}:{quote_plus(DB_PASS)}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

ENGINE = create_engine(
    DB_URL,
    connect_args={"options": f"-c search_path={SCHEMA}"},
    pool_pre_ping=True,           # koneksi lebih tahan lama
)

ARTIFACTS_DIR = "artifacts"
os.makedirs(ARTIFACTS_DIR, exist_ok=True)
OUT_HTML = os.path.join(ARTIFACTS_DIR, "sna_network.html")

# Ambil interaction hanya dari tabel-tabel ini
INTERACTION_SOURCE_TABLES = {
    "edge_events",
    "edges_all_mv",
    "edges_instagram_comment_to_author",
    "edges_tiktok_reply_to_user",
}

def interaction_expr_direct(rel: str, cols: set) -> str:
    """
    Kembalikan ekspresi SQL untuk kolom interaction:
    - Kalau rel termasuk INTERACTION_SOURCE_TABLES dan kolom 'interaction' ada -> pakai langsung
    - Selain itu -> NULL (biar tooltip jadi '-' dan filter masuk 'other')
    """
    if rel in INTERACTION_SOURCE_TABLES and "interaction" in cols:
        return "NULLIF(interaction::text, '')"
    return "NULL::text"

# =========================
# Konfigurasi tooltip node (kolom & format)
# =========================
NODE_TOOLTIP_FIELDS = [
    {"col": "in_avg_sent",  "label": "Sentimen Masuk",  "pct": True},
    {"col": "in_avg_hate",  "label": "Hate Masuk",      "pct": True},
    {"col": "in_avg_spam",  "label": "Spam Masuk",      "pct": True},
    {"col": "in_evt_cnt",   "label": "Event Masuk",     "fmt": ",.0f"},
    {"col": "out_avg_sent", "label": "Sentimen Keluar", "pct": True},
    {"col": "out_avg_hate", "label": "Hate Keluar",     "pct": True},
    {"col": "out_avg_spam", "label": "Spam Keluar",     "pct": True},
    {"col": "out_evt_cnt",  "label": "Event Keluar",    "fmt": ",.0f"},
]

# Kandidat nama MV event (akan dicek mana yg benar-benar ada)
EVENT_TABLE_CANDIDATES = [
    "edge_events", "edges_events", "egdes_events",  # umum + typo
    "edge_event", "edges_event", "edge_events_mv"
]

# Urutan kandidat kolom timestamp untuk sort "terbaru"
TS_CANDIDATES_COMMON = [
    "example_ts", "ts", "ts_last", "created_at", "event_ts", "event_time", "time", "timestamp"
]

def existing_tables(conn, names):
    out = []
    for name in names:
        if table_exists(conn, name):
            out.append(name)
    return out

def fmt_pct(v, nd=2):
    """0.7676 -> 76.76% (string); None/NaN -> '-'"""
    try:
        if v is None:
            return "-"
        v = float(v)
        if v != v:  # NaN
            return "-"
        return f"{v*100:.{nd}f}%"
    except Exception:
        return "-"

def fmt_val(v, fmt=None, pct=False):
    """Formatter umum untuk tooltip node."""
    if v is None:
        return "-"
    try:
        if pct:
            return fmt_pct(v)
        if fmt:
            return format(float(v), fmt)
    except Exception:
        pass
    return str(v)

def trunc(s, n=18):
    s = "" if s is None else str(s)
    return s if len(s) <= n else s[:n-1] + "…"

def safe_txt(s, limit=600):
    """Pertahankan newline agar bisa di-wrap di tooltip."""
    if s is None:
        return "-"
    s = str(s).replace("\r\n", "\n").replace("\r", "\n").strip()
    return s if len(s) <= limit else s[:limit-1] + "…"

# =========================
# Helpers DB
# =========================
def to_i64(df, *cols):
    if df is None or df.empty: return df
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    return df

def table_exists(conn, relname: str) -> bool:
    q = text("""
        SELECT 1
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname=:s AND c.relname=:t AND c.relkind IN ('m','r','v')
        LIMIT 1
    """)
    return conn.execute(q, {"s": SCHEMA, "t": relname}).fetchone() is not None

def list_columns(conn, relname: str) -> set:
    q = text("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema=:s AND table_name=:t
    """)
    return set(r[0].lower() for r in conn.execute(q, {"s": SCHEMA, "t": relname}).fetchall())

def interaction_sql_cols(rel: str, cols: set) -> tuple[str, str]:
    """
    Return (raw_expr, norm_expr)
    raw_expr  : apa adanya dari kolom interaction/sinonim jika ada, else NULL
    norm_expr : normalisasi ringan utk pewarnaan: reply | mention | share | other
    """
    # kandidat nama kolom yang sering dipakai
    cand_cols = [
        "interaction", "interaction_type", "event_type", "event",
        "edge_type", "relation", "action", "type", "kind",
        "label", "category", "message_type", "evt", "evt_type"
    ]

    parts = [f"NULLIF(BTRIM({c}::text), '')" for c in cand_cols if c in cols]
    raw = f"COALESCE({', '.join(parts)})" if parts else "NULL::text"

    # petunjuk dari nama tabel
    rel_l = rel.lower()
    if ("reply" in rel_l) or ("comment" in rel_l):
        hint = "reply"
    elif "mention" in rel_l:
        hint = "mention"
    elif ("retweet" in rel_l) or ("share" in rel_l) or ("repost" in rel_l):
        hint = "share"
    else:
        hint = "other"

    norm = f"""
    CASE
      WHEN LOWER({raw}) LIKE '%%comment_to_author%%' THEN 'reply'
      WHEN LOWER({raw}) LIKE '%%reply_to_user%%'     THEN 'reply'
      WHEN LOWER({raw}) LIKE '%%comment%%'           THEN 'reply'
      WHEN LOWER({raw}) LIKE '%%mention%%'           THEN 'mention'
      WHEN LOWER({raw}) LIKE '%%retweet%%' OR LOWER({raw}) LIKE '%%share%%'
           OR LOWER({raw}) LIKE '%%repost%%' OR LOWER({raw}) = 'rt' THEN 'share'
      WHEN {raw} IS NOT NULL THEN LOWER({raw})
      ELSE '{hint}'
    END
    """
    return raw, norm

# =========================
# Load data dari DB
# =========================
def load_nodes_labels_features_edges():
    with ENGINE.begin() as conn:
        # Nodes
        nodes = pd.read_sql_query(text(f"""
            SELECT node_id::bigint AS node_id, platform,
                   COALESCE(NULLIF(label,''), platform||':'||node_id::text) AS label
            FROM {SCHEMA}.node_dim
        """), conn)

        # Node features (opsional)
        feats = pd.DataFrame()
        if table_exists(conn, "node_features"):
            feats = pd.read_sql_query(text(f"SELECT * FROM {SCHEMA}.node_features"), conn)

        # Temukan MV event yang benar-benar ada
        event_tables = existing_tables(conn, EVENT_TABLE_CANDIDATES)

        # MV lain (agregasi/umum)
        base_candidates = [
            "edges_all_mv",
            "edges_all_agg_mv",
            "edges_instagram_comment_to_author",
            "edges_tiktok_reply_to_user",
            "edges_agg_v2",
        ]
        base_candidates = [t for t in base_candidates if table_exists(conn, t)]

        # Urutan prioritas: event_tables (jika ada) + base_candidates
        candidates = event_tables + base_candidates

        edges_list = []
        for rel in candidates:
            cols = list_columns(conn, rel)

            # weight/platform
            weight_expr   = "COALESCE(weight, 1.0)::float AS weight" if "weight" in cols else "1.0::float AS weight"
            platform_expr = "platform" if "platform" in cols else "'unknown'::text"

            # interaction (raw + norm)
            inter_expr = interaction_expr_direct(rel, cols)

            # pilih kolom timestamp dinamis
            ts_col = next((c for c in TS_CANDIDATES_COMMON if c in cols), None)
            ts_expr = f"{ts_col}" if ts_col else "NULL::timestamptz"

            # select bagian label/score (berbeda utk event vs non-event)
            if rel in event_tables:
                select_extra = """
                    sent_label,
                    sent_score01,
                    emo_label,
                    hate_prob,
                    spam_prob,
                    event_id,
                    example_text,
                    NULL::float AS avg_sent,
                    NULL::float AS avg_hate,
                    NULL::float AS avg_spam
                """
            elif rel == "edges_agg_v2":
                select_extra = """
                    NULL::text  AS sent_label,
                    NULL::float AS sent_score01,
                    NULL::text  AS emo_label,
                    NULL::float AS hate_prob,
                    NULL::float AS spam_prob,
                    NULL::bigint AS event_id,
                    NULL::text  AS example_text,
                    avg_sent,
                    avg_hate,
                    avg_spam
                """
            else:
                select_extra = """
                    NULL::text  AS sent_label,
                    NULL::float AS sent_score01,
                    NULL::text  AS emo_label,
                    NULL::float AS hate_prob,
                    NULL::float AS spam_prob,
                    NULL::bigint AS event_id,
                    NULL::text  AS example_text,
                    NULL::float AS avg_sent,
                    NULL::float AS avg_hate,
                    NULL::float AS avg_spam
                """

            q = f"""
                SELECT
                    source_id::bigint AS source_id,
                    target_id::bigint AS target_id,
                    {weight_expr},
                    {platform_expr} AS platform,
                    {inter_expr} AS interaction,
                    {ts_expr} AS ts,
                    {select_extra},
                    '{rel}'::text AS __source_table
                FROM {SCHEMA}.{rel}
            """
            df = pd.read_sql_query(text(q), conn)
            if not df.empty:
                edges_list.append(df)

        edges_list = [df for df in edges_list if not df.empty]
        edges = pd.concat(edges_list, ignore_index=True) if edges_list else pd.DataFrame()

    nodes = to_i64(nodes, "node_id")
    feats = to_i64(feats, "node_id")
    edges = to_i64(edges, "source_id", "target_id")
        # === Backfill interaction dari edge_events agar tooltip tidak "-" ===
    try:
        inter_map = build_inter_map(limit=500000)
    except Exception:
        inter_map = {}

    if edges is not None and not edges.empty and inter_map:
        # hanya baris yang interaction-nya kosong
        mask = (edges["interaction"].isna()) | (edges["interaction"].astype(str).str.strip() == "")
        mask &= edges["source_id"].notna() & edges["target_id"].notna()
        if mask.any():
            sub = edges.loc[mask, ["source_id", "target_id"]].astype("Int64").astype("int64")
            keys = list(zip(sub["source_id"], sub["target_id"]))
            fills = [inter_map.get(k) for k in keys]
            edges.loc[mask, "interaction"] = fills

    return nodes, feats, edges

def build_inter_map(limit=500000) -> dict[tuple[int, int], str]:
    """Ambil contoh interaction per (source_id, target_id) dari edge_events,
    dan gabungkan uniknya (reply_to_user+comment_to_author+…)
    """
    with ENGINE.begin() as conn:
        if not table_exists(conn, "edge_events"):
            return {}
        examples = pd.read_sql_query(text(f"""
            SELECT
                source_id::bigint AS source_id,
                target_id::bigint AS target_id,
                platform, interaction,
                example_text, sent_label, emo_label, hate_prob, spam_prob
            FROM {SCHEMA}.edge_events
            LIMIT {int(limit)}
        """), conn)

    examples = to_i64(examples, "source_id", "target_id")
    if examples.empty or "interaction" not in examples.columns:
        return {}

    inter_map = (
        examples.dropna(subset=["interaction"])
        .groupby(["source_id", "target_id"])["interaction"]
        .apply(lambda s: "+".join(sorted(set(str(x).strip() for x in s if pd.notna(x) and str(x).strip()))))
        .to_dict()
    )
    return inter_map

def prefer_edge_events(edges: pd.DataFrame) -> pd.DataFrame:
    """Ambil 1 baris per (src,dst) dengan prioritas MV event (apa pun namanya) dan ts terbaru."""
    if edges is None or edges.empty:
        return edges
    # anggap semua tabel yang namanya mengandung 'event' adalah event-table
    is_event = edges["__source_table"].astype(str).str.lower().str.contains("event")
    ev = edges[is_event].copy()
    others = edges[~is_event].copy()

    if not ev.empty and "ts" in ev.columns:
        ev = ev.sort_values(["source_id","target_id","ts"], ascending=[True,True,False])
        ev = ev.drop_duplicates(subset=["source_id","target_id"], keep="first")

    if not ev.empty and not others.empty:
        others = others.merge(ev[["source_id","target_id"]], on=["source_id","target_id"],
                              how="left", indicator=True)
        others = others[others["_merge"] == "left_only"].drop(columns="_merge")

    return pd.concat([ev, others], ignore_index=True)

# =========================
# Build network + HTML
# =========================
def build_network(nodes, feats, edges, min_weight=0.0, max_edges=20000, physics=True, seed=42):
    edges = prefer_edge_events(edges)

    if min_weight > 0:
        edges = edges[edges["weight"] >= float(min_weight)].copy()
    if max_edges and len(edges) > max_edges:
        edges = edges.nlargest(max_edges, "weight").copy()

    # ukuran node dari derajat berbobot
    deg_w = defaultdict(float)
    for _, r in edges.iterrows():
        if pd.isna(r["source_id"]) or pd.isna(r["target_id"]): continue
        deg_w[int(r["source_id"])] += float(r["weight"])
        deg_w[int(r["target_id"])] += float(r["weight"])

    if feats is not None and not feats.empty:
        nodes = nodes.merge(feats, on="node_id", how="left", suffixes=("",""))

    usermap = nodes.set_index("node_id")["label"].to_dict()

    platform_palette = {
        "instagram": "#E1306C", "tiktok": "#010101", "twitter": "#1DA1F2", "x": "#1DA1F2",
        "youtube": "#FF0000", "facebook": "#1877F2", "unknown": "#888888"
    }
    emo_palette = {
        "joy": "#FFC107","sadness": "#2196F3","anger": "#E91E63","fear": "#6A1B9A",
        "surprise": "#00ACC1","disgust": "#4E342E","trust": "#388E3C","anticipation": "#F57C00"
    }

    nt = Network(height="900px", width="100%", bgcolor="#ffffff", font_color="#222", directed=True)
    nt.set_options(json.dumps({
        "layout": {"randomSeed": seed, "improvedLayout": True},
        "nodes": {
            "shape": "dot", "borderWidth": 1, "shadow": True,
            "scaling": {"min": 5, "max": 40, "label": {"enabled": True, "min": 8, "max": 24}},
            "font": {"size": 12, "strokeWidth": 4, "strokeColor": "#ffffff"}
        },
        "edges": {
            "smooth": False, "selectionWidth": 1.8, "hoverWidth": 0.5,
            "color": {"opacity": 0.9},
            "arrows": {"to": {"enabled": True, "scaleFactor": 0.6}}
        },
        "physics": {
            "enabled": bool(physics),
            "solver": "barnesHut",
            "barnesHut": {"gravitationalConstant": -25000, "centralGravity": 0.3, "springLength": 140},
            "stabilization": {"iterations": 200}
        },
        "interaction": {
            "hover": True, "tooltipDelay": 60,
            "navigationButtons": True, "multiselect": True, "hideEdgesOnDrag": True
        },
        # Panel Configure (vis.js)
        "configure": {"enabled": True, "filter": ["nodes","edges","physics","layout","interaction"]}
    }))

    # ==== Nodes (tooltip plain text) ====
    for _, n in nodes.iterrows():
        nid = int(n["node_id"])
        full_label = str(n.get("label") or nid)
        short_label = trunc(full_label, 18)
        platform = str(n.get("platform") or "unknown").lower()
        color = platform_palette.get(platform, "#888888")

        lines = [f"ID: {nid}", f"Platform: {platform}", f"Account: {full_label}"]
        for item in NODE_TOOLTIP_FIELDS:
            col   = item["col"]
            label = item.get("label", col)
            fmt   = item.get("fmt")
            pct   = item.get("pct", False)
            if col in nodes.columns and pd.notna(n.get(col)):
                lines.append(f"{label}: {fmt_val(n[col], fmt=fmt, pct=pct)}")
        title = "\n".join(lines)

        size = 8 + min(32, deg_w.get(nid, 0.0) ** 0.5 * 3.0)
        nt.add_node(nid, label=short_label, title=title, color=color, size=size)

    # ==== Edge color rules ====
    def edge_color(row):
        hate = None; spam = None; sent = None
        if pd.notna(row.get("hate_prob")): hate = float(row["hate_prob"])
        elif pd.notna(row.get("avg_hate")): hate = float(row["avg_hate"])
        if pd.notna(row.get("spam_prob")): spam = float(row["spam_prob"])
        elif pd.notna(row.get("avg_spam")): spam = float(row["avg_spam"])

        if hate is not None and hate >= 0.5: return "#f44336"   # merah
        if spam is not None and spam >= 0.5: return "#FF9800"   # oranye

        if pd.notna(row.get("sent_score01")): sent = float(row["sent_score01"])
        elif pd.notna(row.get("avg_sent")):   sent = float(row["avg_sent"])
        if sent is not None:
            return "#2E7D32" if sent >= 0.66 else ("#757575" if sent >= 0.33 else "#B71C1C")

        if pd.notna(row.get("emo_label")):
            return emo_palette.get(str(row["emo_label"]).lower(), "#607D8B")

        inter = (row.get("interaction") or "").lower()
        if "reply" in inter: return "#4CAF50"
        if "mention" in inter: return "#9C27B0"
        if "retweet" in inter or "share" in inter: return "#2196F3"
        return "#999999"

    # ==== Edge tooltip (plain text) ====
    def edge_title_plain(row, w):
        src = int(row["source_id"]); dst = int(row["target_id"])
        src_user = usermap.get(src, str(src)); dst_user = usermap.get(dst, str(dst))

        # sentiment label & score (fallback avg_sent)
        s_label = row.get("sent_label")
        s_score = row.get("sent_score01")
        if (s_score is None or pd.isna(s_score)) and pd.notna(row.get("avg_sent")):
            s_score = float(row["avg_sent"])
            if not s_label:
                s_label = "positive" if s_score >= 0.66 else ("neutral" if s_score >= 0.33 else "negative")
        s_line = f"{s_label} ({fmt_pct(s_score)})" if s_score is not None and not pd.isna(s_score) else "sentiment: -"

        emo = row.get("emo_label")
        emo_line = f"{emo}" if (emo is not None and not pd.isna(emo) and str(emo) != "") else "emotion: -"

        hate = row.get("hate_prob")
        if (hate is None or pd.isna(hate)) and pd.notna(row.get("avg_hate")): hate = float(row["avg_hate"])
        hate_line = f"{fmt_pct(hate)}" if hate is not None and not pd.isna(hate) else "hatespeech: -"

        spam = row.get("spam_prob")
        if (spam is None or pd.isna(spam)) and pd.notna(row.get("avg_spam")): spam = float(row["avg_spam"])
        spam_line = f"{fmt_pct(spam)}" if spam is not None and not pd.isna(spam) else "spam: -"

        inter = row.get("interaction")
        inter_line = f"{inter}" if (inter is not None and not pd.isna(inter) and str(inter) != "") else "interaction: -"

        txt = safe_txt(row.get("example_text"))

        return "\n".join([
            f"Dari '{src_user}' Ke -> '{dst_user}'",
            f"Interaksi \t\t: {inter_line}",            
            f"Sentiment \t\t: {s_line}",
            f"Emotion \t\t: {emo_line}",
            f"Hatespeech \t\t: {hate_line}",
            f"Spam \t\t: {spam_line}",
            f"Text \t\t: {txt}",
        ])

    # ==== Tambahkan edges (dengan metadata utk filter JS) ====
    for _, e in edges.iterrows():
        try:
            src = int(e["source_id"]); dst = int(e["target_id"])
        except Exception:
            continue
        w = float(e.get("weight") or 1.0)
        nt.add_edge(
            src, dst,
            value=w,
            title=edge_title_plain(e, w),
            color=edge_color(e),
            # metadata untuk filter JS:
            platform=e.get("platform"),
            interaction=e.get("interaction"),
            sent_label=e.get("sent_label"),
            sent_score01=float(e["sent_score01"]) if pd.notna(e.get("sent_score01")) else None,
            emo_label=e.get("emo_label"),
            hate_prob=float(e["hate_prob"]) if pd.notna(e.get("hate_prob")) else None,
            spam_prob=float(e["spam_prob"]) if pd.notna(e.get("spam_prob")) else None,
            avg_sent=float(e["avg_sent"]) if pd.notna(e.get("avg_sent")) else None,
            avg_hate=float(e["avg_hate"]) if pd.notna(e.get("avg_hate")) else None,
            avg_spam=float(e["avg_spam"]) if pd.notna(e.get("avg_spam")) else None,
            example_text=safe_txt(e.get("example_text")),
            ts=str(e.get("ts")) if pd.notna(e.get("ts")) else None,
            __source_table=e.get("__source_table"),
        )

    # === Tulis HTML dari PyVis ===
    html = nt.generate_html(notebook=False)

    # --- CSS: wrap tooltip text ---
    styles = """
<style>
  .vis-tooltip{
    white-space: pre-wrap;   /* hormati \\n dan bungkus baris panjang */
    word-break: break-word;  /* pecah kata super panjang */
    max-width: 560px;
    line-height: 1.3;
  }
</style>
"""
    if "</head>" in html:
        html = html.replace("</head>", styles + "</head>")
    else:
        html = styles + html

    # === Tulis HTML dari PyVis ===
    html = nt.generate_html(notebook=False)

    with open(OUT_HTML, "w", encoding="utf-8") as f:
        f.write(html)
    return OUT_HTML

# =========================
# Main
# =========================
if __name__ == "__main__":
    with ENGINE.connect() as con:
        ver = con.execute(text("SELECT version()")).scalar()
        who = con.execute(text("SELECT current_user")).scalar()
        db  = con.execute(text("SELECT current_database()")).scalar()
        sch = con.execute(text("SHOW search_path")).scalar()
        print("✅ Connected"); print("Version:", ver)
        print("User:", who, "| DB:", db, "| search_path:", sch)

    print("Load nodes, features, edges …")
    nodes, feats, edges = load_nodes_labels_features_edges()
    print(f"Nodes: {len(nodes):,} | Edges (raw): {len(edges):,}")

    out = build_network(
        nodes=nodes, feats=feats, edges=edges,
        min_weight=0.0, max_edges=20000, physics=True, seed=42
    )
    print(f"✅ HTML graph tersimpan di: {out}")