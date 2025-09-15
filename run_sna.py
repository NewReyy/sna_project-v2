# pip install pandas sqlalchemy psycopg2-binary networkx python-dotenv
import os
import pandas as pd
import networkx as nx
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from dotenv import load_dotenv

load_dotenv()

HOST   = os.getenv("PG_HOST", "127.0.0.1")
PORT   = os.getenv("PG_PORT", "5432")
USER   = os.getenv("PG_USER", "postgres")
PWD    = os.getenv("PG_PASSWORD", "")
DBNAME = os.getenv("SNA_DB", "sna")
SCHEMA = os.getenv("SNA_SCHEMA", "sna")

PWD_Q  = quote_plus(PWD)
DB_URL = f"postgresql+psycopg2://{USER}:{PWD_Q}@{HOST}:{PORT}/{DBNAME}"

engine = create_engine(
    DB_URL,
    pool_pre_ping=True,
    connect_args={"options": f"-c search_path={SCHEMA},public"},
)

# ----------------------------- Utils -----------------------------
def refresh_mv(name: str, try_concurrently: bool = True):
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as c:
        if try_concurrently:
            try:
                c.execute(text(f'REFRESH MATERIALIZED VIEW CONCURRENTLY "{SCHEMA}".{name}'))
                return
            except Exception:
                pass
        c.execute(text(f'REFRESH MATERIALIZED VIEW "{SCHEMA}".{name}'))

def safe_overwrite(conn, df: pd.DataFrame, table: str):
    tmp = f"{table}__tmp"
    df.to_sql(tmp, con=conn, schema=SCHEMA, if_exists="replace", index=False)
    conn.execute(text(f'''
        DO $$
        BEGIN
          IF NOT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = :schema AND table_name = :table
          ) THEN
            EXECUTE format('CREATE TABLE %I.%I AS TABLE %I.%I WITH NO DATA;',
                           :schema, :table, :schema, :tmp);
          END IF;
        END$$;
    '''), {"schema": SCHEMA, "table": table, "tmp": tmp})
    conn.execute(text(f'TRUNCATE TABLE "{SCHEMA}".{table}'))
    cols = ','.join([f'"{c}"' for c in df.columns])
    conn.execute(text(
        f'INSERT INTO "{SCHEMA}".{table} ({cols}) '
        f'SELECT {cols} FROM "{SCHEMA}".{tmp}'
    ))
    conn.execute(text(f'DROP TABLE "{SCHEMA}".{tmp}'))

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

# kandidat kolom timestamp
TS_CANDIDATES = ["ts_last", "example_ts", "ts", "created_at", "event_ts", "event_time", "time", "timestamp"]

def pick_ts_expr(cols: set) -> str:
    for c in TS_CANDIDATES:
        if c in cols:
            return c
    return "NULL::timestamptz"

# ------------------ Ambil edges (sinkron dgn visual) ------------------
def fetch_edges_union() -> pd.DataFrame:
    with engine.begin() as conn:
        # Pastikan MV “inti” sudah segar
        mv_order = [
            "edges_instagram_comment_to_author",
            "edges_tiktok_reply_to_user",
            "edge_events",
            "edges_agg_v2",
            "node_dim",
            "edges_all_agg_mv",  # prioritas utama utk set agregat
            "edges_all_mv",      # fallback kalau agg_mv tidak ada
        ]
        for mv in mv_order:
            if table_exists(conn, mv):
                refresh_mv(mv)

        edges_list = []

        # 1) Event tables (tiap baris = event) → weight default 1.0
        for rel in ["edge_events", "edges_instagram_comment_to_author", "edges_tiktok_reply_to_user"]:
            if not table_exists(conn, rel):
                continue
            cols = list_columns(conn, rel)
            ts_expr = pick_ts_expr(cols)
            weight_expr = "COALESCE(weight, 1.0)::float AS weight" if "weight" in cols else "1.0::float AS weight"
            q = f"""
                SELECT
                  source_id::bigint AS source_id,
                  target_id::bigint AS target_id,
                  {weight_expr},
                  {ts_expr} AS ts
                FROM "{SCHEMA}".{rel}
                WHERE source_id IS NOT NULL AND target_id IS NOT NULL
            """
            edges_list.append(pd.read_sql(text(q), conn))

        # 2) Satu dari edges_all_agg_mv (prioritas) ATAU edges_all_mv (fallback)
        agg_rel = "edges_all_agg_mv" if table_exists(conn, "edges_all_agg_mv") else ("edges_all_mv" if table_exists(conn, "edges_all_mv") else None)
        if agg_rel:
            cols = list_columns(conn, agg_rel)
            ts_expr = pick_ts_expr(cols)
            wcol = "weight" if "weight" in cols else None
            weight_expr = f"COALESCE({wcol}, 1.0)::float AS weight" if wcol else "1.0::float AS weight"
            q = f"""
                SELECT
                  source_id::bigint AS source_id,
                  target_id::bigint AS target_id,
                  {weight_expr},
                  {ts_expr} AS ts
                FROM "{SCHEMA}".{agg_rel}
                WHERE source_id IS NOT NULL AND target_id IS NOT NULL
            """
            edges_list.append(pd.read_sql(text(q), conn))

    edges_list = [df for df in edges_list if isinstance(df, pd.DataFrame) and not df.empty and df.shape[1] > 0]
    if not edges_list:
        return pd.DataFrame(columns=["source_id","target_id","weight","last_ts"])

    df = pd.concat(edges_list, ignore_index=True)

    # normalisasi tipe
    for col in ("source_id", "target_id"):
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    df["weight"] = pd.to_numeric(df["weight"], errors="coerce").fillna(1.0)
    df["ts"] = pd.to_datetime(df["ts"], errors="coerce")

    # buang self-loop & NaN
    df = df.dropna(subset=["source_id","target_id"])
    df = df[df["source_id"] != df["target_id"]]

    # agregasi: 1 baris per (src,dst), weight dijumlah, last_ts=max(ts)
    agg = (
        df.groupby(["source_id","target_id"], as_index=False)
          .agg(weight=("weight","sum"), last_ts=("ts","max"))
    )

    # tipe akhir
    agg["source_id"] = agg["source_id"].astype("Int64")
    agg["target_id"] = agg["target_id"].astype("Int64")
    agg["weight"]    = pd.to_numeric(agg["weight"], errors="coerce").fillna(1.0)
    return agg[["source_id","target_id","weight","last_ts"]]

# ----------------------------- Main -----------------------------
if __name__ == "__main__":
    # --- Ambil edges gabungan (sinkron dgn visual) ---
    edges_df = fetch_edges_union()

    if edges_df.empty:
        print("Tidak ada edge dari sumber manapun. Selesai tanpa menghitung metrik.")
        raise SystemExit(0)

    # --- Build graph terarah ---
    G = nx.from_pandas_edgelist(
        edges_df, "source_id", "target_id",
        edge_attr=["weight", "last_ts"], create_using=nx.DiGraph()
    )

    # --- Tambahkan semua node dari node_dim (agar count node sinkron dgn visual) ---
    with engine.begin() as conn:
        nodes_df = pd.read_sql(text(f'SELECT node_id::bigint AS node_id FROM "{SCHEMA}".node_dim'), conn)
    nodes_df["node_id"] = pd.to_numeric(nodes_df["node_id"], errors="coerce").astype("Int64")
    all_nodes = set(nodes_df["node_id"].dropna().astype("int64").tolist())
    G.add_nodes_from(all_nodes)   # menambah isolat (tanpa edge)

    if G.number_of_nodes() == 0 or G.number_of_edges() == 0:
        print("Graf kosong setelah penggabungan. Selesai tanpa menghitung metrik.")
        raise SystemExit(0)

    # --- Centralities ---
    pr  = nx.pagerank(G, weight="weight")
    # sampling untuk betweenness (hemat waktu, tapi tetap representatif)
    k_sample = max(1, min(500, G.number_of_nodes()))
    btw = nx.betweenness_centrality(G, k=k_sample, weight="weight", seed=42)

    # closeness & clustering pada graf tak-terarah dengan jarak = 1/weight
    H = G.to_undirected()
    for u, v, d in H.edges(data=True):
        w = float(d.get("weight", 1.0))
        d["dist"] = 1.0 / max(w, 1e-12)
    clo = nx.closeness_centrality(H, distance="dist")
    clu = nx.clustering(H, weight="weight")

    # komunitas (greedy modularity) di H
    communities = list(nx.algorithms.community.greedy_modularity_communities(H))
    node2comm = {n: cid for cid, com in enumerate(communities) for n in com}

    deg = {n: G.degree(n, weight="weight") for n in G.nodes()}

    # --- Metrics dataframe ---
    metrics = pd.DataFrame({"node_id": list(G.nodes())})
    metrics["degree"]      = metrics["node_id"].map(deg).fillna(0.0)
    metrics["pagerank"]    = metrics["node_id"].map(pr).fillna(0.0)
    metrics["betweenness"] = metrics["node_id"].map(btw).fillna(0.0)
    metrics["closeness"]   = metrics["node_id"].map(clo).fillna(0.0)
    metrics["clustering"]  = metrics["node_id"].map(clu).fillna(0.0)
    metrics["community"]   = metrics["node_id"].map(node2comm)
    metrics["community"]   = metrics["community"].astype("Int64")

    # --- Edges output (untuk tabel sna_edges_agg) ---
    edges_out = edges_df.rename(columns={"last_ts": "last_ts"}).copy()

    # --- Tulis ke DB TANPA DROP ---
    with engine.begin() as conn:
        safe_overwrite(conn, metrics, "sna_metrics")
        safe_overwrite(conn, edges_out, "sna_edges_agg")
        # Index (CREATE IF NOT EXISTS aman)
        conn.execute(text(f'CREATE INDEX IF NOT EXISTS sna_metrics_node_idx ON "{SCHEMA}".sna_metrics (node_id)'))
        conn.execute(text(f'CREATE INDEX IF NOT EXISTS sna_edges_agg_src_tgt_idx ON "{SCHEMA}".sna_edges_agg (source_id, target_id)'))
        conn.execute(text(f'CREATE INDEX IF NOT EXISTS sna_edges_agg_lastts_idx ON "{SCHEMA}".sna_edges_agg (last_ts)'))

    # node_features biasanya tergantung sna_metrics → refresh ulang
    refresh_mv("node_features", try_concurrently=False)

    print(f"Done. Nodes={G.number_of_nodes()} Edges={G.number_of_edges()}")