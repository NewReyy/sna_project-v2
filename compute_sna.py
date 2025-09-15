# compute_sna.py
import os, logging, math, time
import pandas as pd
import networkx as nx
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from dotenv import load_dotenv

load_dotenv(override=True)

log = logging.getLogger("sna")
logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")

HOST = os.getenv("PG_HOST", "localhost")
PORT = os.getenv("PG_PORT", "5432")
USER = os.getenv("PG_USER", "postgres")
PWD  = os.getenv("PG_PASSWORD", "")
SOCIALENS_CLONE = os.getenv("SOCIALENS_CLONE", "socialens_clone_db")
DB = os.getenv("SNA_DB", "sna")

URL_DB = URL.create("postgresql+psycopg2", username=USER, password=PWD, host=HOST, port=PORT, database=DB)
engine = create_engine(URL_DB, pool_pre_ping=True)

BATCH_WINDOW_HOURS = int(os.getenv("BATCH_WINDOW_HOURS", "24"))
ALPHA = 0.85

def _regclass_exists(c, regname: str) -> bool:
    return bool(c.execute(text("SELECT to_regclass(:rname) IS NOT NULL"), {"rname": regname}).scalar())

def _col_exists(c, table: str, col: str) -> bool:
    return bool(c.execute(text("""
        SELECT 1 FROM information_schema.columns
        WHERE table_schema='sna' AND table_name=:t AND column_name=:c
    """), {"t": table, "c": col}).fetchone())

def _table_exists(c, regname: str) -> bool:
    return bool(c.execute(text("SELECT to_regclass(:rname)"), {"rname": regname}).scalar())

def ensure_edges_agg():
    with engine.begin() as c:
        c.execute(text("CREATE SCHEMA IF NOT EXISTS sna;"))
        if _regclass_exists(c, "sna.edges_agg"):
            return
        has_mv_agg = _regclass_exists(c, "sna.edges_all_agg_mv")
        has_mv_all = _regclass_exists(c, "sna.edges_all_mv")
        if not (has_mv_agg or has_mv_all):
            raise RuntimeError("Butuh MV sumber: sna.edges_all_agg_mv atau sna.edges_all_mv.")
        if has_mv_agg:
            c.execute(text("""
                CREATE TABLE IF NOT EXISTS sna.edges_agg AS
                SELECT source_id, target_id, weight, ts_last FROM sna.edges_all_agg_mv;
            """))
        else:
            c.execute(text("""
                CREATE TABLE IF NOT EXISTS sna.edges_agg AS
                SELECT source_id, target_id, SUM(weight) AS weight, MAX(ts) AS ts_last
                FROM sna.edges_all_mv GROUP BY 1,2;
            """))
        c.execute(text("""
            CREATE UNIQUE INDEX IF NOT EXISTS ux_edges_agg_src_tgt ON sna.edges_agg (source_id, target_id);
            CREATE INDEX IF NOT EXISTS ix_edges_agg_tgt ON sna.edges_agg (target_id);
        """))
        log.info("Initialized sna.edges_agg")

def load_edges_for_metrics(window_days: int = 180) -> pd.DataFrame:
    """
    Ambil edge untuk metrik:
    1) Prefer dari sna.sna_edges_agg (hasil run_sna.py yang baru).
    2) Fallback: union dari edge_events + edges_instagram_comment_to_author + edges_tiktok_reply_to_user
       + edges_all_agg_mv (atau edges_all_mv), lalu agregasi SUM(weight), MAX(ts).
    Hasil: source_id, target_id, weight, ts
    """
    # 1) Prefer tabel agregat hasil run_sna.py
    with engine.connect() as c:
        if _table_exists(c, "sna.sna_edges_agg"):
            df = pd.read_sql(text(f"""
                SELECT source_id::bigint AS source_id,
                       target_id::bigint AS target_id,
                       COALESCE(weight,1.0)::float AS weight,
                       last_ts AS ts
                FROM sna.sna_edges_agg
                WHERE last_ts > NOW() - INTERVAL '{int(window_days)} days'
            """), c)
            if not df.empty:
                return df

    # 2) Fallback: union seperti visual
    dfs = []
    ts_candidates = ["example_ts","ts","ts_last","created_at","event_ts","event_time","time","timestamp"]

    with engine.connect() as c:
        # Event-like tables
        for rel in ["edge_events", "edges_instagram_comment_to_author", "edges_tiktok_reply_to_user"]:
            if not _table_exists(c, f"sna.{rel}"):
                continue

            # deteksi kolom ts & weight
            ts_col = next((cand for cand in ts_candidates if _col_exists(c, rel, cand)), None)
            w_exists = _col_exists(c, rel, "weight")

            ts_expr = ts_col if ts_col else "NULL::timestamptz"
            w_expr  = "COALESCE(weight,1.0)::float AS weight" if w_exists else "1.0::float AS weight"
            where_ts = f"AND {ts_expr} > NOW() - INTERVAL '{int(window_days)} days'" if ts_col else ""

            q = f"""
                SELECT
                  source_id::bigint AS source_id,
                  target_id::bigint AS target_id,
                  {w_expr},
                  {ts_expr} AS ts
                FROM sna.{rel}
                WHERE source_id IS NOT NULL AND target_id IS NOT NULL
                  {where_ts}
            """
            dfs.append(pd.read_sql(text(q), c))

        # Aggregated table (prioritas edges_all_agg_mv, fallback edges_all_mv)
        agg_rel = "edges_all_agg_mv" if _table_exists(c, "sna.edges_all_agg_mv") else (
                  "edges_all_mv"     if _table_exists(c, "sna.edges_all_mv")     else None)
        if agg_rel:
            ts_col = next((cand for cand in ts_candidates if _col_exists(c, agg_rel, cand)), None)
            w_exists = _col_exists(c, agg_rel, "weight")

            ts_expr = ts_col if ts_col else "NULL::timestamptz"
            w_expr  = "COALESCE(weight,1.0)::float AS weight" if w_exists else "1.0::float AS weight"
            where_ts = f"AND {ts_expr} > NOW() - INTERVAL '{int(window_days)} days'" if ts_col else ""

            q = f"""
                SELECT
                  source_id::bigint AS source_id,
                  target_id::bigint AS target_id,
                  {w_expr},
                  {ts_expr} AS ts
                FROM sna.{agg_rel}
                WHERE source_id IS NOT NULL AND target_id IS NOT NULL
                  {where_ts}
            """
            dfs.append(pd.read_sql(text(q), c))

    dfs = [d for d in dfs if isinstance(d, pd.DataFrame) and not d.empty]
    if not dfs:
        return pd.DataFrame(columns=["source_id","target_id","weight","ts"])

    df = pd.concat(dfs, ignore_index=True)

    # normalisasi
    for col in ("source_id","target_id"):
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    df["weight"] = pd.to_numeric(df["weight"], errors="coerce").fillna(1.0)
    df["ts"]     = pd.to_datetime(df["ts"], errors="coerce")

    df = df.dropna(subset=["source_id","target_id"])
    df = df[df["source_id"] != df["target_id"]]

    # agregasi final per pasangan
    df = (df.groupby(["source_id","target_id"], as_index=False)
            .agg(weight=("weight","sum"), ts=("ts","max")))
    return df

def load_prev_pagerank():
    try:
        with engine.connect() as c:
            if not _regclass_exists(c, "sna.sna_metrics"):
                return {}
        df = pd.read_sql(text("SELECT node_id, pagerank FROM sna.sna_metrics"), engine)
        return {int(r.node_id): float(r.pagerank) for _, r in df.iterrows()}
    except Exception:
        return {}

def load_all_node_ids() -> pd.Series:
    with engine.connect() as c:
        df = pd.read_sql(text("SELECT node_id::bigint AS node_id FROM sna.node_dim"), c)
    return pd.to_numeric(df["node_id"], errors="coerce").dropna().astype("int64")

def compute_metrics(df_edges, prev_pr=None, all_nodes=None):
    """
    Hitung metrik graf dan SELALU mengembalikan tuple (G, df_nodes, df_edges2).
    df_edges: DataFrame [source_id, target_id, weight, ts]
    prev_pr : dict node->pagerank sebelumnya (opsional)
    all_nodes: iterable/Series node_id dari node_dim untuk inject isolat (opsional)
    """
    # --- Cleaning wajib ---
    if df_edges is None or df_edges.empty:
        # kembalikan objek kosong yang aman
        G_empty = nx.DiGraph()
        df_nodes_empty = pd.DataFrame(columns=[
            "node_id","degree","in_degree","out_degree",
            "pagerank","betweenness","closeness","clustering","community","updated_at"
        ])
        df_edges_empty = pd.DataFrame(columns=["source_id","target_id","last_ts","weight"])
        return G_empty, df_nodes_empty, df_edges_empty

    df_edges = df_edges.copy()
    df_edges = df_edges.dropna(subset=["source_id", "target_id"])
    # bobot positif saja
    if "weight" not in df_edges.columns:
        df_edges["weight"] = 1.0
    df_edges["weight"] = pd.to_numeric(df_edges["weight"], errors="coerce").fillna(0.0)
    df_edges = df_edges[df_edges["weight"] > 0]
    # normalisasi ts
    if "ts" in df_edges.columns:
        df_edges["ts"] = pd.to_datetime(df_edges["ts"], errors="coerce")
    else:
        df_edges["ts"] = pd.NaT

    # --- Build graph ---
    G = nx.from_pandas_edgelist(
        df_edges, "source_id", "target_id",
        edge_attr=["weight", "ts"], create_using=nx.DiGraph()
    )

    # inject isolat dari node_dim agar jumlah node selaras dgn visual
    if all_nodes is not None:
        try:
            all_nodes_list = pd.Series(all_nodes).dropna().astype("int64").tolist()
        except Exception:
            all_nodes_list = list(all_nodes)
        if all_nodes_list:
            G.add_nodes_from(all_nodes_list)

    # jika tetap kosong, kembalikan frame kosong yang aman
    if G.number_of_nodes() == 0:
        df_nodes_empty = pd.DataFrame(columns=[
            "node_id","degree","in_degree","out_degree",
            "pagerank","betweenness","closeness","clustering","community","updated_at"
        ])
        df_edges_empty = pd.DataFrame(columns=["source_id","target_id","last_ts","weight"])
        return G, df_nodes_empty, df_edges_empty

    # --- Komponen lemah untuk PR yang stabil ---
    comps = [G.subgraph(c).copy() for c in nx.weakly_connected_components(G)]

    def safe_pr(H: nx.DiGraph):
        n = H.number_of_nodes()
        if n == 0:
            return {}
        # nstart dari prev_pr bila ada
        if prev_pr:
            nstart = {node: float(prev_pr.get(node, 0.0)) for node in H.nodes()}
            s = sum(nstart.values())
            nstart = {k: (v / s if s > 0 else 1.0 / n) for k, v in nstart.items()}
        else:
            nstart = {node: 1.0 / n for node in H.nodes()}
        dangling = {node: 1.0 / n for node in H.nodes()}
        try:
            return nx.pagerank(H, alpha=ALPHA, weight="weight",
                               max_iter=1000, tol=1e-8,
                               nstart=nstart, dangling=dangling)
        except nx.PowerIterationFailedConvergence:
            return nx.pagerank_numpy(H, alpha=ALPHA, weight="weight")

    # --- Centralities ---
    pr = {}
    for H in comps:
        pr.update(safe_pr(H))

    in_deg  = dict(G.in_degree(weight="weight"))
    out_deg = dict(G.out_degree(weight="weight"))
    deg     = dict(G.degree(weight="weight"))

    # Betweenness sampling untuk efisiensi
    try:
        k_sample = max(1, min(500, G.number_of_nodes()))
        btw = nx.betweenness_centrality(G, k=k_sample, weight="weight", normalized=True, seed=42)
    except Exception:
        btw = {n: 0.0 for n in G.nodes()}

    # Closeness & clustering di graf tak-terarah (jarak = 1/weight)
    try:
        H_und = G.to_undirected()
        for u, v, d in H_und.edges(data=True):
            w = float(d.get("weight", 1.0))
            d["dist"] = 1.0 / max(w, 1e-12)
        clo = nx.closeness_centrality(H_und, distance="dist")
    except Exception:
        clo = {n: 0.0 for n in G.nodes()}

    try:
        clu = nx.clustering(G.to_undirected(), weight="weight")
    except Exception:
        clu = {n: 0.0 for n in G.nodes()}

    # Komunitas (optional – jika lib tidak ada, isi None)
    try:
        from community import community_louvain  # pip install python-louvain
        und = G.to_undirected()
        comm_map = community_louvain.best_partition(und, weight="weight", random_state=42)
    except Exception:
        comm_map = {n: None for n in G.nodes()}

    # --- Dataframe nodes ---
    df_nodes = (
        pd.DataFrame({"node_id": list(G.nodes())})
        .assign(
            degree=lambda d: d.node_id.map(deg).fillna(0.0),
            in_degree=lambda d: d.node_id.map(in_deg).fillna(0.0),
            out_degree=lambda d: d.node_id.map(out_deg).fillna(0.0),
            pagerank=lambda d: d.node_id.map(pr).fillna(0.0),
            betweenness=lambda d: d.node_id.map(btw).fillna(0.0),
            closeness=lambda d: d.node_id.map(clo).fillna(0.0),
            clustering=lambda d: d.node_id.map(clu).fillna(0.0),
            community=lambda d: d.node_id.map(comm_map),
            updated_at=pd.Timestamp.utcnow(),
        )
    )

    # --- Dataframe edges snapshot ---
    df_edges2 = df_edges.rename(columns={"ts": "last_ts"})[
        ["source_id","target_id","weight","last_ts"]
    ].copy()

    # Pastikan tipe aman
    for col in ("source_id","target_id"):
        df_edges2[col] = pd.to_numeric(df_edges2[col], errors="coerce").astype("Int64")
    df_edges2["weight"]  = pd.to_numeric(df_edges2["weight"], errors="coerce").fillna(1.0)
    df_edges2["last_ts"] = pd.to_datetime(df_edges2["last_ts"], errors="coerce")

    # === RETURN SELALU ADA ===
    return G, df_nodes, df_edges2

def persist(df_nodes, df_edges):
    with engine.begin() as c:
        # sna_metrics → truncate dulu, lalu insert
        c.execute(text("TRUNCATE TABLE sna.sna_metrics;"))
        df_nodes.to_sql("sna_metrics", c, schema="sna",
                        if_exists="append", index=False)

        # sna_edges_agg_snapshot → bisa aman replace (karena tidak dipakai MV lain)
        df_edges.to_sql("sna_edges_agg_snapshot", c, schema="sna",
                        if_exists="replace", index=False)

if __name__ == "__main__":
    t0 = time.time()
    # (opsional) biarkan ensure_edges_agg kalau kamu masih pakai edges_agg lama
    try:
        ensure_edges_agg()
    except Exception:
        pass

    log.info("Loading edges for metrics (aligned with viz)…")
    df = load_edges_for_metrics(window_days=180)
    if df.empty:
        log.warning("Tidak ada edge (dari sna.sna_edges_agg maupun fallback union). Exit.")
        raise SystemExit(0)

    all_nodes = load_all_node_ids()

    prev = load_prev_pagerank()

    log.info("Computing metrics…")
    G, nodes, edges = compute_metrics(df, prev_pr=prev, all_nodes=all_nodes)

    if nodes.empty:
        log.warning("Graf kosong. Tidak ada metrik untuk disimpan.")
        raise SystemExit(0)

    log.info("Persisting…")
    persist(nodes, edges)

    log.info("Done. Nodes=%d Edges=%d Elapsed=%.1fs",
             G.number_of_nodes(), G.number_of_edges(), time.time() - t0)
