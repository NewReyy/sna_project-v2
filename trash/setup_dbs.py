import os, gzip
from pathlib import Path
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import subprocess, shutil
from sqlalchemy.engine import URL

load_dotenv()

HOST = os.getenv("PG_HOST", "localhost")
PORT = os.getenv("PG_PORT", "5432")
USER = os.getenv("PG_USER", "postgre")
PWD  = os.getenv("PG_PASSWORD", "")
SOCIALENS_CLONE = os.getenv("SOCIALENS_CLONE", "socialens_clone_db")
SNA_DB = os.getenv("SNA_DB", "sna")
SQL_FILE = Path(os.getenv("SQL_FILE", ""))

ADMIN_URL = f"postgresql+psycopg2://{USER}:{PWD}@{HOST}:{PORT}/postgres"
socialens_url = f"postgresql+psycopg2://{USER}:{PWD}@{HOST}:{PORT}/{SOCIALENS_CLONE}"
sna_url = f"postgresql+psycopg2://{USER}:{PWD}@{HOST}:{PORT}/{SNA_DB}"

def create_db(dbname: str):
    eng = create_engine(ADMIN_URL, isolation_level="AUTOCOMMIT")
    with eng.begin() as c:
        exists = c.execute(text("SELECT 1 FROM pg_database WHERE datname=:d"), {"d": dbname}).scalar()
        if not exists:
            c.execute(text(f'CREATE DATABASE "{dbname}"'))
            print(f"[OK] Created DB {dbname}")
        else:
            print(f"[SKIP] DB {dbname} already exists")

def load_sql_file(db_url: str, sql_path: Path):
    if not sql_path.exists():
        print(f"[WARN] SQL file not found: {sql_path}")
        return

    # DETEKSI dump format
    with open(sql_path, "rb") as f:
        sig = f.read(5)
    is_pgdump = sig.startswith(b"PGDMP") or sql_path.suffix.lower() in {".dump", ".backup"}

    from sqlalchemy.engine import URL
    url = URL.create("postgresql+psycopg2",
                     username=USER, password=PWD,
                     host=HOST, port=int(PORT), database=SOCIALENS_CLONE)

    if is_pgdump:
        print(f"[INFO] Detected pg_dump custom format. Using pg_restore for {sql_path}")
        import subprocess, shutil
        pg_restore = shutil.which("pg_restore") or r"C:\Program Files\PostgreSQL\17\bin\pg_restore.exe"
        env = os.environ.copy()
        env["PGPASSWORD"] = PWD

        # (opsional) bersihkan schema public agar bersih
        eng = create_engine(url)
        with eng.begin() as c:
            c.execute(text("DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;"))

        cmd = [
            pg_restore,
            "--no-owner", "--no-privileges",
            "--if-exists", "--clean",      # aman saat DROP di DB kosong/baru
            "--schema=public",
            "-h", HOST, "-p", str(PORT), "-U", USER,
            "-d", SOCIALENS_CLONE,
            str(sql_path)
        ]
        print("[CMD]", " ".join(cmd))
        res = subprocess.run(cmd, env=env)
        if res.returncode != 0:
            print(f"[WARN] pg_restore exited with code {res.returncode}. Verifying objects...")

        # verifikasi tabel kunci ada
        must_have = [
            "instagram_posts", "instagram_comments",
            # kalau dataset TikTok memang ada, biarkan; kalau tidak, boleh dihapus:
            "tiktok_posts", "tiktok_comments", "tiktok_comments_reply"
        ]
        eng = create_engine(url)
        with eng.connect() as c:
            res = c.execute(text("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema='public' AND table_name = ANY(:names)
            """), {"names": must_have})
            present = {row[0] for row in res}  # <- set nama tabel
        missing = [t for t in must_have if t not in present]
        if missing:
            print(f"[WARN] Beberapa tabel kunci tidak ditemukan: {missing}")
        else:
            print("[OK] Restore completed & verified.")

    else:
        # Plain .sql / .sql.gz
        print(f"[INFO] Loading plain SQL into SOCIALENS_CLONE from {sql_path}")
        import gzip
        if sql_path.suffix == ".gz":
            with gzip.open(sql_path, "rt", encoding="utf-8", errors="ignore") as f:
                sql_text = f.read()
        else:
            with open(sql_path, "r", encoding="utf-8", errors="ignore") as f:
                sql_text = f.read()
        lines = []
        for ln in sql_text.splitlines():
            if ln.strip().lower().startswith("create database") or ln.strip().lower().startswith("\\connect"):
                continue
            lines.append(ln)
        sql_text = "\n".join(lines)
        eng = create_engine(url)
        with eng.begin() as c:
            for stmt in [s.strip() for s in sql_text.split(";") if s.strip()]:
                c.execute(text(stmt))
        print(f"[OK] SQL loaded into SOCIALENS_CLONE")

def setup_fdw_and_views():
    eng = create_engine(sna_url)
    with eng.begin() as c:
        c.execute(text("CREATE SCHEMA IF NOT EXISTS sna;"))
        c.execute(text("CREATE EXTENSION IF NOT EXISTS postgres_fdw;"))
        c.execute(text(f"""
            CREATE SERVER IF NOT EXISTS socialens_fdw
            FOREIGN DATA WRAPPER postgres_fdw
            OPTIONS (host '{HOST}', dbname '{SOCIALENS_CLONE}', port '{PORT}');
        """))
        c.execute(text(f"""
            CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER
            SERVER socialens_fdw OPTIONS (user '{USER}', password '{PWD}');
        """))
        c.execute(text("""
            IMPORT FOREIGN SCHEMA public LIMIT TO
              (instagram_posts, instagram_comments, tiktok_posts, tiktok_comments, tiktok_comments_reply)
            FROM SERVER socialens_fdw INTO public;
        """))
        # Materialized views (Instagram + TikTok)
        c.execute(text("""
        CREATE MATERIALIZED VIEW IF NOT EXISTS sna.edges_instagram_comment_to_author AS
        SELECT c.author_id AS source_id, p.author_id AS target_id,
               0.7::float8 AS weight, c.created_at AS ts,
               'instagram' AS platform, 'comment_to_author' AS interaction
        FROM public.instagram_comments c
        JOIN public.instagram_posts p ON p.id = c.post_id
        WHERE c.author_id IS NOT NULL AND p.author_id IS NOT NULL
        WITH NO DATA;
        """))
        c.execute(text("""
        CREATE MATERIALIZED VIEW IF NOT EXISTS sna.edges_tiktok_reply_to_user AS
        SELECT
        r.author_id AS source_id,
        c.author_id AS target_id,
        0.9::float8 AS weight,
        c.created_at AS ts,        -- pakai timestamp dari parent comment
        'tiktok' AS platform,
        'reply_to_user' AS interaction
        FROM public.tiktok_comments_reply r
        JOIN public.tiktok_comments c ON c.id = r.tiktok_comment_id
        WHERE r.author_id IS NOT NULL AND c.author_id IS NOT NULL
        WITH NO DATA;
        """))
        c.execute(text("""
        CREATE MATERIALIZED VIEW IF NOT EXISTS sna.edges_all_mv AS
        SELECT * FROM sna.edges_instagram_comment_to_author
        UNION ALL
        SELECT * FROM sna.edges_tiktok_reply_to_user
        WITH NO DATA;
        """))
        c.execute(text("""
        CREATE MATERIALIZED VIEW IF NOT EXISTS sna.edges_all_agg_mv AS
        SELECT source_id, target_id, SUM(weight) AS weight, MAX(ts) AS ts_last
        FROM sna.edges_all_mv
        GROUP BY source_id, target_id
        WITH NO DATA;
        """))
        print("[OK] Views created in 'sna' Database")

if __name__ == "__main__":
    create_db(SOCIALENS_CLONE)
    if SQL_FILE:
        load_sql_file(socialens_url, SQL_FILE)
    create_db(SNA_DB)
    setup_fdw_and_views()
    print("[DONE] setup complete")
