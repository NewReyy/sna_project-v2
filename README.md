# Social Network Analysis Project (Postgres + Python + NetworkX)

### Step New
1. Run SQL Dump nya
2. Run Query [ini](<Database/Query MV.sql>) setelah run MV baru refresh MV dengan Query [ini](<Database/Query Update Refresh MV.sql>)
3. Jalankan kode [ini](visualization.py) ("Lama banget 3 jam an belom selesai")


# 📘 Flow Dokumentasi: Social Network Analysis dari Database → PyVis

## 🧭 FLOW (Step‑by‑step)

### 0) Awal Goals
- Menyajikan visualisasi jaringan interaktif lintas platform dengan fokus pada hubungan **user -> post -> project**.
- Node: `project`, `post`, `user`. Edge: `comment`, `reply/mention/share/retweet`.
- Atribut analitik di edge: **sentiment**, **emotion**, **hate speech**, **spam**, **text**, **timestamp**. Untuk pewarnaan edges, atau konektornya.

### 1) Sumber Awal Data
- Tabel: _social_posts_, _projects_, tabel interaksi per platform (Instagram/TikTok), serta edges analitik yakni kolom (sentiment/emotion/hate/spam).

### 2) Normalisasi ID atau Penggabungan
- Menetapkan format **ID konsisten** untuk seluruh node:
  - `project:{project_id}`
  - `post:{platform}:{post_id}`
  - `user:{platform}:{user_id}`
- Bertujuan agar relasi antar entitas lintas tabel bisa disatukan tanpa ambiguitas.

### 3) Normalisasi Post Antar Platform
- Ambil seluruh post dari _social_posts_ dan kaitkan dengan _projects_ agar mendapatkan `project_name`.
- Hasil normalisasi menghasilkan tabel ringkas (logis) dengan kolom: `platform, project_id, project_name, post_id, content_created_at`.
- Data inilah yang menjadi **landasan** untuk menghubungkan komentar/reply ke post yang benar.

### 4) Perancangan Edge Interaksi
- Definisikan hubungan:
  - **Comment**: `user → post` (pengguna mengomentari post).
  - **Reply to user**: `user → user` (balasan pada komentar pengguna lain).
  - **Share/Retweet/Mention**
- Setiap edge itu nanti membawa **data** minimum adalah: `platform, interaction, project_id, project_name, post_id, text, timestamp`.

### 5) Integrasi Atribut Analitik
- Satukan hasil deteksi ke level **edge** (komentar/balasan):
  - *Sentiment*: label & skor (0–1 atau −1..1 yang dinormalisasi ke 0..1 untuk konsistensi tampilan).
  - *Emotion*: label & skor.
  - *Hate speech*: percentase text tersebut hatespeech atau bukan menggunakan kolom is_hate_speech.
  - *Spam*: label/score.

### 6) Buat MV
- Bangun MV:
  1. **Posts** (semua platform) → landasan post.
  2. **Edges per platform** (comment/reply/… ) → menempelkan analitik yang relevan.
  3. **Edges gabungan** → menyatukan semua edge lintas platform.
  4. **Node dimensi** → menurunkan daftar node unik (project/post/user) dari hasil posts & edges.
  5. **Node features** → ringkasan derajat/rata‑rata skor, untuk tooltip/analisis cepat.
- query visualisasi menjadi cepat dan konsisten.

### 7) Indeks & Kinerja
- Buat indeks pada kolom yang sering difilter/dibergabungkan: `(platform, post_id)`, `project_id`, `created_at`.
- Bertujuan untuk mempercepat refresh & pembacaan data oleh layer visualisasi.

### 8) Strategi Refresh & Terjadwal (Cron)
- Urutan refresh ketika ada data baru: Posts → Edge per platform → Edges gabungan → Node dimensi → Node features.

### 9) Bangun Graf
- Bentuk graf terarah (**directed**) untuk menangkap arah interaksi (`user → post`).
- Node menyimpan atribut tampilan: label ringkas (ikon, platform, nama project), tipe node, dan info pendukung.
- Edge menyimpan atribut analitik (sentiment/emotion/hate/spam), teks, waktu, serta jenis interaksi.

### 10) Aturan Styling
- **Warna edge** dengan prioritas:
  1) Hate → **merah cerah**
  2) Spam → **oranye**
  3) Sentiment (0..1) → **hijau** (≥0,66), **kuning** (≥0,33), **merah gelap** (<0,33)
  4) Jika tidak ada analitik → fallback berdasarkan jenis interaksi (comment/reply/share/mention).
- **Warna node**:
  - Post mewarisi warna platform (Instagram/TikTok/Twitter/Facebook/Youtube).
  - Project/User menggunakan palet netral berbeda agar mudah dibedakan.
- **Ketebalan edge** dapat dipengaruhi besaran skor (hate/spam/sentiment) untuk menonjolkan interaksi penting.
- **Tooltip** menampilkan metadata ringkas: project, platform, post_id, waktu, teks, dan ringkasan analitik.

### 11) Layout & Fisika
- Menggunakan **spring layout** untuk menyusun posisi node berdasarkan gaya pegas sehingga komunitas/cluster lebih terlihat.
- Kunci posisi hasil layout saat diekspor agar tidak terus berubah ketika HTML dirender.

### 12) Ekspor
- Hasilkan **file HTML** interaktif (PyVis) yang menggabungkan canvas jaringan, legend, dan tooltip.
- Pastikan encoding yang kompatibel antar OS agar tidak ada karakter rusak.
- Distribusi:
  - Simpan sebagai artefak statis (server web, storage objek).
  - **Embed** di portal internal/dashboard via iframe.

---

> **Tujuan**: Membangun visualisasi jaringan interaktif (PyVis HTML) berbasis data sosial (post & interaksi) lintas platform, dengan node **project / post / user** dan edge **comment / reply / share / mention**. Warna edge memprioritaskan **Hate (merah cerah) → Spam (oranye) → Sentiment (hijau/kuning/merah gelap)**, lalu fallback ke jenis interaksi.

---

## 🔎 Gambaran Besar (End-to-End)
```
[Sumber Data DB]
  ├─ social_posts (post lintas platform)
  ├─ projects (nama project)
  ├─ instagram_comments, tiktok_comments, tiktok_comments_reply
  ├─ sentiment_instagrams, hate_speech_instagram, spam_instagrams, emotion_tiktoks
      ↓ (Materialized Views)
  posts_all_mv  →  edge_*_mv (platform-spesifik)  →  edges_all_mv
      ↓
  node_dim_mv  (+ node_features_mv)
      ↓
[Python]
  Load nodes & edges  →  Build NetworkX DiGraph →  Layout spring →  Export PyVis HTML
```

---

## ✅ Prasyarat
- **PostgreSQL** (v13+)
- **Python 3.9+** dan paket: `sqlalchemy`, `psycopg2-binary`, `pandas`, `networkx`, `pyvis`, `python-dotenv`
- Akses DB dengan tabel:
  - `social_posts`, `projects`
  - `instagram_comments`, `tiktok_comments`, `tiktok_comments_reply`
  - `sentiment_instagrams`, `hate_speech_instagram`, `spam_instagrams`, `emotion_tiktoks`

---

## 🧱 Desain Data
### Node ID (unik & konsisten)
- **Project**: `project:{project_id}`
- **Post**   : `post:{platform}:{post_id}`
- **User**   : `user:{platform}:{user_id}`

### Kolom utama yang dipakai
- **Post**: `project_id`, `post_id`, `platform`, `project_name`
- **Edge**: `source_node_id`, `target_node_id`, `interaction`, `platform`, `text_raw`, `created_at`
- **Analitik**: `sentiment_label/sentiment_score`, `emotion_label/emotion_score`, `hs_is_hate/hs_score/hs_percentage`, `spam_label/spam_score`

---

## 🧩 Materialized Views (MV)
> MV membuat pengambilan data visualisasi lebih cepat & stabil. **Urutan pembuatan penting**.

### 1) `posts_all_mv` – Normalisasi semua post lintas platform
Mengambil `project_id`, `project_name`, dan `post_id` per platform dari `social_posts` + `projects`.

```sql
CREATE SCHEMA IF NOT EXISTS sna;

DROP MATERIALIZED VIEW IF EXISTS sna.posts_all_mv;
CREATE MATERIALIZED VIEW sna.posts_all_mv AS
WITH base AS (
  SELECT sp.id AS social_post_pk,
         sp.project_id,
         p.name AS project_name,
         sp.content_created_at,
         sp.source,
         sp.instagram_post_id,
         sp.tiktok_post_id,
         sp.twitter_post_id,
         sp.facebook_post_id
  FROM social_posts sp
  JOIN projects p ON p.id = sp.project_id
)
SELECT 'instagram'::text AS platform, project_id::bigint, project_name::text,
       instagram_post_id::text AS post_id, social_post_pk::bigint, content_created_at::timestamp
FROM base WHERE instagram_post_id IS NOT NULL
UNION ALL
SELECT 'tiktok', project_id::bigint, project_name::text,
       tiktok_post_id::text, social_post_pk::bigint, content_created_at::timestamp
FROM base WHERE tiktok_post_id IS NOT NULL
UNION ALL
SELECT 'twitter', project_id::bigint, project_name::text,
       twitter_post_id::text, social_post_pk::bigint, content_created_at::timestamp
FROM base WHERE twitter_post_id IS NOT NULL
UNION ALL
SELECT 'facebook', project_id::bigint, project_name::text,
       facebook_post_id::text, social_post_pk::bigint, content_created_at::timestamp
FROM base WHERE facebook_post_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_posts_all_platform_post
  ON sna.posts_all_mv(platform, post_id);
CREATE INDEX IF NOT EXISTS idx_posts_all_project
  ON sna.posts_all_mv(project_id);
```

### 2) `edge_instagram_comment_mv` – Comment IG (user → post) + sentiment/hate/spam
> **ENUM mismatch fix**: cast `si.sentiment::text` agar seragam saat UNION.

```sql
DROP MATERIALIZED VIEW IF EXISTS sna.edge_instagram_comment_mv;
CREATE MATERIALIZED VIEW sna.edge_instagram_comment_mv AS
SELECT
  'instagram'::text                                   AS platform,
  'comment'::text                                     AS interaction,
  ('user:instagram:'||ic.author_id::text)::text       AS source_node_id,
  ('post:instagram:'||ic.post_id::text)::text         AS target_node_id,
  pam.project_id::bigint                              AS project_id,
  pam.project_name::text                              AS project_name,
  ic.post_id::text                                    AS post_id,
  ic.id::bigint                                       AS comment_id,
  ic.created_at::timestamp                            AS created_at,
  ic.text::text                                       AS text_raw,
  si.sentiment::text                                  AS sentiment_label,
  si.score::numeric                                   AS sentiment_score,
  NULL::text                                          AS emotion_label,
  NULL::numeric                                       AS emotion_score,
  hsi.is_hate_speech::boolean                         AS hs_is_hate,
  hsi.hs_score::numeric                               AS hs_score,
  hsi.hate_perentage::numeric                         AS hs_percentage,
  sgi.label::text                                     AS spam_label,
  sgi.score::numeric                                  AS spam_score
FROM instagram_comments ic
JOIN sna.posts_all_mv pam
  ON pam.platform = 'instagram' AND pam.post_id = ic.post_id::text
LEFT JOIN sentiment_instagrams si
  ON si.instagram_comment_id = ic.id
LEFT JOIN hate_speech_instagram hsi
  ON hsi.instagram_comment_id = ic.id
LEFT JOIN spam_instagrams sgi
  ON sgi.instagram_comment_id = ic.id;

CREATE INDEX IF NOT EXISTS idx_edge_ig_target ON sna.edge_instagram_comment_mv(target_node_id);
```

### 3) `edge_tiktok_comment_mv` – Comment TikTok (user → post) + emotion
```sql
DROP MATERIALIZED VIEW IF EXISTS sna.edge_tiktok_comment_mv;
CREATE MATERIALIZED VIEW sna.edge_tiktok_comment_mv AS
SELECT
  'tiktok'::text                                      AS platform,
  'comment'::text                                     AS interaction,
  ('user:tiktok:'||tc.author_id::text)::text          AS source_node_id,
  ('post:tiktok:'||tc.tiktok_post_id::text)::text     AS target_node_id,
  pam.project_id::bigint                              AS project_id,
  pam.project_name::text                              AS project_name,
  tc.tiktok_post_id::text                             AS post_id,
  tc.id::bigint                                       AS comment_id,
  tc.created_at::timestamp                            AS created_at,
  tc.text::text                                       AS text_raw,
  NULL::text                                          AS sentiment_label,
  NULL::numeric                                       AS sentiment_score,
  et.emotion::text                                    AS emotion_label,
  et.score::numeric                                   AS emotion_score,
  NULL::boolean                                       AS hs_is_hate,
  NULL::numeric                                       AS hs_score,
  NULL::numeric                                       AS hs_percentage,
  NULL::text                                          AS spam_label,
  NULL::numeric                                       AS spam_score
FROM tiktok_comments tc
JOIN sna.posts_all_mv pam
  ON pam.platform = 'tiktok' AND pam.post_id = tc.tiktok_post_id::text
LEFT JOIN emotion_tiktoks et
  ON et.tiktok_comment_id = tc.id;

CREATE INDEX IF NOT EXISTS idx_edge_tt_target ON sna.edge_tiktok_comment_mv(target_node_id);
```

### 4) `edge_tiktok_reply_mv` – Reply TikTok (user → user)
> **Patch `created_at`**: jika `tiktok_comments_reply` tidak punya `created_at`, gunakan waktu komentar induk.

```sql
DROP MATERIALIZED VIEW IF EXISTS sna.edge_tiktok_reply_mv;
CREATE MATERIALIZED VIEW sna.edge_tiktok_reply_mv AS
WITH parent AS (
  SELECT
    tc.id::bigint            AS comment_id,
    tc.author_id::text       AS parent_author_id,
    tc.created_at::timestamp AS parent_created_at
  FROM tiktok_comments tc
)
SELECT
  'tiktok'::text                                       AS platform,
  'reply_to_user'::text                                AS interaction,
  ('user:tiktok:'||r.author_id::text)::text            AS source_node_id,
  ('user:tiktok:'||p.parent_author_id)::text           AS target_node_id,
  pam.project_id::bigint                               AS project_id,
  pam.project_name::text                               AS project_name,
  r.tiktok_post_id::text                               AS post_id,
  r.id::bigint                                         AS comment_id,
  p.parent_created_at                                  AS created_at,
  r.text::text                                         AS text_raw,
  NULL::text                                           AS sentiment_label,
  NULL::numeric                                        AS sentiment_score,
  NULL::text                                           AS emotion_label,
  NULL::numeric                                        AS emotion_score,
  NULL::boolean                                        AS hs_is_hate,
  NULL::numeric                                        AS hs_score,
  NULL::numeric                                        AS hs_percentage,
  NULL::text                                           AS spam_label,
  NULL::numeric                                        AS spam_score
FROM tiktok_comments_reply r
LEFT JOIN parent p ON p.comment_id = r.tiktok_comment_id::bigint
JOIN sna.posts_all_mv pam
  ON pam.platform = 'tiktok' AND pam.post_id = r.tiktok_post_id::text;

CREATE INDEX IF NOT EXISTS idx_edge_tt_reply_src  ON sna.edge_tiktok_reply_mv(source_node_id);
CREATE INDEX IF NOT EXISTS idx_edge_tt_reply_tgt  ON sna.edge_tiktok_reply_mv(target_node_id);
CREATE INDEX IF NOT EXISTS idx_edge_tt_reply_proj ON sna.edge_tiktok_reply_mv(project_id);
```

### 5) `edges_all_mv` – Gabungan semua edge (schema kolom seragam)
```sql
DROP MATERIALIZED VIEW IF EXISTS sna.edges_all_mv;
CREATE MATERIALIZED VIEW sna.edges_all_mv AS
SELECT platform, interaction, source_node_id, target_node_id,
       project_id, project_name, post_id, created_at, text_raw,
       sentiment_label, sentiment_score,
       emotion_label,   emotion_score,
       hs_is_hate, hs_score, hs_percentage,
       spam_label, spam_score
FROM sna.edge_instagram_comment_mv
UNION ALL
SELECT platform, interaction, source_node_id, target_node_id,
       project_id, project_name, post_id, created_at, text_raw,
       sentiment_label, sentiment_score,
       emotion_label,   emotion_score,
       hs_is_hate, hs_score, hs_percentage,
       spam_label, spam_score
FROM sna.edge_tiktok_comment_mv
UNION ALL
SELECT platform, interaction, source_node_id, target_node_id,
       project_id, project_name, post_id, created_at, text_raw,
       sentiment_label, sentiment_score,
       emotion_label,   emotion_score,
       hs_is_hate, hs_score, hs_percentage,
       spam_label, spam_score
FROM sna.edge_tiktok_reply_mv;

CREATE INDEX IF NOT EXISTS idx_edges_source ON sna.edges_all_mv(source_node_id);
CREATE INDEX IF NOT EXISTS idx_edges_target ON sna.edges_all_mv(target_node_id);
CREATE INDEX IF NOT EXISTS idx_edges_project ON sna.edges_all_mv(project_id);
```

### 6) `node_dim_mv` – Dimensi node (project/post/user)
```sql
DROP MATERIALIZED VIEW IF EXISTS sna.node_dim_mv;
CREATE MATERIALIZED VIEW sna.node_dim_mv AS
-- project nodes
SELECT DISTINCT
  ('project:'||pam.project_id::text) AS node_id,
  'project'::text                    AS node_type,
  NULL::text                         AS platform,
  pam.project_id::bigint             AS project_id,
  pam.project_name::text             AS project_name,
  NULL::text                         AS post_id,
  pam.project_name::text             AS label
FROM sna.posts_all_mv pam
UNION ALL
-- post nodes
SELECT DISTINCT
  ('post:'||pam.platform||':'||pam.post_id) AS node_id,
  'post'::text                               AS node_type,
  pam.platform::text                         AS platform,
  pam.project_id::bigint                     AS project_id,
  pam.project_name::text                     AS project_name,
  pam.post_id::text                          AS post_id,
  (pam.platform||' • '||pam.post_id||' • '||pam.project_name)::text AS label
FROM sna.posts_all_mv pam
UNION ALL
-- user nodes (source)
SELECT DISTINCT
  e.source_node_id AS node_id,
  'user'::text     AS node_type,
  split_part(e.source_node_id, ':', 2) AS platform,
  e.project_id::bigint, e.project_name::text, NULL::text,
  e.source_node_id AS label
FROM sna.edges_all_mv e
UNION ALL
-- user/post nodes (target)
SELECT DISTINCT
  e.target_node_id AS node_id,
  CASE WHEN e.target_node_id LIKE 'user:%' THEN 'user' ELSE 'post' END::text AS node_type,
  split_part(e.target_node_id, ':', 2) AS platform,
  e.project_id::bigint, e.project_name::text, NULL::text,
  e.target_node_id AS label
FROM sna.edges_all_mv e;
```

> `node_features_mv` untuk derajat & ringkasan skor – dipakai untuk tooltip/analisis ringan.

---

## 🔁 Refresh MV
**Urutan rekomendasi** saat ada data baru:
```sql
REFRESH MATERIALIZED VIEW sna.posts_all_mv;
REFRESH MATERIALIZED VIEW sna.edge_instagram_comment_mv;
REFRESH MATERIALIZED VIEW sna.edge_tiktok_comment_mv;
REFRESH MATERIALIZED VIEW sna.edge_tiktok_reply_mv;
REFRESH MATERIALIZED VIEW sna.edges_all_mv;
REFRESH MATERIALIZED VIEW sna.node_dim_mv;
-- REFRESH MATERIALIZED VIEW sna.node_features_mv; (opsional)
```
> Menggunakan `CONCURRENTLY` setelah MV sudah punya index & pernah di-refresh minimal sekali, dan saat beban baca tinggi.

---

## 🐍 Python: Visualisasi (NetworkX → PyVis)
### Struktur `.env`
```
PG_HOST=127.0.0.1
PG_PORT=5432
PG_USER=postgres
PG_PASSWORD=xxxx
PG_DB=sna
PG_SCHEMA=sna
```

### Instalasi paket
```
pip install sqlalchemy psycopg2-binary pandas networkx pyvis python-dotenv
```

### Script lengkap (warna: Hate→Spam→Sentiment)
> Menentukan warna edge: **merah cerah** untuk hate, **oranye** untuk spam, lalu **hijau/kuning/merah gelap** untuk sentiment (≥0.66/≥0.33/<0.33). Node post mewarisi warna platform.

```python
import os
import pandas as pd
import networkx as nx
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from dotenv import load_dotenv
from pyvis.network import Network

load_dotenv()
HOST   = os.getenv("PG_HOST", "127.0.0.1")
PORT   = os.getenv("PG_PORT", "5432")
USER   = os.getenv("PG_USER", "postgres")
PWD    = os.getenv("PG_PASSWORD", "")
DB     = os.getenv("PG_DB", "sna")
SCHEMA = os.getenv("PG_SCHEMA", "sna")

DB_URL = f"postgresql+psycopg2://{USER}:{quote_plus(PWD)}@{HOST}:{PORT}/{DB}"
engine = create_engine(DB_URL, connect_args={"options": f"-c search_path={SCHEMA}"}, pool_pre_ping=True)

def load_df(sql: str) -> pd.DataFrame:
    with engine.begin() as conn:
        return pd.read_sql(text(sql), conn)

platform_palette = {
    "instagram": "#E1306C", "tiktok": "#010101", "twitter": "#1DA1F2", "x": "#1DA1F2",
    "youtube": "#FF0000", "facebook": "#1877F2", "unknown": "#888888"
}
emo_palette = {
    "joy": "#FFC107", "sadness": "#2196F3", "anger": "#E91E63", "fear": "#6A1B9A",
    "surprise": "#00ACC1", "disgust": "#4E342E", "trust": "#388E3C", "anticipation": "#F57C00"
}

def interaction_color(inter: str) -> str:
    inter = (inter or "").lower()
    if "reply" in inter:   return "#FF9800"
    if "mention" in inter: return "#9C27B0"
    if "retweet" in inter or "share" in inter: return "#3F51B5"
    if "comment" in inter: return "#4CAF50"
    return "#9E9E9E"

def score_based_color(row: dict, fallback: str) -> str:
    # 1) Hate → merah cerah
    hs = row.get("hs_score")
    hs_is = row.get("hs_is_hate")
    hs_pct = row.get("hs_percentage")
    if (hs_is is True) or (pd.notna(hs) and float(hs) >= 0.5) or (pd.notna(hs_pct) and float(hs_pct) >= 50.0):
        return "#FF1744"
    # 2) Spam → oranye
    spam_score = row.get("spam_score")
    spam_label = (row.get("spam_label") or "").lower()
    if (pd.notna(spam_score) and float(spam_score) >= 0.5) or ("spam" in spam_label and spam_label != ""):
        return "#FF9800"
    # 3) Sentiment [0..1] → hijau/kuning/merah gelap
    sent = None
    for k in ("sentiment_score", "sent_score01", "avg_sent"):
        v = row.get(k)
        if pd.notna(v):
            try:
                sent = float(v); break
            except Exception:
                pass
    if sent is not None:
        if sent >= 0.66: return "#2E7D32"  # hijau
        if sent >= 0.33: return "#FBC02D"  # kuning
        return "#B71C1C"                  # merah gelap
    # 4) Emotion → palet (opsional)
    emo = (row.get("emotion_label") or row.get("emo_label") or "").strip().lower()
    if emo:
        return emo_palette.get(emo, "#607D8B")
    # 5) Fallback
    return fallback

# Load MV
nodes = load_df("""
    SELECT node_id, node_type, platform, project_id, project_name, post_id, label
    FROM node_dim_mv
""")
edges = load_df("""
    SELECT platform, interaction, source_node_id, target_node_id,
           project_id, project_name, post_id, created_at, text_raw,
           sentiment_label, sentiment_score,
           emotion_label, emotion_score,
           hs_is_hate, hs_score, hs_percentage,
           spam_label, spam_score
    FROM edges_all_mv
""")

# Build graph
G = nx.DiGraph()
for _, r in nodes.iterrows():
    ntype = r["node_type"]
    if ntype == "project":
        label = f"📁 {r['project_name']} (#{r['project_id']})"
        color = "#607D8B"; size = 28; shape = "box"
    elif ntype == "post":
        label = f"📝 {r['platform']} • {r['post_id']}\n{r['project_name']}"
        color = platform_palette.get((r["platform"] or "unknown").lower(), "#9E9E9E")
        size = 18; shape = "dot"
    else:
        label = r["label"].replace("user:", "👤 ")
        color = "#8BC34A"; size = 10; shape = "dot"
    title = (
        f"<b>Node:</b> {r['node_id']}<br>"
        f"<b>Type:</b> {r['node_type']}<br>"
        f"<b>Project:</b> {r['project_name']} (#{r['project_id']})<br>"
        f"<b>Platform:</b> {r['platform'] or '-'}<br>"
        f"<b>Post ID:</b> {r['post_id'] or '-'}"
    )
    G.add_node(r["node_id"], label=label, color=color, size=size, shape=shape, title=title)

for _, e in edges.iterrows():
    meta = []
    if pd.notna(e["sentiment_label"]) or pd.notna(e["sentiment_score"]):
        meta.append(f"Sentiment: <b>{e.get('sentiment_label','')}</b> ({e.get('sentiment_score','')})")
    if pd.notna(e["emotion_label"]) or pd.notna(e["emotion_score"]):
        meta.append(f"Emotion: <b>{e.get('emotion_label','')}</b> ({e.get('emotion_score','')})")
    if pd.notna(e["hs_is_hate"]) or pd.notna(e["hs_score"]) or pd.notna(e["hs_percentage"]):
        meta.append(f"Hate Speech: <b>{'YES' if e.get('hs_is_hate') else 'NO'}</b> (score={e.get('hs_score','')}, pct={e.get('hs_percentage','')})")
    if pd.notna(e["spam_label"]) or pd.notna(e["spam_score"]):
        meta.append(f"Spam: <b>{e.get('spam_label','')}</b> (score={e.get('spam_score','')})")
    meta_str = "<br>".join(meta) if meta else "-"

    text_html = (e["text_raw"] or "").replace("<", "&lt;").replace(">", "&gt;")
    title = (
        f"<div style='max-width:480px;white-space:normal'>"
        f"<b>Interaction:</b> {e['interaction']} | <b>Platform:</b> {e['platform']}<br>"
        f"<b>Project:</b> {e['project_name']} (#{e['project_id']})<br>"
        f"<b>Post ID:</b> {e['post_id']} | <b>Created:</b> {e.get('created_at','')}"
        f"<br><b>Text:</b> {text_html}<br>{meta_str}</div>"
    )

    base_w = 1.6
    bonus = 0.0
    if pd.notna(e["hs_score"]):      bonus += min(2.0, float(e["hs_score"]) * 2.0)
    if pd.notna(e["spam_score"]):    bonus += min(1.5, float(e["spam_score"]) * 1.5)
    if pd.notna(e["sentiment_score"]): bonus += min(1.5, abs(float(e["sentiment_score"]) - 0.5) * 2.0)
    width = base_w + bonus

    fallback = interaction_color(e["interaction"])
    edge_col = score_based_color(e, fallback)

    G.add_edge(
        e["source_node_id"], e["target_node_id"],
        color=edge_col, title=title, label=e["interaction"], width=width, arrows="to"
    )

pos = nx.spring_layout(G, seed=42)
net = Network(height="800px", width="100%", directed=True, bgcolor="#111111", font_color="white")

for n, data in G.nodes(data=True):
    x, y = pos[n]
    net.add_node(
        n,
        label=data.get("label"),
        title=data.get("title"),
        color=data.get("color"),
        size=data.get("size", 10),
        shape=data.get("shape", "dot"),
        x=float(x)*1000.0, y=float(y)*1000.0, physics=False
    )
for u, v, data in G.edges(data=True):
    net.add_edge(u, v,
        color=data.get("color"),
        title=data.get("title"),
        label=data.get("label"),
        width=float(data.get("width", 1.6))
    )

legend_html = """
<div style="position:absolute; right:20px; top:20px; background:#222; color:#fff; padding:10px 12px; border-radius:10px; font:12px/1.25 sans-serif;">
  <b>Legend</b><br>
  <span style="color:#607D8B">■</span> Project &nbsp;
  <span style="color:#E1306C">●</span> Post (IG) &nbsp;
  <span style="color:#010101">●</span> Post (TikTok) &nbsp;
  <span style="color:#8BC34A">●</span> User
  <hr style="border:0;border-top:1px solid #444;margin:6px 0">
  <span style="color:#4CAF50">━</span> comment &nbsp;
  <span style="color:#FF9800">━</span> reply &nbsp;
  <span style="color:#3F51B5">━</span> share/retweet &nbsp;
  <span style="color:#9C27B0">━</span> mention
  <hr style="border:0;border-top:1px solid #444;margin:6px 0">
  Edge priority: <span style="color:#FF1744">Hate</span> → <span style="color:#FF9800">Spam</span> → Sentiment (<span style="color:#2E7D32">Hijau</span> / <span style="#FBC02D">Kuning</span> / <span style="#B71C1C">Merah gelap</span>)
</div>
"""
net.set_template("""<!DOCTYPE html>
<html><head><meta charset="utf-8"/><title>SNA Network</title>{{ head }}</head>
<body>{{ body }}""" + legend_html + """</body></html>""")

out_html = "sna_network.html"
with open(out_html, "w", encoding="utf-8") as f:
    f.write(net.generate_html(notebook=False))
print(f"Exported: {out_html}")
```

---

## 🧪 Verifikasi Cepat
- **Cek MV siap**
  ```sql
  SELECT platform, COUNT(*) FROM sna.posts_all_mv GROUP BY 1;
  SELECT interaction, COUNT(*) FROM sna.edges_all_mv GROUP BY 1;
  SELECT node_type, COUNT(*) FROM sna.node_dim_mv GROUP BY 1;
  ```
- **Sampling** (pastikan ada data):
  ```sql
  SELECT * FROM sna.edges_all_mv ORDER BY created_at DESC NULLS LAST LIMIT 20;
  ```

---

## 🚀 Eksekusi & Otomasi
- **Sekali jalan (manual)**:
  1. Jalankan semua **CREATE MV** (sekali) → lalu `REFRESH` (tanpa `CONCURRENTLY` pertama kali).
  2. Jalankan script Python → hasil **`sna_network.html`**.

---

## 🧭 Best Practices
- Tambahkan MV untuk platform/aksi lain: `edge_twitter_retweet_mv`, `edge_facebook_share_mv`, `edge_instagram_mention_mv`, dst → UNION ke `edges_all_mv`.
- Simpan **ID format** konsisten—jangan campur tipe.
- Buat **index** di kolom filter: `(platform, post_id)`, `project_id`, `created_at`.
- Pertimbangkan **CONCURRENTLY** untuk refresh saat produksi.
- Pisahkan file: `sql/create_mvs.sql`, `python/interactive_viz_sna.py`, `.env`.

---

## 📎 Ringkasan Checklist
- [ ] Buat schema `sna`
- [ ] Create MV `posts_all_mv`
- [ ] Create MV `edge_instagram_comment_mv`
- [ ] Create MV `edge_tiktok_comment_mv`
- [ ] Create MV `edge_tiktok_reply_mv`
- [ ] Create MV `edges_all_mv`
- [ ] Create MV `node_dim_mv`
- [ ] Refresh MV berurutan
- [ ] Jalankan script Python
- [ ] Validasi hasil HTML

---

## 📚 Lampiran
### A. Query uji cepat per project
```sql
SELECT * FROM sna.edges_all_mv WHERE project_id = :pid ORDER BY created_at DESC LIMIT 100;
```

### B. Filter platform tertentu di Python
```python
edges = edges[edges['platform'].isin(['instagram','tiktok'])]
```

### C. Ubah ambang warna sentiment
```python
# score_based_color():
# if sent >= 0.70 hijau, >=0.40 kuning, else merah gelap
```

---
