"""
Sync corpus.db (SQLite) → Atalaria Postgres + pgvector.

Idempotent full-reload model. Run as cron weekly. Schema mirrors
CORPUS_SCHEMA.md.

Env vars:
  ATALARIA_PG_URL    postgresql://user:pass@host:port/db
  BATCH_SIZE         default 1000

Usage:
  ATALARIA_PG_URL=postgresql://... python sync_to_atalaria.py --reset
  python sync_to_atalaria.py --incremental    # only new rows since last sync
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from pathlib import Path

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:
    print("Install: uv add psycopg[binary] pgvector")
    sys.exit(2)

HERE = Path(__file__).parent
SQLITE_PATH = HERE / "corpus.db"

DDL = """
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS corpus_document (
    doc_id BIGSERIAL PRIMARY KEY,
    source TEXT NOT NULL,
    source_native_id TEXT NOT NULL,
    tipo TEXT, jerarquia TEXT, titulo TEXT, rubro TEXT, clave TEXT,
    epoca TEXT, instancia TEXT, organo TEXT, materias TEXT[],
    fecha_emision DATE, fecha_publicacion DATE,
    vigente BOOLEAN DEFAULT TRUE,
    status_detalle TEXT, autor TEXT,
    metadata JSONB, raw_text TEXT, source_url TEXT,
    fetched_at TIMESTAMPTZ DEFAULT NOW(),
    tsv_es tsvector,
    UNIQUE(source, source_native_id)
);
CREATE INDEX IF NOT EXISTS idx_doc_src    ON corpus_document(source);
CREATE INDEX IF NOT EXISTS idx_doc_fecha  ON corpus_document(fecha_emision DESC);
CREATE INDEX IF NOT EXISTS idx_doc_jer    ON corpus_document(jerarquia);
CREATE INDEX IF NOT EXISTS idx_doc_vigente ON corpus_document(vigente) WHERE vigente;
CREATE INDEX IF NOT EXISTS idx_doc_mat    ON corpus_document USING gin(materias);
CREATE INDEX IF NOT EXISTS idx_doc_meta   ON corpus_document USING gin(metadata jsonb_path_ops);
CREATE INDEX IF NOT EXISTS idx_doc_tsv    ON corpus_document USING gin(tsv_es);

CREATE TABLE IF NOT EXISTS corpus_chunk (
    chunk_id BIGSERIAL PRIMARY KEY,
    doc_id BIGINT NOT NULL REFERENCES corpus_document(doc_id) ON DELETE CASCADE,
    chunk_index INT NOT NULL,
    chunk_type TEXT NOT NULL,
    text TEXT NOT NULL,
    char_count INT,
    token_count INT,
    embedding vector(1536),
    embedding_model TEXT,
    embedded_at TIMESTAMPTZ,
    tsv_es tsvector,
    UNIQUE(doc_id, chunk_index)
);
CREATE INDEX IF NOT EXISTS idx_chunk_doc  ON corpus_chunk(doc_id);
CREATE INDEX IF NOT EXISTS idx_chunk_type ON corpus_chunk(chunk_type);
CREATE INDEX IF NOT EXISTS idx_chunk_emb  ON corpus_chunk USING hnsw (embedding vector_cosine_ops) WITH (m=16, ef_construction=64);
CREATE INDEX IF NOT EXISTS idx_chunk_tsv  ON corpus_chunk USING gin(tsv_es);

CREATE TABLE IF NOT EXISTS corpus_citation (
    cite_id BIGSERIAL PRIMARY KEY,
    citing_doc_id BIGINT REFERENCES corpus_document(doc_id),
    citing_chunk_id BIGINT REFERENCES corpus_chunk(chunk_id),
    norm_type TEXT NOT NULL,
    norm_canon TEXT NOT NULL,
    norm_raw TEXT,
    cited_doc_id BIGINT REFERENCES corpus_document(doc_id),
    context_snippet TEXT
);
CREATE INDEX IF NOT EXISTS idx_cite_citing ON corpus_citation(citing_doc_id);
CREATE INDEX IF NOT EXISTS idx_cite_canon  ON corpus_citation(norm_canon);

CREATE TABLE IF NOT EXISTS crawl_log (
    log_id BIGSERIAL PRIMARY KEY,
    source TEXT NOT NULL,
    op TEXT NOT NULL,
    url TEXT, target_id TEXT,
    status_code INT, bytes INT, duration_ms INT,
    success BOOLEAN, error TEXT, metadata JSONB,
    fetched_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_log_src_time ON crawl_log(source, fetched_at DESC);
CREATE INDEX IF NOT EXISTS idx_log_errors ON crawl_log(source, fetched_at DESC) WHERE NOT success;

CREATE TABLE IF NOT EXISTS search_query (
    query_id BIGSERIAL PRIMARY KEY,
    query_text TEXT NOT NULL,
    embedding vector(1536),
    asked_at TIMESTAMPTZ DEFAULT NOW(),
    user_session TEXT
);

CREATE TABLE IF NOT EXISTS search_retrieval (
    retrieval_id BIGSERIAL PRIMARY KEY,
    query_id BIGINT REFERENCES search_query(query_id),
    chunk_id BIGINT REFERENCES corpus_chunk(chunk_id),
    rank_vector INT, score_vector FLOAT,
    rank_rerank INT, score_rerank FLOAT,
    used_for_answer BOOLEAN DEFAULT FALSE
);
CREATE INDEX IF NOT EXISTS idx_retrieval_query ON search_retrieval(query_id);
"""


def apply_ddl(pg_conn):
    print("Applying DDL...")
    with pg_conn.cursor() as cur:
        cur.execute(DDL)
    pg_conn.commit()


def sync_documents(pg_conn, sqlite_conn, reset: bool, batch: int = 1000):
    if reset:
        print("Truncating corpus_document/chunk/citation in Postgres...")
        with pg_conn.cursor() as cur:
            cur.execute("TRUNCATE corpus_citation, corpus_chunk, corpus_document RESTART IDENTITY CASCADE")
        pg_conn.commit()

    sqlite_conn.row_factory = sqlite3.Row
    cur_sl = sqlite_conn.cursor()
    n_total = cur_sl.execute("SELECT COUNT(*) FROM corpus_document").fetchone()[0]
    print(f"\nSyncing corpus_document ({n_total} rows)...")

    inserted = 0
    cur_sl.execute("""
        SELECT doc_id, source, source_native_id, tipo, jerarquia, titulo, rubro, clave,
               epoca, instancia, organo, materias_json, fecha_emision, fecha_publicacion,
               vigente, status_detalle, autor, metadata_json, raw_text, source_url
        FROM corpus_document
    """)

    # Map SQLite doc_id → new Postgres doc_id (since we use BIGSERIAL on Postgres).
    sqlite_to_pg: dict[int, int] = {}

    batch_rows = []
    with pg_conn.cursor() as cur_pg:
        for r in cur_sl:
            materias = json.loads(r["materias_json"]) if r["materias_json"] else None
            metadata = json.loads(r["metadata_json"]) if r["metadata_json"] else None
            batch_rows.append((
                r["doc_id"], r["source"], r["source_native_id"], r["tipo"], r["jerarquia"],
                r["titulo"], r["rubro"], r["clave"], r["epoca"], r["instancia"], r["organo"],
                materias, r["fecha_emision"], r["fecha_publicacion"],
                bool(r["vigente"]), r["status_detalle"], r["autor"],
                json.dumps(metadata) if metadata else None,
                r["raw_text"], r["source_url"],
            ))
            if len(batch_rows) >= batch:
                _flush_docs(cur_pg, batch_rows, sqlite_to_pg)
                pg_conn.commit()
                inserted += len(batch_rows)
                print(f"  {inserted}/{n_total}", end="\r", flush=True)
                batch_rows = []
        if batch_rows:
            _flush_docs(cur_pg, batch_rows, sqlite_to_pg)
            pg_conn.commit()
            inserted += len(batch_rows)
    print(f"\n  ✓ {inserted} documents")

    # Now sync chunks using sqlite_to_pg mapping
    print("\nSyncing corpus_chunk...")
    n_chunks = cur_sl.execute("SELECT COUNT(*) FROM corpus_chunk").fetchone()[0]
    print(f"  total chunks: {n_chunks}")

    inserted = 0
    cur_sl.execute("SELECT doc_id, chunk_index, chunk_type, text, char_count FROM corpus_chunk")
    batch_rows = []
    with pg_conn.cursor() as cur_pg:
        for r in cur_sl:
            pg_doc_id = sqlite_to_pg.get(r["doc_id"])
            if not pg_doc_id:
                continue
            batch_rows.append((pg_doc_id, r["chunk_index"], r["chunk_type"], r["text"], r["char_count"]))
            if len(batch_rows) >= batch:
                cur_pg.executemany(
                    "INSERT INTO corpus_chunk(doc_id, chunk_index, chunk_type, text, char_count) "
                    "VALUES(%s,%s,%s,%s,%s) ON CONFLICT(doc_id, chunk_index) DO UPDATE SET "
                    "chunk_type=EXCLUDED.chunk_type, text=EXCLUDED.text, char_count=EXCLUDED.char_count",
                    batch_rows
                )
                pg_conn.commit()
                inserted += len(batch_rows)
                print(f"  {inserted}/{n_chunks}", end="\r", flush=True)
                batch_rows = []
        if batch_rows:
            cur_pg.executemany(
                "INSERT INTO corpus_chunk(doc_id, chunk_index, chunk_type, text, char_count) "
                "VALUES(%s,%s,%s,%s,%s) ON CONFLICT(doc_id, chunk_index) DO UPDATE SET "
                "chunk_type=EXCLUDED.chunk_type, text=EXCLUDED.text, char_count=EXCLUDED.char_count",
                batch_rows
            )
            pg_conn.commit()
            inserted += len(batch_rows)
    print(f"\n  ✓ {inserted} chunks")

    # Populate tsvectors
    print("\nBuilding spanish tsvectors...")
    with pg_conn.cursor() as cur:
        cur.execute("UPDATE corpus_document SET tsv_es = to_tsvector('spanish', coalesce(titulo,'') || ' ' || coalesce(rubro,'') || ' ' || coalesce(raw_text,'')) WHERE tsv_es IS NULL")
        cur.execute("UPDATE corpus_chunk SET tsv_es = to_tsvector('spanish', text) WHERE tsv_es IS NULL")
    pg_conn.commit()
    print("  ✓ tsv_es populated")


def _flush_docs(cur, batch_rows, sqlite_to_pg):
    """Insert docs and capture the new doc_id mapping."""
    for row in batch_rows:
        sqlite_id = row[0]
        cur.execute("""
            INSERT INTO corpus_document(source, source_native_id, tipo, jerarquia, titulo,
                                        rubro, clave, epoca, instancia, organo, materias,
                                        fecha_emision, fecha_publicacion, vigente, status_detalle,
                                        autor, metadata, raw_text, source_url)
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT(source, source_native_id) DO UPDATE SET
                tipo=EXCLUDED.tipo, jerarquia=EXCLUDED.jerarquia, titulo=EXCLUDED.titulo,
                rubro=EXCLUDED.rubro, clave=EXCLUDED.clave, epoca=EXCLUDED.epoca,
                instancia=EXCLUDED.instancia, organo=EXCLUDED.organo, materias=EXCLUDED.materias,
                fecha_emision=EXCLUDED.fecha_emision, fecha_publicacion=EXCLUDED.fecha_publicacion,
                vigente=EXCLUDED.vigente, status_detalle=EXCLUDED.status_detalle,
                autor=EXCLUDED.autor, metadata=EXCLUDED.metadata, raw_text=EXCLUDED.raw_text,
                source_url=EXCLUDED.source_url
            RETURNING doc_id
        """, row[1:])
        new_id = cur.fetchone()[0]
        sqlite_to_pg[sqlite_id] = new_id


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--reset", action="store_true", help="Truncate Postgres tables first")
    ap.add_argument("--batch-size", type=int, default=1000)
    args = ap.parse_args()

    pg_url = os.environ.get("ATALARIA_PG_URL")
    if not pg_url:
        print("ATALARIA_PG_URL env var required")
        sys.exit(1)
    if not SQLITE_PATH.exists():
        print(f"corpus.db not found at {SQLITE_PATH}; run export_to_corpus.py first")
        sys.exit(1)

    pg_conn = psycopg.connect(pg_url)
    sqlite_conn = sqlite3.connect(SQLITE_PATH)

    try:
        apply_ddl(pg_conn)
        sync_documents(pg_conn, sqlite_conn, reset=args.reset, batch=args.batch_size)
    finally:
        sqlite_conn.close()
        pg_conn.close()

    print("\nDone. Next steps:")
    print("  1. Embed chunks: separate job calling text-embedding-3-small")
    print("  2. Verify HNSW index on corpus_chunk.embedding is built")
    print("  3. Hook MCP postgres tool to query corpus_chunk/corpus_document")


if __name__ == "__main__":
    main()
