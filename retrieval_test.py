"""
Validate corpus.db schema end-to-end with a working retrieval pipeline.

Demonstrates:
  1. Hybrid BM25-style FTS5 retrieval over corpus_chunk.text (Spanish stems)
  2. Returns top-300 chunks (the pgvector ANN equivalent in our SQLite stage)
  3. Joins with corpus_document for source attribution
  4. Output ready to feed a reranker

This is the schema validation: prove the design supports the
'300+ chunks → top 100' workflow before going to Postgres+pgvector.
"""
from __future__ import annotations

import sqlite3
import sys
import time
from pathlib import Path

DB = Path(__file__).parent / "corpus.db"


def setup_fts(conn: sqlite3.Connection):
    """Create FTS5 virtual table mirroring corpus_chunk for retrieval."""
    print("Building FTS5 index over corpus_chunk (Spanish)…")
    t0 = time.time()
    conn.execute("""
        CREATE VIRTUAL TABLE IF NOT EXISTS chunk_fts USING fts5(
            text, chunk_type, source,
            content='', tokenize='unicode61 remove_diacritics 2'
        )
    """)
    # Check if already populated
    n_fts = conn.execute("SELECT COUNT(*) FROM chunk_fts").fetchone()[0]
    n_chunks = conn.execute("SELECT COUNT(*) FROM corpus_chunk").fetchone()[0]
    if n_fts < n_chunks:
        print(f"  populating FTS ({n_chunks} chunks)…")
        conn.execute("DELETE FROM chunk_fts")
        conn.execute("""
            INSERT INTO chunk_fts(rowid, text, chunk_type, source)
            SELECT c.chunk_id, c.text, c.chunk_type, d.source
            FROM corpus_chunk c JOIN corpus_document d ON d.doc_id = c.doc_id
        """)
        conn.commit()
    n_fts = conn.execute("SELECT COUNT(*) FROM chunk_fts").fetchone()[0]
    print(f"  FTS index: {n_fts} entries ({time.time()-t0:.1f}s)")


def query(conn: sqlite3.Connection, q: str, top_k: int = 300) -> list[dict]:
    """Hybrid retrieval: FTS5 BM25 scoring over Spanish stems, top-K."""
    rows = conn.execute("""
        SELECT
          c.chunk_id, c.chunk_type, c.text, c.char_count,
          d.doc_id, d.source, d.titulo, d.rubro, d.clave, d.fecha_emision,
          d.jerarquia, d.tipo, d.epoca,
          chunk_fts.rank AS bm25_score
        FROM chunk_fts
        JOIN corpus_chunk c ON c.chunk_id = chunk_fts.rowid
        JOIN corpus_document d ON d.doc_id = c.doc_id
        WHERE chunk_fts MATCH ?
        ORDER BY bm25_score
        LIMIT ?
    """, (q, top_k)).fetchall()
    cols = ["chunk_id","chunk_type","text","char_count","doc_id","source",
            "titulo","rubro","clave","fecha","jerarquia","tipo","epoca","bm25"]
    return [dict(zip(cols, r)) for r in rows]


def demo_query(conn, q: str, top_k: int = 300):
    t0 = time.time()
    results = query(conn, q, top_k=top_k)
    dt = (time.time() - t0) * 1000
    print(f"\n[Q] {q!r}  → {len(results)} chunks in {dt:.0f}ms")
    # Source distribution
    by_source = {}
    by_type = {}
    for r in results:
        by_source[r["source"]] = by_source.get(r["source"], 0) + 1
        by_type[r["chunk_type"]] = by_type.get(r["chunk_type"], 0) + 1
    print(f"  by source: {dict(sorted(by_source.items(), key=lambda x:-x[1]))}")
    print(f"  by chunk_type: {dict(sorted(by_type.items(), key=lambda x:-x[1])[:6])}")
    # Top-5 preview
    print(f"  top 5 (BM25):")
    for r in results[:5]:
        title = (r["titulo"] or r["rubro"] or "")[:80]
        snippet = r["text"][:120].replace("\n"," ")
        print(f"   [{r['source']:13s} {r['chunk_type']:14s} bm25={r['bm25']:.3f}] {title}")
        print(f"     → {snippet}…")


def main():
    conn = sqlite3.connect(DB)
    setup_fts(conn)

    print("\n=== Retrieval validation: 300+ candidate chunks per query ===")
    queries = [
        "amparo directo contra sentencia laboral",
        "non bis in idem materia penal",
        "interés superior del menor en proceso familiar",
        "facultades de investigación del Ministerio Público",
        "competencia económica abuso de posición dominante",
        "tortura como prueba ilícita",
        "derechos humanos personas con discapacidad",
        "responsabilidad patrimonial del Estado",
        "art. 8.4 CADH",
        "control de constitucionalidad ex officio",
    ]
    for q in queries:
        demo_query(conn, q, top_k=300)


if __name__ == "__main__":
    main()
