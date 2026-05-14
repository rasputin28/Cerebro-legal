"""
Embed corpus_chunk.text → corpus_chunk.embedding (vector(1536)).

Runs against the Atalaria Postgres. Uses OpenAI text-embedding-3-small in
batches of 100 chunks per request. Resumable: skips rows where embedding IS
NOT NULL.

Env vars:
  ATALARIA_PG_URL    postgresql://...
  OPENAI_API_KEY     sk-...
  EMBED_MODEL        text-embedding-3-small (default) | text-embedding-3-large

Usage:
  uv add openai
  ATALARIA_PG_URL=... OPENAI_API_KEY=... python embed_chunks.py
  python embed_chunks.py --limit 1000      # test run
  python embed_chunks.py --chunk-types rubro,criterio,hechos,justificacion  # priority order
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from typing import Iterator

try:
    import psycopg
    from openai import OpenAI
except ImportError:
    print("Install: uv add psycopg[binary] openai")
    sys.exit(2)


EMBED_MODEL = os.environ.get("EMBED_MODEL", "text-embedding-3-small")
EMBED_DIM = 3072 if "large" in EMBED_MODEL else 1536
BATCH = int(os.environ.get("EMBED_BATCH", "100"))
MAX_INPUT_TOKENS = 8000  # leave headroom under 8192 limit


def fetch_pending(pg, limit: int | None, chunk_types: list[str] | None) -> Iterator[tuple[int, str]]:
    sql = "SELECT chunk_id, text FROM corpus_chunk WHERE embedding IS NULL"
    params: list = []
    if chunk_types:
        sql += " AND chunk_type = ANY(%s)"
        params.append(chunk_types)
    sql += " ORDER BY chunk_id"
    if limit:
        sql += f" LIMIT {limit}"
    with pg.cursor() as cur:
        cur.execute(sql, params)
        for cid, text in cur:
            yield cid, text


def truncate_text(text: str, max_chars: int = 24000) -> str:
    """Rough truncation; embedding-3-small accepts ~8k tokens ≈ 24k chars in Spanish."""
    return text[:max_chars]


def embed_batch(client: OpenAI, texts: list[str]) -> list[list[float]]:
    resp = client.embeddings.create(model=EMBED_MODEL, input=texts)
    return [d.embedding for d in resp.data]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int)
    ap.add_argument("--chunk-types", help="comma-separated chunk_type filter")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    pg_url = os.environ.get("ATALARIA_PG_URL")
    api_key = os.environ.get("OPENAI_API_KEY")
    if not pg_url or not api_key:
        print("ATALARIA_PG_URL and OPENAI_API_KEY required")
        sys.exit(1)

    chunk_types = args.chunk_types.split(",") if args.chunk_types else None

    pg = psycopg.connect(pg_url)
    client = OpenAI(api_key=api_key)

    with pg.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM corpus_chunk WHERE embedding IS NULL"
                    + (" AND chunk_type = ANY(%s)" if chunk_types else ""),
                    [chunk_types] if chunk_types else [])
        n_pending = cur.fetchone()[0]
    print(f"Pending: {n_pending} chunks (model={EMBED_MODEL}, dim={EMBED_DIM}, batch={BATCH})")
    if args.dry_run:
        return

    processed = 0
    t0 = time.time()
    pending = list(fetch_pending(pg, args.limit, chunk_types))
    while pending:
        batch_rows = pending[:BATCH]
        pending = pending[BATCH:]
        texts = [truncate_text(t) for _, t in batch_rows]
        try:
            embeddings = embed_batch(client, texts)
        except Exception as e:
            print(f"[err] {e}; sleeping 30s")
            time.sleep(30)
            continue
        with pg.cursor() as cur:
            cur.executemany(
                "UPDATE corpus_chunk SET embedding = %s, embedding_model = %s, embedded_at = NOW() WHERE chunk_id = %s",
                [(e, EMBED_MODEL, cid) for (cid, _), e in zip(batch_rows, embeddings)]
            )
        pg.commit()
        processed += len(batch_rows)
        rate = processed / (time.time() - t0)
        eta_min = (n_pending - processed) / rate / 60 if rate > 0 else 0
        print(f"  {processed}/{n_pending}  ({rate:.1f}/s, ETA {eta_min:.0f}m)", flush=True)


if __name__ == "__main__":
    main()
