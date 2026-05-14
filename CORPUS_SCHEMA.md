# Unified Corpus Schema — pgvector + reranker target

Diseño normalizado para migrar las bases por-fuente (SQLite) a Postgres con
pgvector. Soporta: semantic search → reranker (recall 300+ chunks → top 100)
→ generación de estrategia. Conservamos `raw_text` y `raw_json` por fuente
para re-extracción sin re-crawl.

## Capas

```
SQLite per-source (ingestion)  →  corpus.{document, chunk, citation}  →  pgvector index
                                   (Postgres production)                 + reranker layer
```

## Postgres production schema

```sql
-- == DOCUMENT LAYER =========================================================
CREATE TABLE corpus_document (
    doc_id          BIGSERIAL PRIMARY KEY,
    source          TEXT      NOT NULL,        -- 'scjn_tesis','scjn_historica','tfja','sre','sentencia_scjn','corte_idh','dof','dip_leyes','tepjf'
    source_native_id TEXT     NOT NULL,        -- registroDigital, ius, id_tfja, token_sre, idEngrose...
    tipo            TEXT,                      -- 'tesis_aislada','jurisprudencia','sentencia','tratado','sentencia_idh','opinion_consultiva','ley','reglamento','dof_publicacion'
    jerarquia       TEXT,                      -- 'constitucional','convencional','federal','estatal','criterio_pjf','administrativo','electoral'
    titulo          TEXT,
    rubro           TEXT,                      -- short canonical title for citation
    clave           TEXT,                      -- legal citation key when present
    epoca           TEXT,
    instancia       TEXT,
    organo          TEXT,
    materias        TEXT[],                    -- multi-label
    fecha_emision   DATE,
    fecha_publicacion DATE,
    vigente         BOOLEAN DEFAULT TRUE,
    status_detalle  TEXT,                      -- 'vigente','suspendida','modificada','derogada','sin_efectos','reformada'
    autor           TEXT,                      -- ponente / magistrado / autor del documento
    metadata        JSONB,                     -- everything else (precedentes, votos, localización, raw fields)
    raw_text        TEXT,                      -- full unsegmented text
    raw_html        TEXT,                      -- when source serves HTML
    raw_pdf_bytes   BYTEA,                     -- when source serves PDF (sentencias, TFJA)
    source_url      TEXT,
    fetched_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(source, source_native_id)
);
CREATE INDEX idx_doc_source       ON corpus_document(source);
CREATE INDEX idx_doc_fecha        ON corpus_document(fecha_emision DESC);
CREATE INDEX idx_doc_jerarquia    ON corpus_document(jerarquia);
CREATE INDEX idx_doc_vigente      ON corpus_document(vigente) WHERE vigente = TRUE;
CREATE INDEX idx_doc_materias_gin ON corpus_document USING gin(materias);
CREATE INDEX idx_doc_meta_gin     ON corpus_document USING gin(metadata jsonb_path_ops);


-- == CHUNK LAYER (pgvector) =================================================
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE corpus_chunk (
    chunk_id        BIGSERIAL PRIMARY KEY,
    doc_id          BIGINT NOT NULL REFERENCES corpus_document(doc_id) ON DELETE CASCADE,
    chunk_index     INT    NOT NULL,
    chunk_type      TEXT   NOT NULL,           -- 'rubro','hechos','criterio','justificacion','considerando','resolutivo','precedente','parrafo','section','full'
    parent_chunk_id BIGINT REFERENCES corpus_chunk(chunk_id), -- hierarchical (paragraph inside section)
    text            TEXT   NOT NULL,
    char_count      INT    NOT NULL,
    token_count     INT,
    embedding       vector(1536),              -- text-embedding-3-small (swap to 3072 for -large)
    embedding_model TEXT,
    embedded_at     TIMESTAMPTZ,
    UNIQUE(doc_id, chunk_index)
);
CREATE INDEX idx_chunk_doc       ON corpus_chunk(doc_id);
CREATE INDEX idx_chunk_type      ON corpus_chunk(chunk_type);
-- HNSW for fast ANN search; m/ef_construction tunable
CREATE INDEX idx_chunk_embedding ON corpus_chunk USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64);


-- == CITATION GRAPH =========================================================
-- Captures normative references parsed from text (constitución, tratados, tesis).
CREATE TABLE corpus_citation (
    cite_id         BIGSERIAL PRIMARY KEY,
    citing_doc_id   BIGINT NOT NULL REFERENCES corpus_document(doc_id) ON DELETE CASCADE,
    citing_chunk_id BIGINT REFERENCES corpus_chunk(chunk_id),
    norm_type       TEXT NOT NULL,             -- 'articulo_const','tratado_art','tesis','ley_art','reglamento_art','opinion_consultiva'
    norm_canon      TEXT NOT NULL,             -- canonical form: 'CPEUM:1','CADH:8.4','TESIS:2a./J. 19/2021 (11a.)','LFT:47'
    norm_raw        TEXT,                      -- as-found-in-text
    cited_doc_id    BIGINT REFERENCES corpus_document(doc_id), -- resolved if we have the target
    context_snippet TEXT,
    char_offset     INT
);
CREATE INDEX idx_cite_citing  ON corpus_citation(citing_doc_id);
CREATE INDEX idx_cite_canon   ON corpus_citation(norm_canon);
CREATE INDEX idx_cite_resolved ON corpus_citation(cited_doc_id) WHERE cited_doc_id IS NOT NULL;


-- == CRAWL OBSERVABILITY ====================================================
CREATE TABLE crawl_log (
    log_id        BIGSERIAL PRIMARY KEY,
    source        TEXT NOT NULL,
    op            TEXT NOT NULL,               -- 'listing','detail','file_fetch','re_parse','citation_extract'
    url           TEXT,
    target_id     TEXT,
    status_code   INT,
    bytes         INT,
    duration_ms   INT,
    success       BOOLEAN,
    error         TEXT,
    metadata      JSONB,
    fetched_at    TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_log_source_time ON crawl_log(source, fetched_at DESC);
CREATE INDEX idx_log_errors      ON crawl_log(source, fetched_at DESC) WHERE success = FALSE;


-- == RERANKER / SEARCH AUDIT =================================================
CREATE TABLE search_query (
    query_id      BIGSERIAL PRIMARY KEY,
    query_text    TEXT NOT NULL,
    embedding     vector(1536),
    asked_at      TIMESTAMPTZ DEFAULT NOW(),
    user_session  TEXT
);
CREATE TABLE search_retrieval (
    retrieval_id  BIGSERIAL PRIMARY KEY,
    query_id      BIGINT REFERENCES search_query(query_id),
    chunk_id      BIGINT REFERENCES corpus_chunk(chunk_id),
    rank_vector   INT,                         -- rank from pgvector ANN (1..N where N≥300)
    score_vector  FLOAT,
    rank_rerank   INT,                         -- rank after cross-encoder rerank (1..100)
    score_rerank  FLOAT,
    used_for_answer BOOLEAN DEFAULT FALSE
);
CREATE INDEX idx_retrieval_query ON search_retrieval(query_id);


-- == FULL-TEXT SUPPLEMENT ====================================================
-- Spanish text search complement for hybrid retrieval (BM25-style with pg_trgm).
ALTER TABLE corpus_document ADD COLUMN tsv_es tsvector;
UPDATE corpus_document SET tsv_es = to_tsvector('spanish',
    coalesce(titulo,'') || ' ' || coalesce(rubro,'') || ' ' || coalesce(raw_text,''));
CREATE INDEX idx_doc_tsv_es ON corpus_document USING gin(tsv_es);

ALTER TABLE corpus_chunk ADD COLUMN tsv_es tsvector;
UPDATE corpus_chunk SET tsv_es = to_tsvector('spanish', text);
CREATE INDEX idx_chunk_tsv_es ON corpus_chunk USING gin(tsv_es);
```

## Chunking strategy

Per-source, semantic-first:

| Fuente | chunk_type values | strategy |
|---|---|---|
| SCJN tesis (11ª) | `rubro`, `hechos`, `criterio`, `justificacion`, `precedente` | one chunk per pre-segmented field |
| SCJN tesis (5ª–10ª) | `rubro`, `cuerpo` + per-`parrafo` | rubro + body split on ¶ |
| SCJN históricas | `rubro`, `cuerpo` | as-is (narrative) |
| Sentencias SCJN | `encabezado`, `considerando`, `resolutivo`, `precedente` | parse `<p class="corte*">` |
| TFJA | `materia`, `rubro`, `cuerpo`, `precedente` | parsed fields |
| SRE tratados | `nombre`, `clausula`, `reserva`, `declaracion` | by article when DOF text loaded |
| Corte IDH | `hechos_probados`, `consideraciones_derecho`, `puntos_resolutivos`, `voto_particular` | by IDH section headers |
| DOF publicaciones | `por_resolutivo`, `articulo` | by article |
| Leyes federales | `articulo`, `transitorio` | per article |

**Chunk size guidelines**: target 400–800 tokens. Where a semantic field is shorter,
keep as a single chunk. Where it's longer (e.g. considerandos > 1500 tokens), split on
paragraph boundaries and link via `parent_chunk_id`.

## Embedding plan

- Model: `text-embedding-3-small` (1536 dim) for breadth — cost-effective, swap to
  `text-embedding-3-large` (3072 dim) only on subset that demands max quality.
- Batch by source; index in HNSW after embedding.
- Re-embed strategy: track `embedding_model` per chunk so we can selectively
  re-embed when upgrading models.

## Reranker layer (recall ≥ 300 → top 100)

- Cross-encoder retrieval: `bge-reranker-v2-m3` or `Cohere rerank-multilingual-v3`.
- Flow:
  1. Query → embedding → pgvector ANN top-300 (with `ef_search=400+`)
  2. Optional hybrid: union with top-K BM25 hits over `tsv_es`
  3. Reranker scores 300 candidates → top-100
  4. Log everything to `search_retrieval` for offline evaluation
- Track per-query "answer chunks" via `used_for_answer = TRUE` for relevance feedback.

## Migration path

1. **Now (SQLite per source)**: continue ingestion as already designed.
2. **Export script** (`export_to_corpus.py`): walks each SQLite table, maps to
   `corpus_document` rows + chunking. One Postgres-loadable JSONL per source.
3. **Postgres setup**: `pg17 + pgvector 0.7` on the same host. Bulk-load with
   `COPY` from JSONL.
4. **Embedding pass**: separate async job batched (1k chunks/batch).
5. **Index build + reranker integration**.

## Source → corpus_document field mapping

Quick reference (English column = corpus, Spanish = current SQLite field):

| corpus | scjn_tesis | scjn_historica | tfja | sre | sentencia_scjn |
|---|---|---|---|---|---|
| source_native_id | registro_digital | ius | id_tfja | token_sre | id_engrose |
| tipo | tipo ('Tesis Aislada'/'Tesis de Jurisprudencia') | 'tesis_historica' | 'criterio_tfja' | 'tratado' | 'sentencia' |
| jerarquia | 'criterio_pjf' (mostly) | 'criterio_pjf' | 'administrativo' | 'convencional' if tipo='ddhh' else 'internacional' | 'pjf_sentencia' |
| titulo | titulo or rubro | rubro | rubro | nombre | rubro (from listing) |
| rubro | rubro | rubro | rubro | nombre | rubro |
| clave | clave | NULL | clave | NULL | num_expediente |
| fecha_emision | fechaPublicacionSemanario (parsed) | NULL | fecha_sesion (parsed) | fecha_adopcion (parsed) | fechaResolucion (parsed) |
| materias | materia (array) | NULL | [materia] | [tipo] | NULL |
| vigente | TRUE (default) | TRUE | status == 'vigente' | estatus check | TRUE |
| status_detalle | 'vigente' | 'historico' | status | estatus | 'pub' |
| metadata | full raw_json | raw_json | raw_text+precedente_raw | detail_html+all fields | listing_json |
| raw_text | texto.contenido | texto | texto + raw_text | (compose from fields) | (parse from HTML) |
