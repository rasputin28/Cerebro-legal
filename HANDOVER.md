# Cerebro Legal — Handover

Sistema de ingestión + razonamiento sobre el corpus jurídico mexicano. Diseñado
para alimentar un pipeline de búsqueda semántica con pgvector + cross-encoder
reranker (300 chunks → 100) + LLM para redacción de estrategia legal.

## Estado de la sesión actual

| Métrica | Valor |
| --- | --- |
| Fuentes cubiertas | **17 sources oficiales** |
| Documents en corpus.db | 758,659+ (creciendo) |
| Chunks indexables | 1.57M (sentencias parse añade ~6-8M más) |
| Citations grafo | 133,079 |
| UN reservas bridged | 52 |
| TFJA tesis | 18,841+ |
| Legis estatales | 7 estados (3,021 leyes en curso) |
| GitHub repo | github.com/rasputin28/Cerebro-legal |

## Fuentes cubiertas

### Completas ✅
- **SCJN tesis modernas** (310,607) — bj.scjn.gob.mx
- **SCJN históricas** (17,498) — sjf2.scjn.gob.mx
- **SCJN ejecutorias** (17,442) — formal sentences originating tesis
- **SCJN votos** (4,935) — votos individuales de Ministros
- **SCJN acuerdos** (3,520)
- **SCJN vtaquigráficas** (3,911 + 3,911 detail) — court session transcripts
- **SCJN votos sentencias** (13,477)
- **SCJN biblioteca** (145,252) — academic publications
- **SCJN expedientes_pub** (9,991+)
- **SCJN sentencias_pub** (104,404 listing + 104,079 detail) — full HTML body
- **bj.scjn legislación** (110,869) — federal+estatal index
- **TFJA criterios** (18,841+, in progress) — fiscal/administrative
- **SRE tratados** (1,505 + 52 UN reservas)
- **Corte IDH** (510+ Serie C contenciosas)
- **Diputados leyes federales** (317 / 1,267 archivos)
- **Citation graph** (133,079 article cites, 1,167 tratado cites, 2,612 tesis cites)
- **Legis estatales QRoo + Yucatán + CDMX + Durango + Guanajuato + Tabasco + Veracruz** (3,021 PDFs)
- **DOF** (12,719+ Diario Oficial publications since 2020)

### Deferidas (hostiles, requieren días de desarrollo) ❌
- **TEPJF**: Radware Bot Manager + ASP.NET WebForms (5 approaches failed)
- **CNDH**: Angular SPA con API async no expuesta
- **SAT**: SvelteKit SPA con bot protection
- **25 de 32 TSJ estatales**: portales custom no crawleables (cada uno = proyecto separado)

## Schema (pgvector target)

`CORPUS_SCHEMA.md` detalla el target Postgres. Estructura clave:

```
corpus_document (PK doc_id, source, native_id, tipo, jerarquia, ...)
  ├── corpus_chunk (FK, chunk_type, text, embedding vector(1536))
  │     └─ HNSW index for ANN top-300
  └── corpus_citation (FK, norm_type, norm_canon, ...)

search_query / search_retrieval (audit del reranker)
crawl_log (observability)
```

Chunk types semánticos: rubro / titulo / hechos / criterio / justificacion /
considerando / resolutivo / precedente / cuerpo / parrafo / clausula / reserva
/ articulo / voto / etc.

## Scripts en el repo

| Script | Función |
| --- | --- |
| `tesis_crawler.py` | SCJN tesis modernas (bj.scjn) |
| `scraper.py` | SCJN históricas |
| `tfja_crawler.py` + `reparse_tfja.py` | TFJA PDFs + regex re-extract |
| `tfja_status_update.py` | JUR_SUSP_MOD → status updates |
| `sentencias_scjn_crawler.py` | SCJN sentencias (listing + detail) |
| `parse_sentencias_streaming.py` | HTML body → semantic sections (mem-bounded) |
| `corteidh_crawler.py` | Corte IDH Serie C/A/E PDFs |
| `dof_crawler.py` | DOF via daily index walk |
| `diputados_crawler.py` | Leyes federales Diputados |
| `sre_crawler.py` + `reparse_sre.py` | Tratados SRE |
| `un_treaty_bridge.py` | UN Treaty Collection reservas (40 entries map) |
| `ejecutorias_crawler.py` / `votos_crawler.py` | SCJN bridge from tesis |
| `bj_multi_crawler.py` | acuerdos/vtaq/votos_sent/legis/biblioteca/expedientes |
| `vtaq_detail_crawler.py` | vtaq full transcripts |
| `legis_estatales_crawler.py` | 7 state congresses |
| `citation_extract.py` | 133k normative citations |
| `export_to_corpus.py` | All sources → unified corpus.db |
| `retrieval_test.py` | Validate FTS 300+ chunk retrieval |
| `validate_pgvector_workflow.py` | End-to-end ANN→rerank topology test |
| `sync_to_atalaria.py` | corpus.db → Atalaria Postgres + pgvector |
| `embed_chunks.py` | text-embedding-3-small batch over chunks |

## Próximos pasos

1. **Cuando OPENAI_API_KEY esté disponible**: `embed_chunks.py` embed ~1.5M+ chunks
2. **Cuando ATALARIA_PG_URL esté disponible**: `sync_to_atalaria.py --reset`
3. **Reranker**: integrar `bge-reranker-v2-m3` (local) o Cohere rerank API
4. **MCP wiring**: ya conectado a Postgres de Atalaria → corpus queryable desde Claude
5. **Backup**: rsync corpus.db a S3/R2/B2 cuando estabilice (~$0.50/mes 25 GB)

## Operación headless del host

`rasputinmac@192.168.68.104` — Ubuntu 24.04, 16 GB RAM, 920 GB libres.
- Crawlers viven en `~/scjn-scraper/`
- venv: `.venv/bin/python` (uv-managed)
- Para apagar GUI permanentemente: `bash ~/scjn-scraper/sudo-setup.sh` (requiere sudo)

## Conocido / por mejorar

- TFJA `tfja_missing` table empty (bug en `mark_missing` con concurrency 6) — los datos válidos (>18k tesis) están limpios, solo falta auditoría de skips
- Sentencias section parse: 39% done streaming, terminará en ~10 min
- Embedding pipeline asume `text-embedding-3-small` (1536 dim); para upgrade a 3072 (large), drop pgvector index → re-embed → rebuild HNSW
