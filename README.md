# Cerebro Legal — Corpus Jurídico Mexicano

Sistema de ingestión + razonamiento legal sobre el corpus jurídico mexicano federal.
Diseñado para alimentar un pipeline de búsqueda semántica con pgvector + reranker
+ wiki LLM para redacción de estrategia legal.

## Arquitectura

```
crawlers (SQLite por fuente)  →  export to corpus  →  Postgres + pgvector
                                                       ↓
                                              ANN top-300 → reranker → top-100 → LLM
```

## Corpus actual (en el host rasputinmac:~/scjn-scraper/)

| Fuente | Tabla(s) SQLite | Rows | Notas |
|---|---|---|---|
| SCJN tesis modernas (5ª–11ª Época) | `tesis.db:tesis` | 310,610 | con bridges (precedentes, ejecutorias, votos, materias) |
| SCJN históricas (1ª–4ª Época, 1871-1907) | `sjf.db:tesis` | 17,498 | narrativas siglo XIX/XX |
| TFJA tesis (Tribunal Fiscal/Administrativo) | `tfja.db:tfja_tesis` | en progreso (~5-8k esperadas) | PDF parsed |
| SRE tratados internacionales | `sre.db:sre_tratados` | 1,505 | 90 DDHH identificados |

## Stack

- Python 3.12 + `uv`
- `httpx[http2]` para crawling JSON APIs
- `playwright` (chromium headless) para sitios SPA
- `pdfplumber` + `pymupdf` para PDFs
- `selectolax` para HTML parsing
- `tenacity` para retries
- SQLite con WAL para ingestion
- Postgres + pgvector (target de migración)

## Scripts principales

| Script | Propósito |
|---|---|
| `tesis_crawler.py` | SCJN tesis modernas (bj.scjn.gob.mx) |
| `scraper.py` | SCJN históricas (sjf2.scjn.gob.mx) |
| `tfja_crawler.py` | TFJA criterios (PDFs sequential ID 1..48500) |
| `sre_crawler.py` | SRE tratados (Playwright on cja.sre.gob.mx) |
| `un_treaty_bridge.py` | Cross-ref UN Treaty Collection para reservas |
| `tfja_status_update.py` | Marca tesis suspendidas/modificadas/derogadas |
| `reparse_tfja.py` / `reparse_sre.py` | Re-extract de fields sin re-bajar |

## Pendientes (roadmap)

- Sentencias SCJN completas (104,525 docs)
- Corte IDH (CIJUR) ~270 docs
- DOF tratados (texto autoritativo)
- Leyes federales (Diputados ~280)
- TEPJF tesis completas (~12k)
- Citation extraction sobre `texto_justificacion` (genera grafo de citas)
- Export-to-corpus migration script
- Postgres + pgvector setup

Schema de migración en [`CORPUS_SCHEMA.md`](./CORPUS_SCHEMA.md).
