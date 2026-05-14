# SCJN Sentencias — Recon Report

Recon-only run. **No bulk download executed.** 10 sentencias piloto en disco.

## TL;DR

| Decision | Verdict |
|---|---|
| Source to crawl | **`bj.scjn.gob.mx/api/v1/bj`** (NOT `sjf2.scjn.gob.mx`). Modern microservice; bigger universe; no auth needed. |
| Strategy | **Crawl `sentencias_pub` directly** — drop the urlSemanario plan (the field is empty in all 17,498 historica rows). |
| File format | Server says `application/pdf` but bodies are **structured HTML**. No PDF parser, no OCR. |
| Universe size | 104,525 sentencias × avg 128 KB HTML ≈ **13.4 GB raw** (likely ~3 GB compressed). |
| Rate limits | 10 sequential pulls at 0.5 s sleep = clean 200s. Imperva cookies present but not enforced for `/api/v1/bj/*`. |
| Blocker | None. Ready to design the full crawler. |

## Endpoints (all GET/POST, no auth)

### Search / discovery
| Verb | URL | Purpose |
|---|---|---|
| GET | `/api/v1/bj/fuentes?group=true` | List all 11 sources (sentencias_pub, tesis, ejecutorias, votos, acuerdos, expedientes_pub, votos_sentencias_pub, legislacion, biblioteca, vtaquigraficas, ccj_cursos, +). |
| GET | `/api/v1/bj/autocompletado?palabra=<q>` | Autocomplete suggestions (handy to enumerate vocabularies). |
| POST | `/api/v1/bj/busqueda` | Universal search. Body shape below. |

### Detail / file
| Verb | URL | Purpose |
|---|---|---|
| (SPA) | `https://bj.scjn.gob.mx/documento/sentencias_pub/<idEngrose>` | Human-facing detail page. |
| GET | `/api/v1/bj/storage/sentencia?externo=true&fileparams=filename:<basename>` | **THE document file.** Returns HTML body. `<basename>` = `archivoURL` stripped of path & extension. |

### Other indices worth noting (not in scope today)
| Index | Rows | Identifier | Purpose |
|---|---|---|---|
| `tesis` | 311,364 | `registroDigital` | Modern + historic tesis. Carries `urlSemanario` and `precedentes` — closes the tesis→sentencia bridge. |
| `ejecutorias` | ? | `registroDigital` | Precedentes (formal). |
| `expedientes_pub` | 150,246 | `asuntoId` | Expediente master records. |
| `votos_sentencias_pub` | ? | `votoId` | Votos particulares attached to sentencias. |
| `legislacion` | ? | `id` | Ordenamientos (federal + state). Covers part of your Tier 1 #3. |

## `/busqueda` payload

```json
{
  "q": "*",                        // empty/wildcard works; "" returns 0
  "page": 1,                       // 1-indexed
  "size": 10,                      // tested up to 100 OK
  "indice": "sentencias_pub",      // or null for all
  "fuente": null,                  // optional idFuente (3 = sentencias_pub)
  "extractos": 200,                // chars of snippet returned
  "semantica": 0,                  // 1 = semantic search (slower)
  "filtros": {                     // documented facets:
    "anio": ["2024"],              //   year (string)
    // others observed at the SPA tab: organoRadicacion, tipoAsunto, ponente,
    // materia, fuente. Need a recon to enumerate the complete filter map.
  },
  "sortField": "",                 // empty => relevance
  "sortDireccion": ""              // "asc" | "desc"
}
```

**Response shape per result:**
```json
{
  "organoJurisdiccional": null,
  "epoca": {"numero":"9","nombre":"Novena Época"},
  "rubro": "EXCEPCIONES NO OPUESTAS. ...",
  "fuente": "Semanario Judicial de la Federación y su Gaceta",
  "fechaResolucion": "13/08/2025",
  "organoRadicacion": "PRIMERA SALA",
  "idEngrose": 233362,
  "asuntoID": 309928,
  "numExpediente": "1309/2023",
  "tipoAsunto": "AMPARO DIRECTO EN REVISIÓN",
  "votacion": "POR UNANIMIDAD DE CINCO VOTOS ...",
  "ponente": "LORETTA ORTIZ AHLF",
  "archivoURL": "1/2023/10/2_309928_7324.docx",
  "anio": "2023",
  "extractos": {}
}
```

Top-level fields: `from`, `fromTo`, `pagina`, `size`, `total`, `totalPaginas`, `resultados[]`.

## Pagination cap (probable)

Same backend family as the historica endpoint (Elasticsearch). `/historicalfile` enforced `max_result_window=10000`. We did NOT yet probe whether `/busqueda` has the same cap (current test only used `q="*"`, `page=1`, `size=10`).

**Plan for the full crawl**: slice by `anio` (1995–2025) and, where any year > 10k, additionally by `organoRadicacion` (Pleno / Primera Sala / Segunda Sala / TCC). Year counts already collected (sample):

| Year | Sentencias | |
|---|---|---|
| 2024 | 2,843 | safe |
| 2020 | 3,029 | safe |
| 2015 | 7,438 | safe |
| 2010 | 4,049 | safe |
| 2005 | 3,903 | safe |
| 2000 | 175 | safe |
| 1995 | 1 | safe |

All under 10 k by year — single-slice loop should work; if any year overflows we sub-slice by sala.

## 10 pilot sentencias

Files in `~/scjn-scraper/sentencias_pilot/` (1.28 MB total):

| idEngrose | Año | Bytes | Format | Notes |
|---|---|---|---|---|
| 233301 | 2024 | 182,878 | HTML doctype | LibreOffice export, modern |
| 232959 | 2024 |  48,552 | HTML doctype | "" |
| 230115 | 2020 |  85,545 | HTML fragment (`<h1>` first) | |
| 226928 | 2020 | 240,246 | HTML fragment | larger sentencia |
| 175545 | 2015 |  89,295 | HTML fragment (`<h1>ACCIÓN DE …`) | |
| 161475 | 2015 |  14,300 | HTML fragment | small / acuerdo |
| 91670 | 2010 | 281,633 | HTML fragment (`<p class="corte1 datos">`) | biggest in pilot |
| 95083 | 2010 | 147,538 | "" | |
| 102506 | 2005 |  66,659 | "" | |
| 97639 | 2005 | 121,339 | "" | |

**Class taxonomy observed in HTML** (semantic markers — basis for downstream extraction):
- `<p class="corte1 datos">` — header line ("AMPARO DIRECTO 309928. SARA …")
- `<p class="corte2 ponente">` — ponente + secretaría
- `<h1>` — type-of-sentencia title (modern files)
- Plain `<p>` blocks for considerandos / resolutivos

## Storage plan (recommended)

Two-table split, per your concern about size:

```sql
CREATE TABLE sentencias_meta (
  id_engrose INTEGER PRIMARY KEY,
  asunto_id INTEGER,
  num_expediente TEXT,
  tipo_asunto TEXT,
  organo_radicacion TEXT,
  ponente TEXT,
  fecha_resolucion TEXT,
  anio TEXT,
  votacion TEXT,
  epoca TEXT,
  archivo_url TEXT,           -- the original path; basename feeds the file API
  raw_search_json TEXT NOT NULL,
  fetched_meta_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE sentencias_raw (
  id_engrose INTEGER PRIMARY KEY REFERENCES sentencias_meta(id_engrose),
  content_type TEXT,
  bytes BLOB NOT NULL,        -- the HTML body, ~128 KB avg
  byte_size INTEGER,
  fetched_doc_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE sentencias_text (
  id_engrose INTEGER PRIMARY KEY REFERENCES sentencias_meta(id_engrose),
  encabezado TEXT,            -- corte1 datos lines
  ponente_section TEXT,       -- corte2 ponente
  considerandos TEXT,         -- the body, flattened
  resolutivos TEXT,           -- tail
  word_count INTEGER,
  extracted_at TEXT DEFAULT (datetime('now'))
);
```

This lets you (a) re-extract text without re-downloading; (b) ship `sentencias_meta` independently as a lightweight index; (c) compress `sentencias_raw.bytes` at rest if needed (zstd cuts these HTML bodies ~5×).

## Risk register

1. **`/busqueda` may cap at 10 k offset** — same ES limit as historica. Mitigated by year-slicing (no slice >10k observed).
2. **Imperva backoff under sustained load** — only 10 reqs tested; full crawl will be ~100 k. Plan: 0.5–1 s sleep, jitter, monitor for 429/503; revert to Playwright cookie refresh if blocked.
3. **HTML content has no canonical schema** — fragments vary by year (modern files have full DOCTYPE; older only `<p class>`). Mitigation: parser handles both via heuristics on `corte1`/`corte2` classes + `<h1>` presence.
4. **`fileparams` format** — currently `filename:<basename>`. Need 1 negative test (try an idEngrose with no `archivoURL` to see fallback). Not blocking.
5. **Older sentencias missing** — bj.scjn `sentencias_pub` likely starts ~1995 (1995 only had 1 hit). For older we'd need `expedientes_pub` or external sources (Compilación, microfilm). Out of scope.

## Recommended next steps (in order)

1. Enumerate full filter taxonomy via Playwright (`organoRadicacion`, `tipoAsunto`, `materia` lists) → 1 hour.
2. Probe `/busqueda` with `page` walk to confirm/refute the 10 k cap → 5 minutes.
3. Build `sentencias_crawler.py` (analogous to `scraper.py`, year-sliced) → 1 hour.
4. Run full crawl ~104 k items @ 0.5 s ≈ **14–17 hours**, monitored under `tmux` (or `nohup`). Expected output: ~13 GB on disk.
5. Build `extract_text.py` parser using selectolax + the class taxonomy → 1 hour, runs in parallel with crawl.
6. (Stretch) Pull `tesis` index (311 k) to materialize the tesis↔sentencia bridge via `precedentes` field.

## Files added this session

```
~/scjn-scraper/
  recon_sentencias.py     # initial bj.scjn endpoint discovery
  probe_modern.py         # confirmed sjf2 historicalfile has no modern data
  probe_bj.py             # bj.scjn fuente + busqueda probe
  probe_files.py          # FAILED file-host guess (SPA fallback trap)
  recon_detail.py         # Playwright click-through; found storage endpoint
  pilot_v2.py             # 10-piloto download via real endpoint
  api-map-sentencias.json # capture: bj.scjn first contact
  api-map-detail.json     # capture: detail page + storage URL
  sentencias_pilot/       # 10 HTML files + pilot_v2_meta.json
  sentencias-recon.md     # (this file)
```
