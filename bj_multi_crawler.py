"""
Multi-index crawler for bj.scjn sources we haven't touched yet.

Pulls listings from /api/v1/bj/busqueda for:
  - acuerdos          (3,520)
  - legislacion       (110,869)
  - vtaquigraficas    (3,913)
  - expedientes_pub   (150,261)
  - votos_sentencias_pub (13,477)
  - biblioteca        (145,252)

Each gets its own SQLite DB with the listing payload as raw_json plus a few
extracted columns.

We stick to listings (no detail-page fetch) for these because the listing
already contains the substantive metadata, and a single-pass crawl over the
total is feasible at scale.
"""
from __future__ import annotations
import argparse, asyncio, json, random, sqlite3, sys
from pathlib import Path
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from rich.console import Console
from rich.progress import Progress, BarColumn, TextColumn, MofNCompleteColumn, TimeRemainingColumn

console = Console()
HERE = Path(__file__).parent
BJ = "https://bj.scjn.gob.mx/api/v1/bj"
HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json",
           "Content-Type": "application/json", "Origin": "https://bj.scjn.gob.mx",
           "Referer": "https://bj.scjn.gob.mx/"}
PAGE_SIZE = 50
CONCURRENCY = 3   # be polite — these are big indices
SLEEP = 0.18

# Indices with their key extraction
INDICES_CONFIG = {
    "acuerdos": {
        "db": "acuerdos.db",
        "table": "acuerdos",
        "id_field": "registroDigital",
        "columns": ["rubro", "fuente", "fechaPublicacionSemanario", "instancia", "organoJurisdiccional"],
    },
    "legislacion": {
        "db": "legislacion.db",
        "table": "legislacion",
        "id_field": "id",
        "columns": ["vigencia", "estado", "fechaPublicado", "ordenamiento", "materia", "ambito", "pais"],
    },
    "vtaquigraficas": {
        "db": "vtaquigraficas.db",
        "table": "vtaquigraficas",
        "id_field": "idVT",
        "columns": ["organoJurisdiccional", "fechaSesion", "instancia", "anio"],
    },
    "expedientes_pub": {
        "db": "expedientes.db",
        "table": "expedientes",
        "id_field": "asuntoId",
        "columns": ["estado", "expediente", "fechaResolucion", "tema", "tipoAsunto", "materias", "pertenencia"],
    },
    "votos_sentencias_pub": {
        "db": "votos_sent.db",
        "table": "votos_sent",
        "id_field": "votoId",
        "columns": ["ministroFirma", "urlInternet", "tipoVotoDocumento", "expediente", "asuntoId",
                    "textoVoto", "tipoVoto", "fechaCierreCompleto", "tipoAsunto"],
    },
    "biblioteca": {
        "db": "biblioteca.db",
        "table": "biblioteca",
        "id_field": "publicacionId",
        "columns": ["titulo", "resumen", "autor", "tema", "coleccion", "urlPrimo"],
    },
}


def init_db(cfg: dict) -> sqlite3.Connection:
    conn = sqlite3.connect(HERE / cfg["db"])
    conn.execute("PRAGMA journal_mode=WAL")
    cols = ", ".join(f'"{c}" TEXT' for c in cfg["columns"])
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {cfg['table']} (
            id_native TEXT PRIMARY KEY,
            {cols},
            raw_json TEXT,
            fetched_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS pages (
            page INTEGER PRIMARY KEY,
            size INTEGER,
            count INTEGER,
            fetched_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.commit()
    return conn


def search_body(indice: str, page: int, size: int = PAGE_SIZE) -> dict:
    return {"q": "*", "page": page, "size": size, "indice": indice, "fuente": None,
            "extractos": 50, "semantica": 0, "filtros": {},
            "sortField": "", "sortDireccion": ""}


@retry(reraise=True, stop=stop_after_attempt(5),
       wait=wait_exponential(multiplier=1, min=2, max=30),
       retry=retry_if_exception_type((httpx.HTTPError, httpx.RemoteProtocolError, httpx.ReadTimeout)))
async def fetch_page(client: httpx.AsyncClient, indice: str, page: int) -> dict:
    r = await client.post(f"{BJ}/busqueda", json=search_body(indice, page))
    if r.status_code >= 500 or r.status_code == 429:
        raise httpx.HTTPError(f"{r.status_code}")
    r.raise_for_status()
    return r.json()


async def crawl_index(indice: str, cfg: dict, page_cap: int | None = None):
    conn = init_db(cfg)
    sem = asyncio.Semaphore(CONCURRENCY)

    async with httpx.AsyncClient(http2=True, headers=HEADERS, timeout=45) as client:
        # discover total
        first = await fetch_page(client, indice, 1)
        total = first.get("total", 0)
        pages = (total + PAGE_SIZE - 1) // PAGE_SIZE
        if page_cap:
            pages = min(pages, page_cap)
        console.print(f"[bold]{indice}:[/] total={total}  pages={pages}")

        with Progress(TextColumn(indice), BarColumn(), MofNCompleteColumn(), TextColumn("•"), TimeRemainingColumn(), console=console) as prog:
            task = prog.add_task(indice, total=pages)

            async def grab(page: int):
                async with sem:
                    if conn.execute("SELECT 1 FROM pages WHERE page=?", (page,)).fetchone():
                        prog.advance(task); return
                    if page == 1:
                        j = first
                    else:
                        try:
                            j = await fetch_page(client, indice, page)
                        except Exception as e:
                            console.print(f"[red]{indice} p{page}: {e}[/]")
                            prog.advance(task); return
                    res = j.get("resultados", []) or []
                    for doc in res:
                        nid = doc.get(cfg["id_field"])
                        if nid is None: continue
                        cols = cfg["columns"]
                        values = [str(doc.get(c, "")) if doc.get(c) is not None else None for c in cols]
                        placeholders = ",".join(["?"] * (len(cols) + 2))
                        conn.execute(
                            f"INSERT OR REPLACE INTO {cfg['table']}(id_native, "
                            + ",".join(f'"{c}"' for c in cols)
                            + ", raw_json) VALUES(" + placeholders + ")",
                            [str(nid)] + values + [json.dumps(doc, ensure_ascii=False)]
                        )
                    conn.execute("INSERT OR REPLACE INTO pages(page, size, count, fetched_at) VALUES(?,?,?,datetime('now'))",
                                 (page, PAGE_SIZE, len(res)))
                    conn.commit()
                    await asyncio.sleep(SLEEP + random.uniform(0, 0.1))
                    prog.advance(task)

            BATCH = 200
            for i in range(1, pages + 1, BATCH):
                await asyncio.gather(*[grab(p) for p in range(i, min(i + BATCH, pages + 1))])

    n = conn.execute(f"SELECT COUNT(*) FROM {cfg['table']}").fetchone()[0]
    console.print(f"[green]{indice}: {n} rows[/]")


async def main_async(indices: list[str], page_cap: int | None):
    for ind in indices:
        if ind not in INDICES_CONFIG:
            console.print(f"[red]unknown index: {ind}[/]")
            continue
        await crawl_index(ind, INDICES_CONFIG[ind], page_cap=page_cap)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--index", action="append",
                    help="repeatable; default = all small ones (skip biblioteca/expedientes_pub)")
    ap.add_argument("--page-cap", type=int, help="limit pages for testing")
    args = ap.parse_args()
    indices = args.index or ["acuerdos", "vtaquigraficas", "votos_sentencias_pub", "legislacion"]
    # Skip biblioteca + expedientes_pub by default (~300k combined, gigantic)
    asyncio.run(main_async(indices, args.page_cap))


if __name__ == "__main__":
    sys.exit(main())
