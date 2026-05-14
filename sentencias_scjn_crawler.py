"""
SCJN Sentencias crawler (bj.scjn.gob.mx).

Endpoints (mapped in earlier recon):
  POST /api/v1/bj/busqueda   indice=sentencias_pub
  GET  /api/v1/bj/storage/sentencia?externo=true&fileparams=filename:<basename>

Server declares Content-Type: application/pdf but body is actually HTML with
semantic classes:
  <p class="corte1 datos">    → encabezado / datos del caso
  <p class="corte2 ponente">  → ponente + secretario
  <h1>...</h1>                → titulos en versiones modernas

Schema (sentencias.db):
  sentencias_meta:    listing-extracted metadata (PK id_engrose)
  sentencias_raw:     downloaded HTML body, kept verbatim
  sentencias_section: parsed semantic segments (one row per section type)
  listing_pages:      crawl progress tracker

Three phases:
  Phase 1 (listing): walk /busqueda by año (1995..2025), 50 docs/page, no auth.
  Phase 2 (detail):  for each meta row without raw, fetch the storage endpoint.
  Phase 3 (parse):   walk sentencias_raw → sentencias_section using class markers.

Run:
  .venv/bin/python sentencias_scjn_crawler.py --smoke
  .venv/bin/python sentencias_scjn_crawler.py --phase listing
  .venv/bin/python sentencias_scjn_crawler.py --phase detail
  .venv/bin/python sentencias_scjn_crawler.py --phase parse
  .venv/bin/python sentencias_scjn_crawler.py            # all three
"""
from __future__ import annotations

import argparse
import asyncio
import json
import random
import re
import sqlite3
import sys
import time
from pathlib import Path

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from selectolax.parser import HTMLParser
from rich.console import Console
from rich.progress import Progress, BarColumn, TextColumn, MofNCompleteColumn, TimeRemainingColumn

console = Console()

BJ = "https://bj.scjn.gob.mx/api/v1/bj"
DB_PATH = Path(__file__).parent / "sentencias.db"
PAGE_SIZE = 50
LISTING_CONCURRENCY = 3
DETAIL_CONCURRENCY = 6
SLEEP_BASE = 0.15
SLEEP_JITTER = 0.10
# Defaults: SCJN sentencias_pub spans roughly 1995..present.
DEFAULT_YEARS = list(range(1995, 2026))

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "es-MX,es;q=0.9",
    "Origin": "https://bj.scjn.gob.mx",
    "Referer": "https://bj.scjn.gob.mx/",
    "Content-Type": "application/json",
}


# ----------------------------- DB -----------------------------------------------

def init_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sentencias_meta (
            id_engrose INTEGER PRIMARY KEY,
            asunto_id INTEGER,
            num_expediente TEXT,
            tipo_asunto TEXT,
            organo_radicacion TEXT,
            ponente TEXT,
            fecha_resolucion TEXT,
            anio TEXT,
            votacion TEXT,
            epoca_numero TEXT,
            epoca_nombre TEXT,
            fuente TEXT,
            archivo_url TEXT,
            raw_listing_json TEXT NOT NULL,
            listing_fetched_at TEXT,
            detail_fetched_at TEXT,
            parsed_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sentencias_raw (
            id_engrose INTEGER PRIMARY KEY,
            content_type TEXT,
            body_html TEXT,
            byte_size INTEGER,
            fetched_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sentencias_section (
            section_id INTEGER PRIMARY KEY AUTOINCREMENT,
            id_engrose INTEGER NOT NULL,
            section_index INTEGER NOT NULL,
            section_type TEXT,
            section_class TEXT,
            text TEXT,
            char_count INTEGER
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS listing_pages (
            slice TEXT NOT NULL,
            page INTEGER NOT NULL,
            size INTEGER NOT NULL,
            count INTEGER NOT NULL,
            total_at_fetch INTEGER NOT NULL,
            fetched_at TEXT,
            PRIMARY KEY(slice, page)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_meta_anio ON sentencias_meta(anio)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_meta_expediente ON sentencias_meta(num_expediente)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_meta_asunto ON sentencias_meta(asunto_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_meta_detail_null ON sentencias_meta(id_engrose) WHERE detail_fetched_at IS NULL")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_section_engrose ON sentencias_section(id_engrose)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_section_type ON sentencias_section(section_type)")
    conn.commit()
    return conn


# ----------------------------- HTTP --------------------------------------------

def search_body(year: int, page: int, size: int = PAGE_SIZE) -> dict:
    return {
        "q": "*", "page": page, "size": size,
        "indice": "sentencias_pub", "fuente": None,
        "extractos": 50, "semantica": 0,
        "filtros": {"anio": [str(year)]},
        "sortField": "", "sortDireccion": "",
    }


async def jittered_sleep():
    await asyncio.sleep(SLEEP_BASE + random.uniform(0, SLEEP_JITTER))


@retry(
    reraise=True,
    stop=stop_after_attempt(6),
    wait=wait_exponential(multiplier=1, min=2, max=60),
    retry=retry_if_exception_type((httpx.HTTPError, httpx.RemoteProtocolError, httpx.ReadTimeout)),
)
async def fetch_listing(client: httpx.AsyncClient, year: int, page: int) -> dict:
    r = await client.post(f"{BJ}/busqueda", json=search_body(year, page))
    if r.status_code >= 500 or r.status_code == 429:
        raise httpx.HTTPError(f"server {r.status_code}")
    r.raise_for_status()
    return r.json()


@retry(
    reraise=True,
    stop=stop_after_attempt(6),
    wait=wait_exponential(multiplier=1, min=2, max=60),
    retry=retry_if_exception_type((httpx.HTTPError, httpx.RemoteProtocolError, httpx.ReadTimeout)),
)
async def fetch_storage(client: httpx.AsyncClient, basename: str) -> httpx.Response:
    return await client.get(
        f"{BJ}/storage/sentencia",
        params={"externo": "true", "fileparams": f"filename:{basename}"},
        follow_redirects=True,
    )


def basename_of(archivo_url: str | None) -> str | None:
    if not archivo_url:
        return None
    last = archivo_url.split("/")[-1]
    return last.rsplit(".", 1)[0] if "." in last else last


# ----------------------------- Listing -----------------------------------------

def upsert_meta_from_listing(conn: sqlite3.Connection, doc: dict) -> None:
    ie = doc.get("idEngrose")
    if ie is None:
        return
    epoca = doc.get("epoca") or {}
    conn.execute("""
        INSERT INTO sentencias_meta(
            id_engrose, asunto_id, num_expediente, tipo_asunto, organo_radicacion,
            ponente, fecha_resolucion, anio, votacion, epoca_numero, epoca_nombre,
            fuente, archivo_url, raw_listing_json, listing_fetched_at)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,datetime('now'))
        ON CONFLICT(id_engrose) DO UPDATE SET
            asunto_id=excluded.asunto_id,
            num_expediente=excluded.num_expediente,
            tipo_asunto=excluded.tipo_asunto,
            organo_radicacion=excluded.organo_radicacion,
            ponente=excluded.ponente,
            fecha_resolucion=excluded.fecha_resolucion,
            anio=excluded.anio,
            votacion=excluded.votacion,
            epoca_numero=excluded.epoca_numero,
            epoca_nombre=excluded.epoca_nombre,
            fuente=excluded.fuente,
            archivo_url=excluded.archivo_url,
            raw_listing_json=excluded.raw_listing_json,
            listing_fetched_at=datetime('now')
    """, (
        ie, doc.get("asuntoID"), doc.get("numExpediente"), doc.get("tipoAsunto"),
        doc.get("organoRadicacion"), doc.get("ponente"), doc.get("fechaResolucion"),
        str(doc.get("anio")) if doc.get("anio") is not None else None,
        doc.get("votacion"),
        str(epoca.get("numero")) if epoca.get("numero") is not None else None,
        epoca.get("nombre"),
        doc.get("fuente"), doc.get("archivoURL"),
        json.dumps(doc, ensure_ascii=False),
    ))


async def run_listing(conn: sqlite3.Connection, years: list[int], smoke: bool = False) -> int:
    sem = asyncio.Semaphore(LISTING_CONCURRENCY)

    async with httpx.AsyncClient(http2=True, timeout=60, headers=HEADERS) as client:
        plan: list[tuple[int, int]] = []
        for y in years:
            r = await fetch_listing(client, y, 1)
            total = r.get("total", 0)
            pages = (total + PAGE_SIZE - 1) // PAGE_SIZE
            if smoke:
                pages = min(pages, 1)
            plan.append((y, pages))
            console.print(f"[bold]año {y}:[/] total={total}  pages={pages}")

        grand_total_pages = sum(p for _, p in plan)
        with Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TextColumn("•"),
            TimeRemainingColumn(),
            console=console,
        ) as prog:
            task = prog.add_task("listing", total=grand_total_pages)
            seen = 0

            for year, pages in plan:
                slice_key = f"anio={year}"

                async def one(year: int, slice_key: str, page: int):
                    nonlocal seen
                    async with sem:
                        row = conn.execute(
                            "SELECT count FROM listing_pages WHERE slice=? AND page=?",
                            (slice_key, page),
                        ).fetchone()
                        if row and row[0] > 0:
                            prog.advance(task)
                            return
                        try:
                            j = await fetch_listing(client, year, page)
                        except Exception as e:
                            console.print(f"[red]listing {slice_key}/{page} failed: {e}[/red]")
                            prog.advance(task)
                            return
                        docs = j.get("resultados") or []
                        for d in docs:
                            upsert_meta_from_listing(conn, d)
                            seen += 1
                        conn.execute(
                            "INSERT OR REPLACE INTO listing_pages(slice, page, size, count, total_at_fetch, fetched_at) "
                            "VALUES(?,?,?,?,?,datetime('now'))",
                            (slice_key, page, PAGE_SIZE, len(docs), j.get("total", 0)),
                        )
                        conn.commit()
                        await jittered_sleep()
                        prog.advance(task)

                await asyncio.gather(*[one(year, slice_key, p) for p in range(1, pages + 1)])

        return seen


# ----------------------------- Detail ------------------------------------------

async def run_detail(conn: sqlite3.Connection, limit: int | None = None) -> int:
    sem = asyncio.Semaphore(DETAIL_CONCURRENCY)

    rows = conn.execute("""
        SELECT m.id_engrose, m.archivo_url
        FROM sentencias_meta m
        LEFT JOIN sentencias_raw r ON r.id_engrose = m.id_engrose
        WHERE r.id_engrose IS NULL AND m.archivo_url IS NOT NULL
        ORDER BY m.id_engrose
    """).fetchall()
    if limit:
        rows = rows[:limit]
    console.print(f"[bold]pending detail downloads:[/] {len(rows)}")

    async with httpx.AsyncClient(http2=True, timeout=90, headers=HEADERS) as client:
        with Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TextColumn("•"),
            TimeRemainingColumn(),
            console=console,
        ) as prog:
            task = prog.add_task("detail", total=len(rows))
            ok = 0

            async def one(ie: int, archivo: str):
                nonlocal ok
                async with sem:
                    bn = basename_of(archivo)
                    if not bn:
                        prog.advance(task)
                        return
                    try:
                        r = await fetch_storage(client, bn)
                    except Exception as e:
                        console.print(f"[red]storage {ie} failed: {e}[/red]")
                        prog.advance(task)
                        return
                    if r.status_code != 200 or len(r.content) < 500:
                        prog.advance(task)
                        return
                    body = r.text
                    conn.execute(
                        "INSERT OR REPLACE INTO sentencias_raw(id_engrose, content_type, body_html, byte_size, fetched_at) "
                        "VALUES(?,?,?,?,datetime('now'))",
                        (ie, r.headers.get("content-type", ""), body, len(body)),
                    )
                    conn.execute(
                        "UPDATE sentencias_meta SET detail_fetched_at=datetime('now') WHERE id_engrose=?",
                        (ie,),
                    )
                    conn.commit()
                    ok += 1
                    await jittered_sleep()
                    prog.advance(task)

            BATCH = 200
            for i in range(0, len(rows), BATCH):
                await asyncio.gather(*[one(ie, arch) for ie, arch in rows[i:i + BATCH]])

        return ok


# ----------------------------- Parse --------------------------------------------

CLASS_TYPE_MAP = {
    "corte1 datos": "encabezado",
    "corte2 ponente": "ponente",
    "corte3 resolutivo": "resolutivo",
    "corte3 considerando": "considerando",
}


def classify_section_class(cls: str) -> str:
    cls_l = cls.lower()
    if "datos" in cls_l: return "encabezado"
    if "ponente" in cls_l: return "ponente"
    if "resolutivo" in cls_l: return "resolutivo"
    if "considerando" in cls_l: return "considerando"
    if "antecedente" in cls_l: return "antecedente"
    if "precedente" in cls_l: return "precedente"
    if "voto" in cls_l: return "voto"
    if "corte" in cls_l: return "cuerpo"
    return "otro"


def parse_html_to_sections(html: str) -> list[dict]:
    """Split into semantic sections using the corte* class markers."""
    if not html:
        return []
    tree = HTMLParser(html)
    sections: list[dict] = []

    # Handle three formats observed in pilot:
    #  (a) HTML fragment with <p class="corte1 datos"> ...
    #  (b) Modern doctype HTML with <h1>, <h2>, <p> blocks
    #  (c) Plain <p>...</p> stream.

    paragraphs = tree.css("p, h1, h2, h3")
    if not paragraphs:
        return [{
            "section_index": 0, "section_type": "cuerpo", "section_class": None,
            "text": (tree.body.text(separator=" ", strip=True) if tree.body else ""),
        }]

    idx = 0
    for p in paragraphs:
        cls = p.attributes.get("class", "") or ""
        sec_type = classify_section_class(cls) if cls else "parrafo"
        # Headers always start a new section
        if p.tag in ("h1", "h2", "h3") and not cls:
            sec_type = "titulo" if p.tag == "h1" else "subtitulo"
        text = p.text(separator=" ", strip=True)
        if not text or len(text) < 3:
            continue
        sections.append({
            "section_index": idx,
            "section_type": sec_type,
            "section_class": cls or None,
            "text": text,
        })
        idx += 1

    return sections


def run_parse(conn: sqlite3.Connection, limit: int | None = None) -> int:
    rows = conn.execute("""
        SELECT m.id_engrose, r.body_html
        FROM sentencias_meta m
        JOIN sentencias_raw r ON r.id_engrose = m.id_engrose
        WHERE m.parsed_at IS NULL
        ORDER BY m.id_engrose
    """)
    rows = rows.fetchall()
    if limit:
        rows = rows[:limit]
    console.print(f"[bold]pending parses:[/] {len(rows)}")

    parsed_total = 0
    with Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TextColumn("•"),
        TimeRemainingColumn(),
        console=console,
    ) as prog:
        task = prog.add_task("parse", total=len(rows))
        for ie, html in rows:
            secs = parse_html_to_sections(html or "")
            conn.execute("DELETE FROM sentencias_section WHERE id_engrose=?", (ie,))
            for s in secs:
                conn.execute(
                    "INSERT INTO sentencias_section(id_engrose, section_index, section_type, section_class, text, char_count) "
                    "VALUES(?,?,?,?,?,?)",
                    (ie, s["section_index"], s["section_type"], s["section_class"], s["text"], len(s["text"])),
                )
            conn.execute("UPDATE sentencias_meta SET parsed_at=datetime('now') WHERE id_engrose=?", (ie,))
            conn.commit()
            parsed_total += 1
            prog.advance(task)

    return parsed_total


# ----------------------------- main --------------------------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--phase", choices=["listing", "detail", "parse", "all"], default="all")
    ap.add_argument("--smoke", action="store_true")
    ap.add_argument("--year", action="append", type=int, help="restrict to year (repeatable)")
    args = ap.parse_args()

    years = args.year or DEFAULT_YEARS
    if args.smoke:
        years = [2024]

    conn = init_db(DB_PATH)
    console.print(f"[bold]DB:[/] {DB_PATH}")
    console.print(f"[bold]Years:[/] {years[:5]}{'...' if len(years) > 5 else ''}")

    t0 = time.time()
    loop = asyncio.new_event_loop()
    if args.phase in ("listing", "all"):
        n = loop.run_until_complete(run_listing(conn, years, smoke=args.smoke))
        console.print(f"[green]listing inserts: {n}[/]")
    if args.phase in ("detail", "all"):
        n = loop.run_until_complete(run_detail(conn, limit=50 if args.smoke else None))
        console.print(f"[green]detail downloads: {n}[/]")
    if args.phase in ("parse", "all"):
        n = run_parse(conn, limit=50 if args.smoke else None)
        console.print(f"[green]parse: {n}[/]")

    n_meta = conn.execute("SELECT COUNT(*) FROM sentencias_meta").fetchone()[0]
    n_raw = conn.execute("SELECT COUNT(*) FROM sentencias_raw").fetchone()[0]
    n_sec = conn.execute("SELECT COUNT(*) FROM sentencias_section").fetchone()[0]
    console.print(f"[bold]meta:[/] {n_meta}  [bold]raw:[/] {n_raw}  [bold]sections:[/] {n_sec}")
    console.print(f"[bold]elapsed:[/] {time.time() - t0:.1f}s")


if __name__ == "__main__":
    sys.exit(main())
