"""
SCJN Tesis crawler (modern bj.scjn.gob.mx index).

Two phases:
  Phase 1 — listing: paginate /api/v1/bj/busqueda with indice=tesis,
            slicing by epoca.numero. Insert basic metadata.
  Phase 2 — detail: for each registroDigital not yet detailed,
            GET /api/v1/bj/documento/tesis/{rd} and fill semantic columns
            + bridge tables (precedente, ejecutoria, voto, materia).

Resume-safe. Async concurrency. Jittered sleep.

Run:
    .venv/bin/python tesis_crawler.py                 # full crawl
    .venv/bin/python tesis_crawler.py --phase listing # listing only
    .venv/bin/python tesis_crawler.py --phase detail  # detail only
    .venv/bin/python tesis_crawler.py --epoca 11      # one slice
    .venv/bin/python tesis_crawler.py --smoke         # 2 listing pages + 50 details
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
from typing import Any

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from rich.console import Console
from rich.progress import Progress, BarColumn, TextColumn, MofNCompleteColumn, TimeRemainingColumn

console = Console()

BJ = "https://bj.scjn.gob.mx/api/v1/bj"
DB_PATH = Path(__file__).parent / "tesis.db"
PAGE_SIZE = 50
LISTING_CONCURRENCY = 3
DETAIL_CONCURRENCY = 6
SLEEP_BASE = 0.15
SLEEP_JITTER = 0.10
EPOCHS = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11"]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "es-MX,es;q=0.9",
    "Origin": "https://bj.scjn.gob.mx",
    "Referer": "https://bj.scjn.gob.mx/",
    "Content-Type": "application/json",
}

PRECEDENTE_RE = re.compile(
    r"data-asunto=['\"]([^'\"]+)['\"][^>]*data-expediente=['\"]([^'\"]+)['\"]",
    re.I,
)


# ---------- DB setup ----------

def init_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tesis (
            registro_digital INTEGER PRIMARY KEY,
            rubro TEXT,
            titulo TEXT,
            subtitulo TEXT,
            tipo TEXT,
            epoca_numero TEXT,
            epoca_nombre TEXT,
            clave TEXT,
            fuente TEXT,
            instancia TEXT,
            organo_jurisdiccional TEXT,
            circuito TEXT,
            pagina INTEGER,
            libro TEXT,
            tomo TEXT,
            mes INTEGER,
            anio INTEGER,
            volumen TEXT,
            fecha_publ_semanario TEXT,
            fecha_publ_obligatoriedad TEXT,
            formas_integracion TEXT,
            notas TEXT,
            huella_digital TEXT,
            texto_contenido TEXT,
            texto_hechos TEXT,
            texto_justificacion TEXT,
            texto_criterios_juridicos TEXT,
            tipo_documento INTEGER,
            listing_json TEXT,
            detail_json TEXT,
            listing_fetched_at TEXT,
            detail_fetched_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tesis_materia (
            registro_digital INTEGER NOT NULL,
            materia TEXT NOT NULL,
            PRIMARY KEY(registro_digital, materia)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tesis_precedente (
            registro_digital INTEGER NOT NULL,
            orden INTEGER NOT NULL,
            asunto TEXT,
            expediente TEXT,
            texto_full TEXT,
            PRIMARY KEY(registro_digital, orden)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tesis_ejecutoria_ref (
            registro_digital INTEGER NOT NULL,
            rd_ejecutoria INTEGER NOT NULL,
            PRIMARY KEY(registro_digital, rd_ejecutoria)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tesis_voto_ref (
            registro_digital INTEGER NOT NULL,
            rd_voto INTEGER NOT NULL,
            PRIMARY KEY(registro_digital, rd_voto)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS listing_pages (
            epoca TEXT NOT NULL,
            page INTEGER NOT NULL,
            size INTEGER NOT NULL,
            count INTEGER NOT NULL,
            fetched_at TEXT,
            PRIMARY KEY(epoca, page)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tesis_epoca ON tesis(epoca_numero)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tesis_instancia ON tesis(instancia)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tesis_detail_null ON tesis(registro_digital) WHERE detail_fetched_at IS NULL")
    conn.commit()
    return conn


# ---------- HTTP helpers ----------

async def jittered_sleep():
    await asyncio.sleep(SLEEP_BASE + random.uniform(0, SLEEP_JITTER))


def search_body(epoca: str, page: int, size: int = PAGE_SIZE) -> dict:
    return {
        "q": "*",
        "page": page,
        "size": size,
        "indice": "tesis",
        "fuente": None,
        "extractos": 50,
        "semantica": 0,
        "filtros": {"epoca.numero": [epoca]},
        "sortField": "",
        "sortDireccion": "",
    }


@retry(
    reraise=True,
    stop=stop_after_attempt(6),
    wait=wait_exponential(multiplier=1, min=2, max=60),
    retry=retry_if_exception_type((httpx.HTTPError, httpx.RemoteProtocolError, httpx.ReadTimeout)),
)
async def fetch_listing(client: httpx.AsyncClient, epoca: str, page: int) -> dict:
    r = await client.post(f"{BJ}/busqueda", json=search_body(epoca, page))
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
async def fetch_detail(client: httpx.AsyncClient, rd: int) -> dict | None:
    r = await client.get(f"{BJ}/documento/tesis/{rd}")
    if r.status_code == 404:
        return None
    if r.status_code >= 500 or r.status_code == 429:
        raise httpx.HTTPError(f"server {r.status_code}")
    r.raise_for_status()
    return r.json()


# ---------- DB writers ----------

def upsert_listing_row(conn: sqlite3.Connection, doc: dict, epoca: str) -> None:
    rd = doc.get("registroDigital")
    if rd is None:
        return
    loc = doc.get("localizacion") or {}
    ep = doc.get("epoca") or {}
    conn.execute(
        """
        INSERT INTO tesis(
            registro_digital, rubro, tipo, epoca_numero, epoca_nombre, clave, fuente,
            instancia, organo_jurisdiccional, circuito, pagina, libro, tomo, mes, anio,
            volumen, fecha_publ_semanario, listing_json, listing_fetched_at)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,datetime('now'))
        ON CONFLICT(registro_digital) DO UPDATE SET
            rubro=excluded.rubro, tipo=excluded.tipo,
            epoca_numero=excluded.epoca_numero, epoca_nombre=excluded.epoca_nombre,
            clave=excluded.clave, fuente=excluded.fuente,
            instancia=excluded.instancia, organo_jurisdiccional=excluded.organo_jurisdiccional,
            circuito=excluded.circuito,
            pagina=excluded.pagina, libro=excluded.libro, tomo=excluded.tomo,
            mes=excluded.mes, anio=excluded.anio, volumen=excluded.volumen,
            fecha_publ_semanario=excluded.fecha_publ_semanario,
            listing_json=excluded.listing_json,
            listing_fetched_at=datetime('now')
        """,
        (
            rd, doc.get("rubro"), doc.get("tipo"),
            ep.get("numero") or epoca, ep.get("nombre"),
            doc.get("clave"), doc.get("fuente"),
            doc.get("instancia"), doc.get("organoJurisdiccional"), doc.get("circuito"),
            loc.get("pagina"), loc.get("libro"), loc.get("tomo"), loc.get("mes"), loc.get("anio"),
            doc.get("volumen"), doc.get("fechaPublicacionSemanario"),
            json.dumps(doc, ensure_ascii=False),
        ),
    )


def update_detail_row(conn: sqlite3.Connection, doc: dict) -> None:
    rd = doc.get("registroDigital") or doc.get("id")
    if rd is None:
        return
    loc = doc.get("localizacion") or {}
    ep = doc.get("epoca") or {}
    texto = doc.get("texto") if isinstance(doc.get("texto"), dict) else {}

    conn.execute(
        """
        UPDATE tesis SET
            titulo=?, subtitulo=?,
            tipo=COALESCE(?, tipo),
            epoca_numero=COALESCE(?, epoca_numero),
            epoca_nombre=COALESCE(?, epoca_nombre),
            clave=COALESCE(?, clave),
            fuente=COALESCE(?, fuente),
            instancia=COALESCE(?, instancia),
            organo_jurisdiccional=COALESCE(?, organo_jurisdiccional),
            circuito=COALESCE(?, circuito),
            pagina=COALESCE(?, pagina),
            libro=COALESCE(?, libro),
            tomo=COALESCE(?, tomo),
            mes=COALESCE(?, mes),
            anio=COALESCE(?, anio),
            volumen=COALESCE(?, volumen),
            fecha_publ_semanario=COALESCE(?, fecha_publ_semanario),
            fecha_publ_obligatoriedad=?,
            formas_integracion=?,
            notas=?,
            huella_digital=?,
            texto_contenido=?,
            texto_hechos=?,
            texto_justificacion=?,
            texto_criterios_juridicos=?,
            tipo_documento=?,
            detail_json=?,
            detail_fetched_at=datetime('now')
        WHERE registro_digital=?
        """,
        (
            doc.get("titulo"), doc.get("subtitulo"),
            doc.get("tipo"), ep.get("numero"), ep.get("nombre"),
            doc.get("clave"), doc.get("fuente"),
            doc.get("instancia"), doc.get("organoJurisdiccional"), doc.get("circuito"),
            loc.get("pagina"), loc.get("libro"), loc.get("tomo"), loc.get("mes"), loc.get("anio"),
            doc.get("volumen"), doc.get("fechaPublicacionSemanario"),
            doc.get("fechaPublicacionObligatoriedad"),
            doc.get("formasIntegracion"),
            doc.get("notas"),
            doc.get("huellaDigital"),
            texto.get("contenido"),
            texto.get("hechos"),
            texto.get("justificacion"),
            texto.get("criteriosJuridicos"),
            doc.get("tipoDocumento"),
            json.dumps(doc, ensure_ascii=False),
            rd,
        ),
    )
    # If the listing row didn't exist (rare), insert minimal then update again.
    if conn.total_changes == 0:
        conn.execute(
            "INSERT OR IGNORE INTO tesis(registro_digital, detail_json, detail_fetched_at) "
            "VALUES(?, ?, datetime('now'))",
            (rd, json.dumps(doc, ensure_ascii=False)),
        )

    # Bridges
    materias = doc.get("materia") or []
    if isinstance(materias, list):
        conn.execute("DELETE FROM tesis_materia WHERE registro_digital=?", (rd,))
        for m in materias:
            if m:
                conn.execute(
                    "INSERT OR IGNORE INTO tesis_materia(registro_digital, materia) VALUES(?,?)",
                    (rd, m),
                )

    nps = doc.get("notaPrecedente") or []
    if isinstance(nps, list):
        conn.execute("DELETE FROM tesis_precedente WHERE registro_digital=?", (rd,))
        orden = 0
        for np in nps:
            if not isinstance(np, dict):
                continue
            t = np.get("texto") or ""
            matches = PRECEDENTE_RE.findall(t)
            if matches:
                for asunto, expediente in matches:
                    conn.execute(
                        "INSERT OR IGNORE INTO tesis_precedente(registro_digital, orden, asunto, expediente, texto_full) "
                        "VALUES(?,?,?,?,?)",
                        (rd, orden, asunto.strip(), expediente.strip(), t),
                    )
                    orden += 1
            elif t:
                # No structured tag, but text exists — keep as-is for later re-parse.
                conn.execute(
                    "INSERT OR IGNORE INTO tesis_precedente(registro_digital, orden, asunto, expediente, texto_full) "
                    "VALUES(?,?,NULL,NULL,?)",
                    (rd, orden, t),
                )
                orden += 1

    for k, tbl in (("rdEjecutoria", "tesis_ejecutoria_ref"), ("rdVotos", "tesis_voto_ref")):
        vals = doc.get(k) or []
        if isinstance(vals, list):
            col = "rd_ejecutoria" if k == "rdEjecutoria" else "rd_voto"
            conn.execute(f"DELETE FROM {tbl} WHERE registro_digital=?", (rd,))
            for v in vals:
                if isinstance(v, int):
                    conn.execute(
                        f"INSERT OR IGNORE INTO {tbl}(registro_digital, {col}) VALUES(?,?)",
                        (rd, v),
                    )


# ---------- Phase 1: listing ----------

async def run_listing(conn: sqlite3.Connection, epocas: list[str], smoke_pages: int | None = None) -> None:
    sem = asyncio.Semaphore(LISTING_CONCURRENCY)

    async with httpx.AsyncClient(http2=True, timeout=60, headers=HEADERS) as client:
        # 1. discover total per epoca
        plan: list[tuple[str, int]] = []  # (epoca, total_pages)
        for ep in epocas:
            r = await client.post(f"{BJ}/busqueda", json=search_body(ep, 1, size=1))
            total = r.json().get("total", 0)
            pages = (total + PAGE_SIZE - 1) // PAGE_SIZE
            if smoke_pages:
                pages = min(pages, smoke_pages)
            plan.append((ep, pages))
            console.print(f"[bold]epoca {ep}:[/] {total} tesis → {pages} pages")

        with Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TextColumn("•"),
            TimeRemainingColumn(),
            console=console,
        ) as prog:
            for ep, pages in plan:
                if pages == 0:
                    continue
                task = prog.add_task(f"epoca {ep} listing", total=pages)

                async def grab(page: int):
                    async with sem:
                        row = conn.execute(
                            "SELECT count FROM listing_pages WHERE epoca=? AND page=?",
                            (ep, page),
                        ).fetchone()
                        if row and row[0] > 0:
                            prog.advance(task)
                            return
                        try:
                            j = await fetch_listing(client, ep, page)
                        except Exception as e:
                            console.print(f"[red]listing {ep}/{page} failed: {e}[/red]")
                            prog.advance(task)
                            return
                        docs = j.get("resultados") or []
                        for d in docs:
                            upsert_listing_row(conn, d, ep)
                        conn.execute(
                            "INSERT OR REPLACE INTO listing_pages(epoca, page, size, count, fetched_at) "
                            "VALUES(?,?,?,?,datetime('now'))",
                            (ep, page, PAGE_SIZE, len(docs)),
                        )
                        conn.commit()
                        await jittered_sleep()
                        prog.advance(task)

                await asyncio.gather(*[grab(p) for p in range(1, pages + 1)])


# ---------- Phase 2: detail ----------

async def run_detail(conn: sqlite3.Connection, limit: int | None = None) -> None:
    sem = asyncio.Semaphore(DETAIL_CONCURRENCY)

    rows = conn.execute(
        "SELECT registro_digital FROM tesis WHERE detail_fetched_at IS NULL ORDER BY registro_digital"
    ).fetchall()
    if limit:
        rows = rows[:limit]
    rds = [r[0] for r in rows]
    console.print(f"[bold]Pending details:[/] {len(rds)}")
    if not rds:
        return

    async with httpx.AsyncClient(http2=True, timeout=60, headers=HEADERS) as client:
        with Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TextColumn("•"),
            TimeRemainingColumn(),
            console=console,
        ) as prog:
            task = prog.add_task("detail", total=len(rds))

            async def grab(rd: int):
                async with sem:
                    try:
                        j = await fetch_detail(client, rd)
                    except Exception as e:
                        console.print(f"[red]detail {rd} failed: {e}[/red]")
                        prog.advance(task)
                        return
                    if j:
                        update_detail_row(conn, j)
                        conn.commit()
                    await jittered_sleep()
                    prog.advance(task)

            # Batch to avoid huge gather list.
            BATCH = 200
            for i in range(0, len(rds), BATCH):
                await asyncio.gather(*[grab(rd) for rd in rds[i:i + BATCH]])


# ---------- main ----------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--phase", choices=["listing", "detail", "all"], default="all")
    ap.add_argument("--epoca", action="append", help="restrict to one or more épocas; defaults to all 1-11")
    ap.add_argument("--smoke", action="store_true", help="smoke run: 2 listing pages/époch + first 50 details")
    args = ap.parse_args()

    epocas = args.epoca or EPOCHS

    conn = init_db(DB_PATH)
    console.print(f"[bold]DB:[/] {DB_PATH}")
    console.print(f"[bold]Épocas:[/] {epocas}")

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    t0 = time.time()
    try:
        if args.phase in ("listing", "all"):
            loop.run_until_complete(run_listing(conn, epocas, smoke_pages=2 if args.smoke else None))
        if args.phase in ("detail", "all"):
            loop.run_until_complete(run_detail(conn, limit=50 if args.smoke else None))
    finally:
        loop.close()

    n_listed = conn.execute("SELECT COUNT(*) FROM tesis WHERE listing_fetched_at IS NOT NULL").fetchone()[0]
    n_detail = conn.execute("SELECT COUNT(*) FROM tesis WHERE detail_fetched_at IS NOT NULL").fetchone()[0]
    n_prec = conn.execute("SELECT COUNT(*) FROM tesis_precedente").fetchone()[0]
    console.print(f"[green]Listing rows: {n_listed}[/]")
    console.print(f"[green]Detail rows:  {n_detail}[/]")
    console.print(f"[green]Precedente refs:  {n_prec}[/]")
    console.print(f"[bold]Elapsed:[/] {time.time()-t0:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
