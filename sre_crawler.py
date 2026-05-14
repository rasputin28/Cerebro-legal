"""
SRE Tratados crawler.

Phase 1 (listing): Playwright on /tratadosmexico/buscador, empty search →
  paginate 1..151, collect (token, nombre, categoria_listing) tuples.
Phase 2 (detail): for each token, GET /tratadosmexico/tratados/{token}, parse
  the <b>-labeled fields with selectolax.

Schema (sre_tratados):
  token_sre PK, nombre, url_sre, categoria, tema, estatus,
  lugar_adopcion, fecha_adopcion,
  aprobacion_senado, publ_dof_aprobacion, fecha_vigor_mexico, publ_dof_promulgacion,
  tramite_constitucional, notas,
  tipo (ddhh/comercial/otro — inferred from categoria),
  reservas TEXT (NULL until UN bridge),
  detail_html TEXT,
  listing_fetched_at, detail_fetched_at

Run:
  .venv/bin/python sre_crawler.py --phase listing
  .venv/bin/python sre_crawler.py --phase detail
  .venv/bin/python sre_crawler.py            # both phases
"""
from __future__ import annotations

import argparse
import asyncio
import json
import re
import sqlite3
import sys
import time
from pathlib import Path

from playwright.async_api import async_playwright
from selectolax.parser import HTMLParser
from rich.console import Console
from rich.progress import Progress, BarColumn, TextColumn, MofNCompleteColumn, TimeRemainingColumn

console = Console()

DB_PATH = Path(__file__).parent / "sre.db"
BUSCADOR_URL = "https://cja.sre.gob.mx/tratadosmexico/buscador"
DETAIL_BASE = "https://cja.sre.gob.mx/tratadosmexico/tratados/"

DDHH_HINTS = re.compile(r"derechos\s+humanos|civiles|sociales|cultural|niño|mujer|indígena|tortura|"
                        r"discriminación|refugiado|asilo|protección|libertad|igualdad",
                        re.I)
COMMERCIAL_HINTS = re.compile(r"comerc|inversi[oó]n|aduanal|fiscal|tributari|libre\s+comercio|tlcan|t-mec|"
                              r"financ|econ[oó]mic|empresa|mercanc[ií]a|propiedad\s+intelectual",
                              re.I)


def init_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sre_tratados (
            token_sre TEXT PRIMARY KEY,
            nombre TEXT,
            url_sre TEXT,
            categoria TEXT,
            tema TEXT,
            estatus TEXT,
            lugar_adopcion TEXT,
            fecha_adopcion TEXT,
            aprobacion_senado TEXT,
            publ_dof_aprobacion TEXT,
            fecha_vigor_mexico TEXT,
            publ_dof_promulgacion TEXT,
            tramite_constitucional TEXT,
            notas TEXT,
            tipo TEXT,
            reservas TEXT,
            detail_html TEXT,
            listing_fetched_at TEXT,
            detail_fetched_at TEXT,
            pagina_listado INTEGER
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_sre_tipo ON sre_tratados(tipo)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_sre_detail_null ON sre_tratados(token_sre) WHERE detail_fetched_at IS NULL")
    conn.commit()
    return conn


def infer_tipo(categoria: str | None, tema: str | None, nombre: str | None) -> str:
    blob = " ".join(x for x in (categoria, tema, nombre) if x).lower()
    if DDHH_HINTS.search(blob):
        return "ddhh"
    if COMMERCIAL_HINTS.search(blob):
        return "comercial"
    return "otro"


# ---------- Phase 1: listing ----------

async def phase_listing(conn: sqlite3.Connection) -> int:
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        ctx = await browser.new_context(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) Chrome/131.0",
            locale="es-MX",
        )
        page = await ctx.new_page()
        await page.goto(BUSCADOR_URL, wait_until="networkidle", timeout=60_000)
        await page.wait_for_timeout(3000)

        # Submit empty search
        btn = page.get_by_role("button", name=re.compile("Buscar", re.I)).first
        if await btn.count():
            await btn.click()
        else:
            submit = page.locator("input[type='submit']").first
            await submit.click()
        await page.wait_for_timeout(5000)

        # Find max page
        html = await page.content()
        max_page = 1
        for m in re.finditer(r"page=(\d+)|p[áa]gina\s*(\d+)|\b(\d{1,3})\b", html):
            for g in m.groups():
                if g:
                    n = int(g)
                    if n > max_page and n < 1000:
                        max_page = n
        # Also count from pagination DOM
        pag_links = await page.locator(".pagination .page-link, .pagination a").all()
        nums = []
        for link in pag_links:
            try:
                t = (await link.inner_text()).strip()
                if t.isdigit():
                    nums.append(int(t))
            except Exception:
                pass
        if nums:
            max_page = max(max_page, max(nums))
        console.print(f"[bold]max page detected:[/] {max_page}")

        total = 0
        with Progress(
            TextColumn("listing"),
            BarColumn(),
            MofNCompleteColumn(),
            TextColumn("•"),
            TimeRemainingColumn(),
            console=console,
        ) as prog:
            task = prog.add_task("pages", total=max_page)

            for pn in range(1, max_page + 1):
                # Each page extract from current DOM
                html = await page.content()
                tokens = re.findall(r'/tratadosmexico/tratados/([\w\-=]+)', html)
                tokens = list(dict.fromkeys(tokens))  # dedupe in order

                # Extract token → nombre by walking result blocks: try common card patterns
                tree = HTMLParser(html)
                # Heuristic: each result has an anchor whose href contains a token, and nombre is the anchor text
                nombres = {}
                for a in tree.css("a"):
                    href = a.attributes.get("href", "")
                    m = re.search(r'/tratadosmexico/tratados/([\w\-=]+)', href)
                    if m:
                        tok = m.group(1)
                        text = a.text(strip=True)
                        if text and tok not in nombres:
                            nombres[tok] = text

                for tok in tokens:
                    nombre = nombres.get(tok)
                    conn.execute(
                        """INSERT INTO sre_tratados(token_sre, nombre, url_sre, listing_fetched_at, pagina_listado)
                           VALUES(?,?,?,datetime('now'),?)
                           ON CONFLICT(token_sre) DO UPDATE SET
                               nombre=COALESCE(excluded.nombre, sre_tratados.nombre),
                               listing_fetched_at=datetime('now'),
                               pagina_listado=excluded.pagina_listado""",
                        (tok, nombre, DETAIL_BASE + tok, pn),
                    )
                    total += 1
                conn.commit()
                prog.advance(task)

                if pn == max_page:
                    break

                # Click next-page link (›) or page number n+1
                next_link = page.locator(f".pagination .page-item:not(.disabled) a:has-text('{pn+1}')").first
                if await next_link.count() == 0:
                    # try »
                    next_link = page.locator(".pagination .page-item:not(.disabled) a[aria-label*='Next']").first
                if await next_link.count() == 0:
                    # last-ditch: click any anchor whose text is the next number
                    next_link = page.get_by_text(str(pn + 1), exact=True).first
                try:
                    await next_link.click(timeout=5000)
                    await page.wait_for_timeout(2500)
                except Exception as e:
                    console.print(f"[red]page {pn+1} click failed: {e} — trying URL approach[/red]")
                    # Some Angular paginators reflect state in url ?page=N — try it
                    await page.goto(f"{BUSCADOR_URL}?page={pn+1}", wait_until="networkidle", timeout=20_000)
                    await page.wait_for_timeout(2000)

        await browser.close()
        return total


# ---------- Phase 2: detail ----------

LABEL_MAP = {
    "lugar y fecha de adopción": "lugar_adopcion",
    "categoría": "categoria",
    "estatus": "estatus",
    "tema": "tema",
    "notas": "notas",
    "trámite constitucional": "tramite_constitucional",
    "aprobación del senado": "aprobacion_senado",
    "publicación dof aprobación": "publ_dof_aprobacion",
    "entrada en vigor": "fecha_vigor_mexico",
    "publicación dof promulgación": "publ_dof_promulgacion",
}


def parse_detail_html(html: str) -> dict:
    """Walk <b> labels and capture trailing text in the parent block."""
    tree = HTMLParser(html)
    out: dict = {}
    # title (h1/h2/h3)
    for sel in ["h1", "h2.title", ".titulo", "h2", "h3"]:
        n = tree.css_first(sel)
        if n:
            t = n.text(strip=True)
            if t and len(t) > 5:
                out["nombre"] = t
                break

    # iterate every <b> label
    for b in tree.css("b"):
        label = b.text(strip=True).rstrip(":").lower()
        key = LABEL_MAP.get(label)
        if not key:
            continue
        # text after the <b>: walk parent and take everything after this node
        parent = b.parent
        if parent is None:
            continue
        # Find this <b>'s position and concat following text
        found_self = False
        captured_text = []
        for child in parent.iter(include_text=True):
            if child is b:
                found_self = True
                continue
            if not found_self:
                continue
            # Stop at next <b> (next field) or break tags that delimit
            if hasattr(child, "tag") and child.tag == "b":
                break
            text = child.text(deep=True, strip=True) if hasattr(child, "text") else str(child)
            if text:
                captured_text.append(text)
        val = " ".join(captured_text).strip(" :;,.\n\t")
        if val:
            out[key] = val
    return out


async def phase_detail(conn: sqlite3.Connection) -> int:
    rows = conn.execute(
        "SELECT token_sre FROM sre_tratados WHERE detail_fetched_at IS NULL"
    ).fetchall()
    tokens = [r[0] for r in rows]
    if not tokens:
        console.print("nothing to do for detail phase")
        return 0
    console.print(f"[bold]Pending details:[/] {len(tokens)}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        ctx = await browser.new_context(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) Chrome/131.0",
            locale="es-MX",
        )
        page = await ctx.new_page()

        with Progress(
            TextColumn("detail"),
            BarColumn(),
            MofNCompleteColumn(),
            TextColumn("•"),
            TimeRemainingColumn(),
            console=console,
        ) as prog:
            task = prog.add_task("detail", total=len(tokens))
            ok = 0
            for tok in tokens:
                try:
                    await page.goto(DETAIL_BASE + tok, wait_until="domcontentloaded", timeout=30_000)
                    await page.wait_for_timeout(800)
                    html = await page.content()
                    parsed = parse_detail_html(html)
                    nombre = parsed.get("nombre")
                    tipo = infer_tipo(parsed.get("categoria"), parsed.get("tema"), nombre)
                    conn.execute(
                        """UPDATE sre_tratados SET
                              nombre=COALESCE(?, nombre),
                              categoria=?, tema=?, estatus=?, lugar_adopcion=?,
                              fecha_adopcion=?, aprobacion_senado=?, publ_dof_aprobacion=?,
                              fecha_vigor_mexico=?, publ_dof_promulgacion=?,
                              tramite_constitucional=?, notas=?, tipo=?, detail_html=?,
                              detail_fetched_at=datetime('now')
                           WHERE token_sre=?""",
                        (
                            nombre,
                            parsed.get("categoria"), parsed.get("tema"), parsed.get("estatus"),
                            parsed.get("lugar_adopcion"),
                            parsed.get("lugar_adopcion"),  # fecha_adopcion = lugar y fecha; parser leaves combined; refined later
                            parsed.get("aprobacion_senado"), parsed.get("publ_dof_aprobacion"),
                            parsed.get("fecha_vigor_mexico"), parsed.get("publ_dof_promulgacion"),
                            parsed.get("tramite_constitucional"), parsed.get("notas"),
                            tipo, html, tok,
                        ),
                    )
                    conn.commit()
                    ok += 1
                except Exception as e:
                    console.print(f"[red]detail {tok[:20]}... failed: {e}[/red]")
                prog.advance(task)

        await browser.close()
        return ok


# ---------- main ----------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--phase", choices=["listing", "detail", "all"], default="all")
    args = ap.parse_args()

    conn = init_db(DB_PATH)
    console.print(f"[bold]DB:[/] {DB_PATH}")

    t0 = time.time()
    loop = asyncio.new_event_loop()
    if args.phase in ("listing", "all"):
        n = loop.run_until_complete(phase_listing(conn))
        console.print(f"[green]listing wrote rows: {n}[/]")
    if args.phase in ("detail", "all"):
        n = loop.run_until_complete(phase_detail(conn))
        console.print(f"[green]detail rows updated: {n}[/]")

    listed = conn.execute("SELECT COUNT(*) FROM sre_tratados").fetchone()[0]
    detailed = conn.execute("SELECT COUNT(*) FROM sre_tratados WHERE detail_fetched_at IS NOT NULL").fetchone()[0]
    by_tipo = conn.execute("SELECT tipo, COUNT(*) FROM sre_tratados WHERE tipo IS NOT NULL GROUP BY tipo").fetchall()
    console.print(f"[bold]listed:[/] {listed}  [bold]detailed:[/] {detailed}")
    for tipo, n in by_tipo:
        console.print(f"  {tipo}: {n}")
    console.print(f"[bold]Elapsed:[/] {time.time()-t0:.1f}s")


if __name__ == "__main__":
    sys.exit(main())
