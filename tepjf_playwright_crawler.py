"""
TEPJF crawler v2 — Playwright-driven ASP.NET WebForms (IUSE).

The IUSE search page (https://www.te.gob.mx/IUSEapp/) is an ASP.NET WebForms
application. Search uses __doPostBack with ViewState. Strategy:
  1. Load /IUSEapp/
  2. Click 'Búsqueda avanzada' → fires __doPostBack('lnkBusquedaAvanzada','')
  3. On the advanced search page, leave all fields empty and submit → returns full
     paginated list of tesis y jurisprudencia
  4. For each result row, click the detail link → capture HTML
  5. Walk pagination until all pages done

Schema (tepjf.db):
  tepjf_tesis(id_iuse PK, tipo, clave, rubro, texto, partes, instancia,
              fecha, anio, url, raw_html, status)
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

from playwright.async_api import async_playwright, Page
from rich.console import Console
from rich.progress import Progress, BarColumn, TextColumn, MofNCompleteColumn

console = Console()
DB_PATH = Path(__file__).parent / "tepjf.db"


def init_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tepjf_tesis (
            id_iuse TEXT PRIMARY KEY,
            tipo TEXT,
            clave TEXT,
            rubro TEXT,
            texto TEXT,
            partes TEXT,
            instancia TEXT,
            fecha TEXT,
            anio INTEGER,
            url TEXT,
            raw_html TEXT,
            status TEXT,
            fetched_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tepjf_clave ON tepjf_tesis(clave)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tepjf_anio ON tepjf_tesis(anio)")
    conn.commit()
    return conn


TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")


def text_only(s: str) -> str:
    s = TAG_RE.sub(" ", s)
    return WS_RE.sub(" ", s).strip()


# IUSE detail parsing
RUBRO_RE = re.compile(r"<(?:p|td|div)[^>]*>([A-ZÁÉÍÓÚÑ][^<]{30,400})</", re.S)
CLAVE_RE = re.compile(r"(Jurisprudencia\s+\d+/\d{4}|Tesis\s+(?:[XLIV]+/?)?\d+/\d{4})", re.I)
INSTANCIA_RE = re.compile(r"\b(Sala\s+Superior|Sala\s+Regional[^<\n]*)", re.I)
TIPO_RE = re.compile(r"\b(Jurisprudencia|Tesis\s+Aislada|Tesis\s+(?:Relevante|Vinculante))\b", re.I)


async def crawl_iuse() -> int:
    conn = init_db(DB_PATH)
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        ctx = await browser.new_context(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) Chrome/131.0",
            locale="es-MX",
        )
        page = await ctx.new_page()
        console.print("loading IUSE home...")
        await page.goto("https://www.te.gob.mx/IUSEapp/", wait_until="networkidle", timeout=60_000)
        await page.wait_for_timeout(3000)

        # Click "Búsqueda avanzada" — it uses __doPostBack
        console.print("clicking Búsqueda avanzada...")
        await page.evaluate("__doPostBack('lnkBusquedaAvanzada','')")
        await page.wait_for_timeout(5000)

        # Submit empty form to get all results — find the search button
        # ASP.NET form has a submit button; press it. We try several common patterns.
        clicked = False
        for sel in [
            "input[type='submit'][value*='Buscar' i]",
            "input[type='submit'][value*='Consultar' i]",
            "input[type='button'][value*='Buscar' i]",
            "#btnBuscar",
        ]:
            try:
                btn = page.locator(sel).first
                if await btn.count() and await btn.is_visible():
                    console.print(f"  submit via {sel}")
                    await btn.click(timeout=5000)
                    clicked = True
                    break
            except Exception:
                continue
        if not clicked:
            console.print("[yellow]no submit button found; trying postback fallback[/]")
            await page.evaluate("__doPostBack('btnBuscar','')")
        await page.wait_for_timeout(8000)

        # Now we should be on results page. Walk pagination.
        n_done = 0
        page_no = 1
        while True:
            html = await page.content()
            # Find detail links — pattern usually 'TesisDetalle.aspx?...' or postback link
            details = re.findall(r'href="([^"]*TesisDetalle[^"]*)"|onclick="[^"]*__doPostBack\(&#39;([^&]+)&#39;', html, re.I)
            results_on_page = list(set(d[0] or d[1] for d in details if (d[0] or d[1])))
            console.print(f"page {page_no}: {len(results_on_page)} result links")
            if not results_on_page:
                console.print("[yellow]No results on page — IUSE may need different navigation.[/]")
                # Save the current HTML for inspection
                Path("iuse_results_dump.html").write_text(html[:80000])
                break
            # For each result, click and capture detail HTML
            for r_id in results_on_page[:10]:  # cap per page for testing
                try:
                    if r_id.startswith("http") or r_id.startswith("/"):
                        await page.goto(r_id if r_id.startswith("http") else f"https://www.te.gob.mx{r_id}", timeout=20_000)
                    else:
                        await page.evaluate(f"__doPostBack({r_id!r},'')")
                    await page.wait_for_timeout(2000)
                    detail_html = await page.content()
                    parsed = parse_iuse_detail(detail_html)
                    if parsed.get("rubro"):
                        rid = parsed.get("clave") or r_id[:40]
                        conn.execute("""
                            INSERT OR REPLACE INTO tepjf_tesis(id_iuse, tipo, clave, rubro, texto,
                                                                instancia, raw_html, status, fetched_at)
                            VALUES(?,?,?,?,?,?,?,?,datetime('now'))
                        """, (rid, parsed.get("tipo"), parsed.get("clave"), parsed.get("rubro"),
                              parsed.get("texto"), parsed.get("instancia"), detail_html[:200000], "ok"))
                        conn.commit()
                        n_done += 1
                    # Back to results page
                    await page.go_back()
                    await page.wait_for_timeout(1500)
                except Exception as e:
                    console.print(f"  detail err: {e}")
            # Next page
            page_no += 1
            if page_no > 200:
                break
            try:
                next_btn = page.locator("a[href*='Page']").first
                if await next_btn.count():
                    await next_btn.click(timeout=5000)
                    await page.wait_for_timeout(3000)
                else:
                    break
            except Exception:
                break
        await browser.close()
    return n_done


def parse_iuse_detail(html: str) -> dict:
    out: dict = {}
    text = text_only(html)
    m = CLAVE_RE.search(text)
    if m: out["clave"] = m.group(1)
    m = INSTANCIA_RE.search(text)
    if m: out["instancia"] = m.group(1)
    m = TIPO_RE.search(text)
    if m:
        t = m.group(1).lower()
        out["tipo"] = "jurisprudencia" if "jurisprudencia" in t else "tesis_aislada"
    # Rubro = the first long all-caps Spanish title
    for m in re.finditer(r"([A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑ\.\,\s\-\(\)\:\d]{30,200}\.?)", text):
        candidate = m.group(1).strip()
        if 30 < len(candidate) < 220 and sum(1 for c in candidate if c.isupper()) > 15:
            out["rubro"] = candidate
            break
    # texto = longest paragraph
    paragraphs = [text_only(p) for p in re.findall(r"<p[^>]*>(.*?)</p>", html, re.S | re.I)]
    paragraphs = [p for p in paragraphs if 100 < len(p) < 5000]
    if paragraphs:
        out["texto"] = max(paragraphs, key=len)
    return out


def main():
    n = asyncio.run(crawl_iuse())
    console.print(f"[green]tepjf_tesis ok: {n}[/]")


if __name__ == "__main__":
    sys.exit(main())
