"""
DOF crawler — Diario Oficial de la Federación.

API descubierta:
  https://www.dof.gob.mx/index_111.php?year=Y&month=M&day=D
    → HTML con la lista de notas del día. Contiene codigos como ?codigo=NNNNNNN
  https://www.dof.gob.mx/nota_detalle.php?codigo=N
    → HTML con texto completo de la nota

Estrategia: walk fechas (1990..2025), por cada día listo codigos y bajo
detalles. Filtramos in-DB por tipo ("Decreto Promulgatorio" para tratados,
"Decreto" para reformas legislativas, etc.).

verify=False porque DOF tiene problemas de cadena SSL.

Schema (dof.db):
  dof_pubs(codigo PK, fecha_pub, organismo, tipo, titulo, texto_html, byte_size,
           raw_html, status, fetched_at)
  dof_dias(fecha PK, codigos_count, fetched_at)
"""
from __future__ import annotations

import argparse
import re
import sqlite3
import sys
import time
import datetime as dt
from pathlib import Path

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from rich.console import Console
from rich.progress import Progress, BarColumn, TextColumn, MofNCompleteColumn, TimeRemainingColumn

console = Console()
DB_PATH = Path(__file__).parent / "dof.db"
HEADERS = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) Chrome/131.0"}


def init_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS dof_pubs (
            codigo INTEGER PRIMARY KEY,
            fecha_pub TEXT,
            organismo TEXT,
            tipo TEXT,
            titulo TEXT,
            texto TEXT,
            raw_html TEXT,
            byte_size INTEGER,
            status TEXT,
            fetched_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS dof_dias (
            fecha TEXT PRIMARY KEY,
            codigos_count INTEGER,
            fetched_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_dof_fecha ON dof_pubs(fecha_pub)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_dof_tipo ON dof_pubs(tipo)")
    conn.commit()
    return conn


TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")


def text_only(s: str) -> str:
    s = TAG_RE.sub(" ", s)
    return WS_RE.sub(" ", s).strip()


@retry(reraise=True, stop=stop_after_attempt(5),
       wait=wait_exponential(multiplier=1, min=2, max=30),
       retry=retry_if_exception_type((httpx.HTTPError, httpx.RemoteProtocolError, httpx.ReadTimeout)))
def fetch(client: httpx.Client, url: str) -> httpx.Response:
    return client.get(url, timeout=30, follow_redirects=True)


def parse_index_page(html: str) -> set[int]:
    return set(int(c) for c in re.findall(r"codigo=(\d+)", html))


TITULO_RE = re.compile(r"<title>(?:DOF\s*-\s*)?([^<]+)</title>", re.I)
ORG_RE = re.compile(r"<b>SECRETAR[ÍI]A\s+DE\s+([^<\n]{3,80})</b>", re.I)
TIPO_RE = re.compile(r"^(DECRETO\s+(?:PROMULGATORIO|POR\s+EL\s+QUE)?[^.\n]{0,80}|ACUERDO[^.\n]{0,80}|REGLAMENTO[^.\n]{0,80}|LEY[^.\n]{0,80}|RESOLUCI[ÓO]N[^.\n]{0,80}|AVISO[^.\n]{0,80}|CIRCULAR[^.\n]{0,80})", re.M | re.I)


def parse_nota(html: str) -> dict:
    text = text_only(html)
    out: dict = {"raw_html": html, "texto": text[:200000]}
    m = TITULO_RE.search(html)
    if m: out["titulo"] = m.group(1).strip()
    m = ORG_RE.search(html)
    if m: out["organismo"] = m.group(1).strip()
    m = TIPO_RE.search(text)
    if m: out["tipo"] = m.group(1).strip()[:120]
    return out


def daterange(start: dt.date, end: dt.date):
    cur = start
    while cur <= end:
        # Skip Sundays (DOF publishes mostly Mon-Sat, sometimes Sun)
        yield cur
        cur += dt.timedelta(days=1)


def crawl(start_date: str, end_date: str, detail: bool = True):
    conn = init_db(DB_PATH)
    sd = dt.date.fromisoformat(start_date)
    ed = dt.date.fromisoformat(end_date)
    total_days = (ed - sd).days + 1

    with httpx.Client(http2=True, headers=HEADERS, timeout=45, follow_redirects=True, verify=False) as c:
        with Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TextColumn("•"),
            TimeRemainingColumn(),
            console=console,
        ) as prog:
            task = prog.add_task("dof days", total=total_days)
            for d in daterange(sd, ed):
                date_iso = d.isoformat()
                if conn.execute("SELECT 1 FROM dof_dias WHERE fecha=?", (date_iso,)).fetchone():
                    prog.advance(task); continue
                url = f"https://www.dof.gob.mx/index_111.php?year={d.year}&month={d.month:02d}&day={d.day:02d}"
                try:
                    r = fetch(c, url)
                except Exception:
                    prog.advance(task); continue
                if r.status_code != 200:
                    prog.advance(task); continue
                codigos = parse_index_page(r.text)
                # Filter out the year/day codes in URLs (those are different)
                codigos = {c for c in codigos if 1000000 < c < 10000000}
                conn.execute("INSERT OR REPLACE INTO dof_dias(fecha, codigos_count, fetched_at) VALUES(?,?,datetime('now'))",
                             (date_iso, len(codigos)))
                # Each codigo: fetch detail
                if detail:
                    for codigo in codigos:
                        if conn.execute("SELECT 1 FROM dof_pubs WHERE codigo=?", (codigo,)).fetchone():
                            continue
                        try:
                            dr = fetch(c, f"https://www.dof.gob.mx/nota_detalle.php?codigo={codigo}")
                        except Exception:
                            continue
                        if dr.status_code != 200 or len(dr.text) < 5000:
                            continue
                        parsed = parse_nota(dr.text)
                        conn.execute("""
                            INSERT OR REPLACE INTO dof_pubs(codigo, fecha_pub, organismo, tipo, titulo,
                                                            texto, raw_html, byte_size, status, fetched_at)
                            VALUES(?,?,?,?,?,?,?,?,?,datetime('now'))
                        """, (
                            codigo, date_iso, parsed.get("organismo"), parsed.get("tipo"),
                            parsed.get("titulo"), parsed.get("texto"),
                            dr.text[:300000], len(dr.text), "ok",
                        ))
                        time.sleep(0.1)
                conn.commit()
                prog.advance(task)
                time.sleep(0.1)

    n = conn.execute("SELECT COUNT(*) FROM dof_pubs WHERE status='ok'").fetchone()[0]
    by_tipo = conn.execute("SELECT substr(tipo,1,30), COUNT(*) FROM dof_pubs WHERE tipo IS NOT NULL GROUP BY 1 ORDER BY 2 DESC LIMIT 12").fetchall()
    console.print(f"[green]dof_pubs: {n}[/]")
    for t, k in by_tipo:
        console.print(f"  {t}: {k}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2020-01-01")
    ap.add_argument("--end", default=str(dt.date.today()))
    ap.add_argument("--no-detail", action="store_true")
    args = ap.parse_args()
    crawl(args.start, args.end, detail=not args.no_detail)


if __name__ == "__main__":
    sys.exit(main())
