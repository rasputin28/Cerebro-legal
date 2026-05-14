"""
TEPJF tesis crawler — IUSE (Sistema Histórico de Tesis).

The IUSE portal has the canonical electoral tesis. Endpoint pattern observed:
  https://www.te.gob.mx/IUSEapp/tesisjur.aspx?idtesis={ID}&tpoBusqueda=A&sWord=

IDs typically run sequentially 1..2000+. Each detail page renders a table with
the tesis data (rubro, texto, partes, fecha).
"""
from __future__ import annotations

import argparse
import re
import sqlite3
import sys
import time
from pathlib import Path

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from rich.console import Console
from rich.progress import Progress, BarColumn, TextColumn, MofNCompleteColumn, TimeRemainingColumn

console = Console()
DB_PATH = Path(__file__).parent / "tepjf.db"
HEADERS = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) Chrome/131.0"}

# Multiple URL patterns to try; first hit wins.
URL_TEMPLATES = [
    "https://www.te.gob.mx/IUSEapp/tesisjur.aspx?idtesis={}&tpoBusqueda=A&sWord=",
    "https://portal.te.gob.mx/colecciones/tesis-jurisprudencia/{}",
]


def init_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tepjf_tesis (
            id_tepjf INTEGER PRIMARY KEY,
            tipo TEXT,       -- 'jurisprudencia','tesis_aislada'
            clave TEXT,      -- e.g. '6/2008'
            rubro TEXT,
            texto TEXT,
            partes TEXT,
            instancia TEXT,  -- 'Sala Superior', 'Sala Regional ...'
            fecha TEXT,
            anio INTEGER,
            url TEXT,
            raw_html TEXT,
            status TEXT,
            fetched_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tepjf_anio ON tepjf_tesis(anio)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tepjf_tipo ON tepjf_tesis(tipo)")
    conn.commit()
    return conn


TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")


def text_only(s: str) -> str:
    s = TAG_RE.sub(" ", s).replace("&nbsp;", " ")
    return WS_RE.sub(" ", s).strip()


# Field regexes for the IUSE tesis page
RUBRO_RE = re.compile(r"(?:rubro|sumario)[\s:]+([A-ZÁÉÍÓÚÑ][^<\n]{15,300})", re.I)
CLAVE_RE = re.compile(r"\b((?:Jurisprudencia|Tesis)\s+(?:[XLI]+/?)?\d{1,4}\s*/\s*\d{4})", re.I)
INSTANCIA_RE = re.compile(r"\b(Sala\s+Superior|Sala\s+Regional[^\n<]*)\b", re.I)
TIPO_RE = re.compile(r"\b(Jurisprudencia|Tesis\s+Aislada|Tesis\s+(?:Relevante|Vinculante))\b", re.I)
PARTES_RE = re.compile(r"(?:Actor|Promovente|Demandante)[\s:]+([^\n<]{3,200})", re.I)
FECHA_RE = re.compile(r"(\d{1,2}\s+de\s+\w+\s+de\s+(\d{4}))", re.I)


def parse_iuse_html(html: str) -> dict:
    body_text = text_only(html)
    out: dict = {}
    m = RUBRO_RE.search(body_text)
    if m: out["rubro"] = m.group(1).strip()
    m = CLAVE_RE.search(body_text)
    if m: out["clave"] = re.sub(r"\s+", " ", m.group(1).strip())
    m = INSTANCIA_RE.search(body_text)
    if m: out["instancia"] = m.group(1).strip()
    m = TIPO_RE.search(body_text)
    if m:
        t = m.group(1).lower()
        out["tipo"] = "jurisprudencia" if "jurisprudencia" in t else "tesis_aislada"
    m = PARTES_RE.search(body_text)
    if m: out["partes"] = m.group(1).strip()
    m = FECHA_RE.search(body_text)
    if m:
        out["fecha"] = m.group(1)
        out["anio"] = int(m.group(2))
    # Texto: take the longest paragraph
    paragraphs = re.findall(r"<p[^>]*>(.*?)</p>", html, re.S | re.I)
    paragraphs = [text_only(p) for p in paragraphs]
    paragraphs = [p for p in paragraphs if 80 < len(p) < 4000]
    if paragraphs:
        out["texto"] = max(paragraphs, key=len)
    return out


@retry(reraise=True, stop=stop_after_attempt(4),
       wait=wait_exponential(multiplier=1, min=2, max=30),
       retry=retry_if_exception_type((httpx.HTTPError, httpx.RemoteProtocolError, httpx.ReadTimeout)))
def fetch(client: httpx.Client, url: str) -> httpx.Response:
    return client.get(url, timeout=45, follow_redirects=True)


def crawl(start: int, end: int) -> None:
    conn = init_db(DB_PATH)
    with httpx.Client(http2=True, headers=HEADERS, timeout=60, follow_redirects=True) as c:
        with Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TextColumn("•"),
            TimeRemainingColumn(),
            console=console,
        ) as prog:
            task = prog.add_task("tepjf", total=end - start + 1)
            consecutive_empty = 0
            for tid in range(start, end + 1):
                if conn.execute("SELECT 1 FROM tepjf_tesis WHERE id_tepjf=?", (tid,)).fetchone():
                    prog.advance(task); continue
                got = False
                for tpl in URL_TEMPLATES:
                    url = tpl.format(tid)
                    try:
                        r = fetch(c, url)
                    except Exception as e:
                        continue
                    if r.status_code == 200 and len(r.text) > 2000:
                        html = r.text
                        # Detect "not found" placeholder
                        if re.search(r"no se encontr|0\s+resultados|sin resultados", html, re.I):
                            continue
                        parsed = parse_iuse_html(html)
                        if not parsed.get("rubro") and not parsed.get("texto"):
                            continue
                        conn.execute("""
                            INSERT OR REPLACE INTO tepjf_tesis(
                                id_tepjf, tipo, clave, rubro, texto, partes, instancia,
                                fecha, anio, url, raw_html, status, fetched_at)
                            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,datetime('now'))
                        """, (
                            tid, parsed.get("tipo"), parsed.get("clave"), parsed.get("rubro"),
                            parsed.get("texto"), parsed.get("partes"), parsed.get("instancia"),
                            parsed.get("fecha"), parsed.get("anio"), url, html[:200000], "ok"
                        ))
                        conn.commit()
                        consecutive_empty = 0
                        got = True
                        break
                if not got:
                    consecutive_empty += 1
                    conn.execute(
                        "INSERT OR IGNORE INTO tepjf_tesis(id_tepjf, status) VALUES(?,?)",
                        (tid, "not_found")
                    )
                    conn.commit()
                    if consecutive_empty >= 30 and tid > 200:
                        console.print(f"[yellow]30 consecutive misses at id={tid}; stopping.[/]")
                        prog.update(task, total=tid - start + 1)
                        break
                prog.advance(task)
                time.sleep(0.2)

    n = conn.execute("SELECT COUNT(*) FROM tepjf_tesis WHERE status='ok'").fetchone()[0]
    console.print(f"[green]tepjf_tesis ok: {n}[/]")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", type=int, default=1)
    ap.add_argument("--end", type=int, default=5000)
    args = ap.parse_args()
    crawl(args.start, args.end)


if __name__ == "__main__":
    sys.exit(main())
