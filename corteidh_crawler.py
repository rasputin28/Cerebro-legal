"""
Corte IDH crawler — versión PDF-direct.

URL patterns descubiertos:
  Serie C (contenciosos): https://www.corteidh.or.cr/docs/casos/articulos/seriec_<N>_esp.pdf
  Serie A (opiniones consultivas): https://www.corteidh.or.cr/docs/opiniones/seriea_<N>_esp.pdf
  Serie E (medidas provisionales): https://www.corteidh.or.cr/docs/medidas/.../seriee_<N>_esp.pdf

Estrategia: iterar N=1..500 para serieC y N=1..50 para serieA. Skip 404s.
Para cada PDF: bajar, parsear texto con pdfplumber, extraer metadata clave
(nombre del caso, país, fecha de la sentencia, párrafos).

Schema (corteidh.db):
  idh_docs(doc_id PK, serie 'C'/'A'/'E', numero, url, pdf_bytes, byte_size,
           raw_text, nombre, pais, fecha_sentencia, anio, status, fetched_at)
"""
from __future__ import annotations

import argparse
import io
import re
import sqlite3
import sys
import time
from pathlib import Path

import httpx
import pdfplumber
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from rich.console import Console
from rich.progress import Progress, BarColumn, TextColumn, MofNCompleteColumn, TimeRemainingColumn

console = Console()
DB_PATH = Path(__file__).parent / "corteidh.db"
HEADERS = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) Chrome/131.0"}

URL_TEMPLATES = {
    "C": "https://www.corteidh.or.cr/docs/casos/articulos/seriec_{}_esp.pdf",
    "A": "https://www.corteidh.or.cr/docs/opiniones/seriea_{}_esp.pdf",
    "E": "https://www.corteidh.or.cr/docs/medidas/seriee_{}_esp.pdf",
}


def init_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS idh_docs (
            serie TEXT NOT NULL,
            numero INTEGER NOT NULL,
            url TEXT NOT NULL,
            pdf_bytes BLOB,
            byte_size INTEGER,
            raw_text TEXT,
            nombre TEXT,
            pais TEXT,
            fecha_sentencia TEXT,
            anio INTEGER,
            tipo TEXT,
            status TEXT,
            fetched_at TEXT DEFAULT (datetime('now')),
            PRIMARY KEY(serie, numero)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_idh_serie ON idh_docs(serie)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_idh_pais ON idh_docs(pais)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_idh_anio ON idh_docs(anio)")
    conn.commit()
    return conn


# Metadata extraction from PDF text (Corte IDH sentences follow consistent format)
TITLE_RE = re.compile(r"^\s*(Caso\s+[\w\sÁÉÍÓÚñÑ\.,\-]+?Vs\.\s+[\w\sÁÉÍÓÚñÑ\.,\-]+)$", re.M | re.I)
OPINION_RE = re.compile(r"^\s*(Opinión\s+Consultiva\s+OC[-\s\d/]+[\w\s\dÁÉÍÓÚñÑ]+)$", re.M | re.I)
PAIS_RE = re.compile(r"\bVs\.\s+([\w\sÁÉÍÓÚñÑ]+?)\b", re.I)
FECHA_RE = re.compile(r"Sentencia\s+de\s+(\d{1,2}\s+de\s+\w+\s+de\s+\d{4})", re.I)
ANIO_RE = re.compile(r"\b(19|20)\d{2}\b")


def parse_pdf(pdf_bytes: bytes) -> dict:
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            pages = pdf.pages[:3]  # first 3 pages usually contain title + metadata
            head_text = "\n".join((p.extract_text() or "") for p in pages)
            full_text_pages = []
            for p in pdf.pages:
                full_text_pages.append(p.extract_text() or "")
            full = "\n".join(full_text_pages)
    except Exception as e:
        return {"_err": str(e)}

    out: dict = {"raw_text": full}
    # Title
    m = TITLE_RE.search(head_text) or OPINION_RE.search(head_text)
    if m:
        out["nombre"] = re.sub(r"\s+", " ", m.group(1)).strip()
    if "Opinión Consultiva" in head_text or "OPINIÓN CONSULTIVA" in head_text:
        out["tipo"] = "opinion_consultiva"
    elif "Caso" in head_text:
        out["tipo"] = "sentencia_contenciosa"
    # País
    m = PAIS_RE.search(head_text)
    if m:
        out["pais"] = m.group(1).strip().rstrip(".,;:").rstrip()
    # Fecha
    m = FECHA_RE.search(head_text)
    if m:
        out["fecha_sentencia"] = m.group(1)
        my = ANIO_RE.search(m.group(1))
        if my:
            out["anio"] = int(my.group(0))
    return out


@retry(reraise=True, stop=stop_after_attempt(4),
       wait=wait_exponential(multiplier=1, min=2, max=30),
       retry=retry_if_exception_type((httpx.HTTPError, httpx.RemoteProtocolError, httpx.ReadTimeout)))
def fetch(client: httpx.Client, url: str) -> httpx.Response:
    return client.get(url, timeout=120, follow_redirects=True)


def crawl_serie(client: httpx.Client, conn: sqlite3.Connection,
                serie: str, max_n: int, prog: Progress) -> int:
    task = prog.add_task(f"Serie {serie}", total=max_n)
    ok = 0
    consecutive_404 = 0
    for n in range(1, max_n + 1):
        if conn.execute("SELECT 1 FROM idh_docs WHERE serie=? AND numero=?", (serie, n)).fetchone():
            prog.advance(task); continue
        url = URL_TEMPLATES[serie].format(n)
        try:
            r = fetch(client, url)
        except Exception as e:
            conn.execute("INSERT OR IGNORE INTO idh_docs(serie, numero, url, status) VALUES(?,?,?,?)",
                         (serie, n, url, f"err: {e}"))
            conn.commit()
            prog.advance(task)
            continue
        if r.status_code == 200 and r.content[:5] == b"%PDF-":
            consecutive_404 = 0
            parsed = parse_pdf(r.content)
            conn.execute("""
                INSERT OR REPLACE INTO idh_docs(serie, numero, url, pdf_bytes, byte_size,
                                                 raw_text, nombre, pais, fecha_sentencia, anio,
                                                 tipo, status, fetched_at)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,datetime('now'))
            """, (serie, n, url, r.content, len(r.content),
                  parsed.get("raw_text"), parsed.get("nombre"), parsed.get("pais"),
                  parsed.get("fecha_sentencia"), parsed.get("anio"),
                  parsed.get("tipo"), "ok"))
            conn.commit()
            ok += 1
        else:
            consecutive_404 += 1
            conn.execute("INSERT OR IGNORE INTO idh_docs(serie, numero, url, status) VALUES(?,?,?,?)",
                         (serie, n, url, f"http_{r.status_code}"))
            conn.commit()
            # If we see lots of consecutive 404s, the series has likely ended.
            if consecutive_404 >= 8 and n > 50:
                console.print(f"[yellow]Serie {serie}: 8 consecutive 404s at n={n}; stopping.[/]")
                prog.update(task, total=n)
                break
        prog.advance(task)
        time.sleep(0.2)
    return ok


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-c", type=int, default=500, help="Serie C upper bound")
    ap.add_argument("--max-a", type=int, default=50)
    ap.add_argument("--max-e", type=int, default=30)
    args = ap.parse_args()

    conn = init_db(DB_PATH)
    console.print(f"[bold]DB:[/] {DB_PATH}")

    with httpx.Client(http2=True, headers=HEADERS, timeout=120, follow_redirects=True) as c:
        with Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TextColumn("•"),
            TimeRemainingColumn(),
            console=console,
        ) as prog:
            crawl_serie(c, conn, "C", args.max_c, prog)
            crawl_serie(c, conn, "A", args.max_a, prog)
            crawl_serie(c, conn, "E", args.max_e, prog)

    n_ok = conn.execute("SELECT COUNT(*) FROM idh_docs WHERE status='ok'").fetchone()[0]
    by_s = conn.execute("SELECT serie, COUNT(*) FROM idh_docs WHERE status='ok' GROUP BY serie").fetchall()
    console.print(f"[green]idh_docs ok: {n_ok}[/]")
    for s, k in by_s:
        console.print(f"  Serie {s}: {k}")


if __name__ == "__main__":
    sys.exit(main())
