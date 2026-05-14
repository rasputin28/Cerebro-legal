"""
CNDH (Comisión Nacional de los Derechos Humanos) recomendaciones crawler.

The CNDH publishes Recomendaciones at:
  https://www.cndh.org.mx/documento/recomendaciones?recomendado=&anio=YYYY
  https://www.cndh.org.mx/sites/default/files/documentos/YYYY-NN/RecYYYY_NNN.pdf

Strategy: walk years 1990..present, fetch the year's index page, extract all
Recomendacion links, download each PDF.
"""
from __future__ import annotations

import argparse
import re
import sqlite3
import sys
import time
import datetime as dt
import io
from pathlib import Path

import httpx
import pdfplumber
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from rich.console import Console
from rich.progress import Progress, BarColumn, TextColumn, MofNCompleteColumn, TimeRemainingColumn

console = Console()
DB_PATH = Path(__file__).parent / "cndh.db"
HEADERS = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) Chrome/131.0"}


def init_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cndh_recs (
            num_rec TEXT PRIMARY KEY,           -- e.g. "Recomendación 23/2024"
            anio INTEGER,
            numero INTEGER,
            destinatario TEXT,                  -- e.g. "Secretaría de Marina"
            tema TEXT,
            fecha TEXT,
            url_pdf TEXT,
            pdf_bytes BLOB,
            raw_text TEXT,
            byte_size INTEGER,
            status TEXT,
            fetched_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cndh_anio ON cndh_recs(anio)")
    conn.commit()
    return conn


@retry(reraise=True, stop=stop_after_attempt(4),
       wait=wait_exponential(multiplier=1, min=2, max=30),
       retry=retry_if_exception_type((httpx.HTTPError, httpx.RemoteProtocolError, httpx.ReadTimeout)))
def fetch(client: httpx.Client, url: str) -> httpx.Response:
    return client.get(url, timeout=60, follow_redirects=True)


def parse_index(html: str) -> list[dict]:
    """Pull (number, year, url, destinatario, tema) tuples from the year listing."""
    out = []
    # CNDH uses card-like structures or table rows; permissive regex over anchors
    for m in re.finditer(r'<a[^>]+href="([^"]+\.pdf)"[^>]*>([^<]{2,500})</a>', html, re.I):
        href, text = m.group(1), m.group(2).strip()
        text_clean = re.sub(r"\s+", " ", text)
        # Try to extract Rec NN/YYYY
        n = re.search(r"Rec(?:omendaci[oó]n)?[\s/\-#]*(\d{1,4})\s*/\s*(\d{4})", text_clean, re.I) or \
            re.search(r"(\d{1,4})\s*/\s*(\d{4})", text_clean) or \
            re.search(r"Rec(\d{1,4})_?(\d{4})", href, re.I) or \
            re.search(r"/(\d{4})-(\d{1,4})/", href) or \
            re.search(r"Rec(\d+)_(\d+)", href, re.I)
        if not n:
            continue
        num, year = n.group(1), n.group(2)
        if int(year) < 1990 or int(year) > 2026:
            # might have num/year flipped
            try:
                if 1990 <= int(num) <= 2026:
                    num, year = year, num
            except Exception:
                pass
        out.append({
            "numero": int(num),
            "anio": int(year),
            "url": href if href.startswith("http") else f"https://www.cndh.org.mx{href}",
            "title": text_clean[:300],
        })
    return out


TITULO_RE = re.compile(r"Recomendaci[oó]n\s+(?:No\.|N[uú]mero)?\s*(\d+/\d{4}|\d+[\sde]+\d{4})", re.I)
DEST_RE = re.compile(r"(?:DESTINATARIA|DESTINATARIO|destinada\s+a|dirigida\s+a)[:\s]+([^\n\r\.;]+)", re.I)
FECHA_RE = re.compile(r"M[eé]xico,\s+(?:Ciudad\s+de\s+M[eé]xico,\s+)?(?:a\s+)?(\d{1,2}\s+de\s+\w+\s+de\s+\d{4})", re.I)


def parse_rec_pdf(pdf_bytes: bytes) -> dict:
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            text = "\n".join((p.extract_text() or "") for p in pdf.pages[:5])
    except Exception as e:
        return {"_err": str(e)}
    out = {"raw_text": text}
    m = TITULO_RE.search(text)
    if m: out["num_rec"] = "Recomendación " + m.group(1)
    m = DEST_RE.search(text)
    if m: out["destinatario"] = m.group(1).strip()[:200]
    m = FECHA_RE.search(text)
    if m: out["fecha"] = m.group(1)
    # tema: try to find a "Tema:" line
    m = re.search(r"^\s*(?:Tema|Caso)[:\s]+(.{20,300})", text, re.M | re.I)
    if m: out["tema"] = m.group(1).strip()
    return out


INDEX_URLS = [
    "https://www.cndh.org.mx/documento/recomendaciones?anio={year}",
    "https://www.cndh.org.mx/tipo/1/recomendacion?anio={year}",
]


def crawl(start_year: int, end_year: int):
    conn = init_db(DB_PATH)
    with httpx.Client(http2=True, headers=HEADERS, timeout=45, follow_redirects=True, verify=False) as c:
        all_recs = []
        for year in range(start_year, end_year + 1):
            found = []
            for tpl in INDEX_URLS:
                url = tpl.format(year=year)
                try:
                    r = fetch(c, url)
                except Exception:
                    continue
                if r.status_code == 200:
                    found.extend(parse_index(r.text))
            # dedupe by url
            seen = set()
            uniq = []
            for it in found:
                if it["url"] not in seen:
                    seen.add(it["url"])
                    uniq.append(it)
            console.print(f"  año {year}: {len(uniq)} recs detected")
            all_recs.extend(uniq)

        console.print(f"\ntotal recs to download: {len(all_recs)}")
        with Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TextColumn("•"),
            TimeRemainingColumn(),
            console=console,
        ) as prog:
            task = prog.add_task("recs", total=len(all_recs))
            ok = 0
            for it in all_recs:
                num_rec = f"Recomendación {it['numero']}/{it['anio']}"
                if conn.execute("SELECT 1 FROM cndh_recs WHERE num_rec=?", (num_rec,)).fetchone():
                    prog.advance(task); continue
                try:
                    r = fetch(c, it["url"])
                except Exception:
                    prog.advance(task); continue
                if r.status_code != 200 or r.content[:5] != b"%PDF-":
                    conn.execute("INSERT OR IGNORE INTO cndh_recs(num_rec, anio, numero, url_pdf, status) VALUES(?,?,?,?,?)",
                                 (num_rec, it["anio"], it["numero"], it["url"], f"http_{r.status_code}"))
                    conn.commit()
                    prog.advance(task)
                    continue
                parsed = parse_rec_pdf(r.content)
                conn.execute("""
                    INSERT OR REPLACE INTO cndh_recs(num_rec, anio, numero, destinatario, tema, fecha,
                                                     url_pdf, pdf_bytes, raw_text, byte_size, status, fetched_at)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,datetime('now'))
                """, (num_rec, it["anio"], it["numero"],
                      parsed.get("destinatario"), parsed.get("tema"), parsed.get("fecha"),
                      it["url"], r.content, parsed.get("raw_text"), len(r.content), "ok"))
                conn.commit()
                ok += 1
                prog.advance(task)
                time.sleep(0.3)

    n = conn.execute("SELECT COUNT(*) FROM cndh_recs WHERE status='ok'").fetchone()[0]
    console.print(f"[green]cndh_recs ok: {n}[/]")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", type=int, default=2010)
    ap.add_argument("--end", type=int, default=dt.date.today().year)
    args = ap.parse_args()
    crawl(args.start, args.end)


if __name__ == "__main__":
    sys.exit(main())
