"""
Legislación estatal — Quintana Roo y Yucatán.

URLs descubiertas:
  QRoo:    https://www.congresoqroo.gob.mx/leyes/
  Yucatán: https://congresoyucatan.gob.mx/legislacion/leyes

Ambas páginas tienen el catálogo HTML con enlaces a PDFs de cada ley.
Iteramos: parsear los anchors, descargar cada PDF, extraer texto con pdfplumber.

Schema (legis_estatales.db):
  legis(law_id PK AUTOINCREMENT, estado, nombre, url_pdf, ext, bytes BLOB,
        raw_text, byte_size, status, fetched_at)
"""
from __future__ import annotations
import io, re, sqlite3, sys, time
from pathlib import Path
import httpx
import pdfplumber
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from rich.console import Console
from rich.progress import Progress, BarColumn, TextColumn, MofNCompleteColumn, TimeRemainingColumn

console = Console()
DB_PATH = Path(__file__).parent / "legis_estatales.db"
H = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) Chrome/131.0"}

STATE_PAGES = {
    "QROO": "https://www.congresoqroo.gob.mx/leyes/",
    "YUC":  "https://congresoyucatan.gob.mx/legislacion/leyes",
    "CDMX": "https://www.congresocdmx.gob.mx/leyes/",
    "DGO":  "https://www.congresodurango.gob.mx/legislacion/",  # has 190 PDFs (more than /leyes/)
    "GTO":  "https://www.congresogto.gob.mx/leyes/",
    "TAB":  "https://www.congresotabasco.gob.mx/leyes/",
    "VER":  "https://www.legisver.gob.mx/leyes/",
}


def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS legis (
            law_id INTEGER PRIMARY KEY AUTOINCREMENT,
            estado TEXT NOT NULL,
            nombre TEXT,
            url_pdf TEXT UNIQUE,
            ext TEXT,
            bytes BLOB,
            raw_text TEXT,
            byte_size INTEGER,
            status TEXT,
            fetched_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_legis_estado ON legis(estado)")
    conn.commit()
    return conn


LINK_RE = re.compile(r'<a[^>]+href=["\']([^"\']+\.(?:pdf|docx?))["\'][^>]*>(.{0,400}?)</a>', re.S | re.I)
TAG_RE = re.compile(r"<[^>]+>")


def clean(s):
    return re.sub(r"\s+", " ", TAG_RE.sub(" ", s)).strip()


def absolutize(href, base):
    if href.startswith("http"): return href
    if href.startswith("//"):   return "https:" + href
    if href.startswith("/"):
        from urllib.parse import urlparse
        u = urlparse(base)
        return f"{u.scheme}://{u.netloc}{href}"
    base_dir = base.rsplit("/", 1)[0]
    while href.startswith("../"):
        href = href[3:]
        base_dir = base_dir.rsplit("/", 1)[0]
    return f"{base_dir}/{href}"


@retry(reraise=True, stop=stop_after_attempt(5),
       wait=wait_exponential(multiplier=1, min=2, max=30),
       retry=retry_if_exception_type((httpx.HTTPError, httpx.RemoteProtocolError, httpx.ReadTimeout)))
def fetch(client, url):
    return client.get(url, timeout=60, follow_redirects=True)


def parse_pdf_text(b: bytes) -> str:
    try:
        with pdfplumber.open(io.BytesIO(b)) as pdf:
            return "\n".join((p.extract_text() or "") for p in pdf.pages[:30])
    except Exception:
        return ""


def crawl():
    conn = init_db()
    seen_urls = {r[0] for r in conn.execute("SELECT url_pdf FROM legis")}
    with httpx.Client(http2=True, headers=H, timeout=60, follow_redirects=True, verify=False) as c:
        all_links = []
        for state, page_url in STATE_PAGES.items():
            console.print(f"\n[bold]{state}:[/] {page_url}")
            r = fetch(c, page_url)
            if r.status_code != 200:
                console.print(f"  index {r.status_code}; skip")
                continue
            for href, text in LINK_RE.findall(r.text):
                url = absolutize(href, page_url)
                if url in seen_urls: continue
                all_links.append({
                    "estado": state,
                    "nombre": clean(text)[:200],
                    "url": url,
                    "ext": url.rsplit(".", 1)[-1].lower().split("#")[0].split("?")[0],
                })
            console.print(f"  found {len([x for x in all_links if x['estado']==state])} links so far")

        console.print(f"\n[bold]Total fresh links to download:[/] {len(all_links)}")
        ok = 0
        with Progress(TextColumn("legis"), BarColumn(), MofNCompleteColumn(), TextColumn("•"), TimeRemainingColumn(), console=console) as prog:
            task = prog.add_task("legis", total=len(all_links))
            for it in all_links:
                try:
                    r = fetch(c, it["url"])
                except Exception as e:
                    conn.execute("INSERT OR IGNORE INTO legis(estado, nombre, url_pdf, ext, status) VALUES(?,?,?,?,?)",
                                 (it["estado"], it["nombre"], it["url"], it["ext"], f"err: {e}"))
                    conn.commit(); prog.advance(task); continue
                if r.status_code != 200:
                    conn.execute("INSERT OR IGNORE INTO legis(estado, nombre, url_pdf, ext, status) VALUES(?,?,?,?,?)",
                                 (it["estado"], it["nombre"], it["url"], it["ext"], f"http_{r.status_code}"))
                    conn.commit(); prog.advance(task); continue
                text = ""
                if it["ext"] == "pdf" and r.content[:5] == b"%PDF-":
                    text = parse_pdf_text(r.content)
                conn.execute("""INSERT OR REPLACE INTO legis(estado, nombre, url_pdf, ext, bytes, raw_text,
                                                              byte_size, status, fetched_at)
                                VALUES(?,?,?,?,?,?,?,?,datetime('now'))""",
                             (it["estado"], it["nombre"], it["url"], it["ext"],
                              r.content, text, len(r.content), "ok"))
                conn.commit()
                ok += 1
                prog.advance(task)
                time.sleep(0.25)
        for state in STATE_PAGES:
            n = conn.execute("SELECT COUNT(*) FROM legis WHERE estado=? AND status='ok'", (state,)).fetchone()[0]
            console.print(f"[green]{state}: {n} leyes descargadas[/]")


if __name__ == "__main__":
    crawl()
