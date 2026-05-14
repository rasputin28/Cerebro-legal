"""
Crawler federal de leyes/códigos/reglamentos/estatutos vía diputados.gob.mx/LeyesBiblio.

The site lists every federal-level ordenamiento with multiple formats (PDF/Word/HTML)
plus the date of última reforma. Strategy:
  1. Fetch the master index.htm
  2. Extract every law row: nombre, abbreviation, file links (pdf/doc/htm), última reforma
  3. For each PDF link, download the PDF → store bytes + extract text
  4. For each HTM link, download → store HTML body
Resume-safe: skips already-downloaded files by URL.

Schema (dip_leyes.db):
  dip_leyes(law_id PK AUTOINCREMENT, slug, nombre, abreviatura, ultima_reforma TEXT,
            categoria TEXT, status TEXT DEFAULT 'vigente', primary_url TEXT, fetched_at)
  dip_archivos(arch_id PK AUTOINCREMENT, law_id FK, url, ext, bytes BLOB, byte_size,
               http_status, fetched_at, parsed_text TEXT)
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

BASE = "https://www.diputados.gob.mx/LeyesBiblio"
DB_PATH = Path(__file__).parent / "dip_leyes.db"
HEADERS = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) Chrome/131.0"}


def init_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS dip_leyes (
            law_id INTEGER PRIMARY KEY AUTOINCREMENT,
            slug TEXT,
            nombre TEXT NOT NULL,
            abreviatura TEXT,
            ultima_reforma TEXT,
            categoria TEXT,
            status TEXT DEFAULT 'vigente',
            primary_url TEXT,
            fetched_at TEXT DEFAULT (datetime('now')),
            UNIQUE(nombre, abreviatura)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS dip_archivos (
            arch_id INTEGER PRIMARY KEY AUTOINCREMENT,
            law_id INTEGER REFERENCES dip_leyes(law_id),
            url TEXT NOT NULL UNIQUE,
            ext TEXT NOT NULL,
            byte_size INTEGER,
            http_status INTEGER,
            bytes BLOB,
            parsed_text TEXT,
            fetched_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_arch_law ON dip_archivos(law_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_arch_ext ON dip_archivos(ext)")
    conn.commit()
    return conn


def absolutize(href: str, base_url: str) -> str:
    if href.startswith("http"):
        return href
    if href.startswith("//"):
        return "https:" + href
    if href.startswith("/"):
        return f"https://www.diputados.gob.mx{href}"
    base_dir = base_url.rsplit("/", 1)[0]
    while href.startswith("../"):
        href = href[3:]
        base_dir = base_dir.rsplit("/", 1)[0]
    return f"{base_dir}/{href}"


# Parse rows of the form (from observed HTML):
#   <tr><td>...nombre...</td><td>abrev</td><td>última reforma</td>
#       <td><a href="...pdf">PDF</a></td><td><a href="...doc">Word</a></td><td><a href="...htm">html</a></td></tr>
ROW_RE = re.compile(
    r"<tr[^>]*>\s*(?P<inner>.*?)\s*</tr>",
    re.S | re.I,
)
# Each cell
CELL_RE = re.compile(r"<td[^>]*>(?P<cell>.*?)</td>", re.S | re.I)
# Permissive link regex; the law catalog uses nested <font> tags so we allow tags inside.
LINK_RE  = re.compile(r'<a[^>]+href="(?P<href>[^"]+)"[^>]*>(?P<text>.{0,300}?)</a>', re.S | re.I)
TAG_RE  = re.compile(r"<[^>]+>")


def clean(s: str) -> str:
    s = TAG_RE.sub(" ", s)
    s = s.replace("&nbsp;", " ").replace("&amp;", "&").replace("&aacute;", "á") \
         .replace("&eacute;", "é").replace("&iacute;", "í").replace("&oacute;", "ó") \
         .replace("&uacute;", "ú").replace("&ntilde;", "ñ")
    return re.sub(r"\s+", " ", s).strip()


def parse_index(html: str, base_url: str, category: str) -> list[dict]:
    """Walk all <tr> rows; collect rows that contain at least one PDF/DOC link.
    The catalog rows look like:
       <tr>...<td>Nombre largo de la ley</td><td>Abrev</td><td>Fecha</td>
              <td><a href="...pdf">PDF</a></td><td><a href="...doc">Word</a></td>
              <td><a href="ref/xxx.htm">html</a></td>... </tr>
    """
    laws = []
    for tr_match in ROW_RE.finditer(html):
        inner = tr_match.group("inner")
        cells = [m.group("cell") for m in CELL_RE.finditer(inner)]
        if len(cells) < 2:
            continue
        # File links — only the actual law-content files. Reference htm pages in 'ref/' are
        # parallel content for the same law (cpeum.htm = reformas history); include them too.
        file_links = []
        for c in cells:
            for lm in LINK_RE.finditer(c):
                href = lm.group("href").strip()
                ext = href.rsplit(".", 1)[-1].split("#")[0].split("?")[0].lower() if "." in href else None
                if ext in ("pdf", "doc", "docx") or (ext == "htm" and "/" in href and not href.endswith("index.htm")):
                    file_links.append({"ext": ext, "url": absolutize(href, base_url)})
        # Need at least one PDF/DOC to call it a law row
        if not any(f["ext"] in ("pdf", "doc", "docx") for f in file_links):
            continue
        text_cells = [clean(c) for c in cells]
        # Drop tiny labels (PDF/Word/htm)
        text_cells = [t for t in text_cells if t and not re.match(r"^(PDF|Word|html?|Doc)$", t, re.I)]
        if not text_cells:
            continue
        nombre = text_cells[0]
        if len(nombre) < 8:
            # Maybe the first cell was just an abbreviation; use the longest cell as nombre
            longest = max(text_cells, key=len)
            if len(longest) >= 8:
                nombre = longest
            else:
                continue
        abrev = None
        for t in text_cells:
            if t != nombre and 2 < len(t) < 20 and re.match(r"^[A-Z][\w\.]{1,18}$", t):
                abrev = t
                break
        fecha = None
        for t in text_cells:
            if re.search(r"\d{1,2}[-/\s]?\w+[-/\s]?\d{4}|DOF[-\s]\w+[-\s]\d{4}", t, re.I):
                fecha = t
                break
        laws.append({
            "nombre": nombre,
            "abreviatura": abrev,
            "ultima_reforma": fecha,
            "categoria": category,
            "files": file_links,
        })
    return laws


def crawl_index(conn: sqlite3.Connection) -> int:
    """Pull the master index.htm and insert one row per law."""
    url = f"{BASE}/index.htm"
    console.print(f"[bold]fetching index:[/] {url}")
    with httpx.Client(http2=True, headers=HEADERS, timeout=60, follow_redirects=True) as c:
        r = c.get(url)
        r.raise_for_status()
        # The site is ISO-8859-1 (per Content-Type header).
        html = r.content.decode("latin-1")
        console.print(f"  index size: {len(html):,} chars")
        laws = parse_index(html, url, "general")
        console.print(f"  laws parsed: {len(laws)}")

        n_new = 0
        for law in laws:
            primary_url = law["files"][0]["url"] if law["files"] else None
            cur = conn.execute("""
                INSERT INTO dip_leyes(nombre, abreviatura, ultima_reforma, categoria, primary_url)
                VALUES(?,?,?,?,?)
                ON CONFLICT(nombre, abreviatura) DO UPDATE SET
                    ultima_reforma=excluded.ultima_reforma,
                    categoria=excluded.categoria,
                    primary_url=excluded.primary_url,
                    fetched_at=datetime('now')
                RETURNING law_id
            """, (law["nombre"], law["abreviatura"], law["ultima_reforma"], law["categoria"], primary_url))
            row = cur.fetchone()
            law_id = row[0] if row else None
            if not law_id:
                law_id = conn.execute(
                    "SELECT law_id FROM dip_leyes WHERE nombre=? AND IFNULL(abreviatura,'')=IFNULL(?,'')",
                    (law["nombre"], law["abreviatura"])
                ).fetchone()[0]
            for f in law["files"]:
                conn.execute(
                    "INSERT OR IGNORE INTO dip_archivos(law_id, url, ext) VALUES(?,?,?)",
                    (law_id, f["url"], f["ext"])
                )
            n_new += 1
        conn.commit()
        return n_new


@retry(reraise=True, stop=stop_after_attempt(5),
       wait=wait_exponential(multiplier=1, min=2, max=30),
       retry=retry_if_exception_type((httpx.HTTPError, httpx.RemoteProtocolError, httpx.ReadTimeout)))
def download(client: httpx.Client, url: str) -> httpx.Response:
    return client.get(url, timeout=120)


def crawl_files(conn: sqlite3.Connection, limit: int | None = None) -> int:
    rows = conn.execute(
        "SELECT arch_id, url, ext FROM dip_archivos WHERE bytes IS NULL ORDER BY arch_id"
    ).fetchall()
    if limit:
        rows = rows[:limit]
    console.print(f"[bold]pending downloads:[/] {len(rows)}")

    ok = 0
    with httpx.Client(http2=True, headers=HEADERS, timeout=120, follow_redirects=True) as c:
        with Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TextColumn("•"),
            TimeRemainingColumn(),
            console=console,
        ) as prog:
            task = prog.add_task("files", total=len(rows))
            for arch_id, url, ext in rows:
                try:
                    r = download(c, url)
                except Exception as e:
                    conn.execute("UPDATE dip_archivos SET http_status=-1, parsed_text=? WHERE arch_id=?",
                                 (f"err: {e}", arch_id))
                    conn.commit()
                    prog.advance(task)
                    continue
                # Try parsed_text immediately for htm files (small + useful)
                parsed = None
                if ext in ("htm", "html") and r.status_code == 200:
                    try:
                        parsed = clean(r.text)
                        # cap to 5 MB
                        parsed = parsed[:5_000_000]
                    except Exception:
                        parsed = None
                conn.execute(
                    "UPDATE dip_archivos SET bytes=?, byte_size=?, http_status=?, parsed_text=?, fetched_at=datetime('now') WHERE arch_id=?",
                    (r.content if r.status_code == 200 else None, len(r.content), r.status_code, parsed, arch_id)
                )
                conn.commit()
                if r.status_code == 200:
                    ok += 1
                prog.advance(task)
                time.sleep(0.15)
    return ok


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--phase", choices=["index", "files", "all"], default="all")
    ap.add_argument("--limit", type=int)
    args = ap.parse_args()

    conn = init_db(DB_PATH)
    console.print(f"[bold]DB:[/] {DB_PATH}")

    t0 = time.time()
    if args.phase in ("index", "all"):
        n = crawl_index(conn)
        console.print(f"[green]index laws: {n}[/]")
    if args.phase in ("files", "all"):
        n = crawl_files(conn, limit=args.limit)
        console.print(f"[green]files downloaded: {n}[/]")

    n_laws = conn.execute("SELECT COUNT(*) FROM dip_leyes").fetchone()[0]
    n_arch = conn.execute("SELECT COUNT(*) FROM dip_archivos").fetchone()[0]
    n_dl = conn.execute("SELECT COUNT(*) FROM dip_archivos WHERE bytes IS NOT NULL").fetchone()[0]
    console.print(f"[bold]laws:[/] {n_laws}  [bold]archivos:[/] {n_arch}  [bold]downloaded:[/] {n_dl}")
    console.print(f"[bold]elapsed:[/] {time.time() - t0:.1f}s")


if __name__ == "__main__":
    sys.exit(main())
