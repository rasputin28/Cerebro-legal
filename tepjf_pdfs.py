"""TEPJF — bajar TODAS las compilaciones PDFs estáticas del RepositorioJurisprudencia."""
import re, sqlite3, time
from pathlib import Path
import httpx
import pdfplumber, io

DB_PATH = Path(__file__).parent / "tepjf.db"
H = {"User-Agent": "Mozilla/5.0 Chrome/131.0"}


def init_db(path: Path):
    conn = sqlite3.connect(path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tepjf_pdfs (
            url TEXT PRIMARY KEY,
            nombre TEXT,
            tipo TEXT,
            anio INTEGER,
            byte_size INTEGER,
            pdf_bytes BLOB,
            raw_text TEXT,
            status TEXT,
            fetched_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.commit()
    return conn


KNOWN_PDFS = [
    # Acuerdos publicados que vimos en recon
    "https://www.te.gob.mx/RepositorioJurisprudencia/Acuerdo%203-2021_TEPJF.pdf",
    "https://www.te.gob.mx/RepositorioJurisprudencia/Acuerdo%204-2021_TEPJF.pdf",
    # Compilaciones oficiales — patrones comunes
]

# Generate candidate URLs to brute-force the repository
PATTERNS = [
    "https://www.te.gob.mx/RepositorioJurisprudencia/Acuerdo_{n}-{y}_TEPJF.pdf",
    "https://www.te.gob.mx/RepositorioJurisprudencia/Acuerdo%20{n}-{y}_TEPJF.pdf",
    "https://www.te.gob.mx/RepositorioJurisprudencia/Acuerdo_{n}_{y}_TEPJF.pdf",
    "https://www.te.gob.mx/RepositorioJurisprudencia/Jurisprudencia_{n}_{y}.pdf",
    "https://www.te.gob.mx/RepositorioJurisprudencia/Jurisprudencia%20{n}-{y}.pdf",
    "https://www.te.gob.mx/RepositorioJurisprudencia/Tesis_{n}_{y}.pdf",
    "https://www.te.gob.mx/RepositorioJurisprudencia/Tesis%20{n}-{y}.pdf",
    "https://www.te.gob.mx/RepositorioJurisprudencia/Compilacion_{y}.pdf",
    "https://www.te.gob.mx/RepositorioJurisprudencia/Memoria_{y}.pdf",
    "https://www.te.gob.mx/RepositorioJurisprudencia/Resena_{y}.pdf",
]


def parse_pdf_text(pdf_bytes: bytes) -> str:
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            return "\n".join((p.extract_text() or "") for p in pdf.pages[:50])
    except Exception:
        return ""


def main():
    conn = init_db(DB_PATH)
    # Generate candidates
    candidates: list[str] = list(KNOWN_PDFS)
    for y in range(2000, 2026):
        for n in range(1, 30):
            for tpl in PATTERNS:
                candidates.append(tpl.format(n=n, y=y))

    candidates = list(dict.fromkeys(candidates))
    print(f"probing {len(candidates)} URLs")
    ok = 0
    with httpx.Client(http2=True, headers=H, timeout=45, follow_redirects=True) as c:
        for i, url in enumerate(candidates):
            if conn.execute("SELECT 1 FROM tepjf_pdfs WHERE url=?", (url,)).fetchone():
                continue
            try:
                # HEAD first to save bandwidth
                hr = c.head(url, timeout=15)
            except Exception:
                continue
            if hr.status_code != 200 or "application/pdf" not in hr.headers.get("content-type", ""):
                continue
            # GET the file
            try:
                r = c.get(url)
            except Exception:
                continue
            if r.status_code != 200 or r.content[:5] != b"%PDF-":
                continue
            text = parse_pdf_text(r.content)
            name = re.search(r"/([^/]+)\.pdf$", url).group(1)
            anio = None
            am = re.search(r"_(\d{4})_?", url)
            if am: anio = int(am.group(1))
            conn.execute("""
                INSERT OR REPLACE INTO tepjf_pdfs(url, nombre, anio, byte_size, pdf_bytes, raw_text, status, fetched_at)
                VALUES(?,?,?,?,?,?,?,datetime('now'))
            """, (url, name, anio, len(r.content), r.content, text[:200000], "ok"))
            conn.commit()
            ok += 1
            print(f"  HIT {i:>4}/{len(candidates)}: {name} ({len(r.content):,}b)")
            time.sleep(0.2)
    print(f"\ntepjf_pdfs ok: {ok}")


if __name__ == "__main__":
    main()
