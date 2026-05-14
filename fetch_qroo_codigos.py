"""Fetch the remaining 6 QRoo códigos that the catalog crawler missed."""
import httpx, sqlite3, io
import pdfplumber
from pathlib import Path

DB = Path(__file__).parent / "legis_estatales.db"

CODIGOS = [
    ("Código Civil para el Estado de Quintana Roo",                                  "https://documentos.congresoqroo.gob.mx/codigos/C1420150724272_2.pdf"),
    ("Código Fiscal Municipal del Estado de Quintana Roo",                           "https://documentos.congresoqroo.gob.mx/codigos/C4-XVIII-09122014-20250121T151435-L1420141209156.pdf"),
    ("Código Fiscal del Estado de Quintana Roo",                                     "https://documentos.congresoqroo.gob.mx/codigos/C1420141216234.pdf"),
    ("Código Penal para el Estado Libre y Soberano de Quintana Roo",                 "https://documentos.congresoqroo.gob.mx/codigos/C1420150724271_1.pdf"),
    ("Código de Procedimientos Civiles para el Estado Libre y Soberano de Quintana Roo", "https://documentos.congresoqroo.gob.mx/codigos/C1220101210004.pdf"),
    ("Código de Procedimientos Penales para el Estado Libre y Soberano de Quintana Roo (vigente)", "https://documentos.congresoqroo.gob.mx/codigos/C142014111915202.pdf"),
    ("Código de Justicia Administrativa del Estado de Quintana Roo (alt)",           "https://documentos.congresoqroo.gob.mx/codigos/C10-XV-20171227-141.pdf"),
    ("Código de Procedimientos y Justicia Administrativa del Estado de Quintana Roo","https://documentos.congresoqroo.gob.mx/codigos/C12-XVIII-09102024-20241018T095006-L1820241009013.pdf"),
]

H = {"User-Agent": "Mozilla/5.0 Chrome/131.0"}


def parse_pdf_text(b):
    try:
        with pdfplumber.open(io.BytesIO(b)) as pdf:
            return "\n".join((p.extract_text() or "") for p in pdf.pages[:50])
    except Exception:
        return ""


conn = sqlite3.connect(DB)
ok = 0
with httpx.Client(http2=True, headers=H, follow_redirects=True, verify=False, timeout=60) as c:
    for nombre, url in CODIGOS:
        # Skip if already in DB by URL
        if conn.execute("SELECT 1 FROM legis WHERE url_pdf=?", (url,)).fetchone():
            print(f"  skip (already in DB): {nombre[:60]}")
            continue
        r = c.get(url)
        if r.status_code != 200 or r.content[:5] != b"%PDF-":
            print(f"  ❌ {r.status_code}: {nombre[:60]}")
            continue
        text = parse_pdf_text(r.content)
        conn.execute(
            "INSERT OR REPLACE INTO legis(estado, nombre, url_pdf, ext, bytes, raw_text, byte_size, status, fetched_at) VALUES(?,?,?,?,?,?,?,?,datetime('now'))",
            ("QROO_CODIGOS", nombre, url, "pdf", r.content, text, len(r.content), "ok")
        )
        conn.commit()
        ok += 1
        print(f"  ✓ {nombre[:60]} ({len(r.content):,}b)")

print(f"\nFetched {ok} new códigos QRoo")
print(conn.execute("SELECT COUNT(*) FROM legis WHERE estado='QROO_CODIGOS'").fetchone())
