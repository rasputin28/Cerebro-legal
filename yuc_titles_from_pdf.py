"""Extract Yucatán law titles from PDF first-page text."""
import re, sqlite3
from pathlib import Path

DB = Path(__file__).parent / "legis_estatales.db"


# Common opening patterns: title is in first 1-3 lines after H. CONGRESO / SECRETARÍA header
TITLE_PATTERNS = [
    # H. CONGRESO DEL ESTADO DE YUCATÁN \n SECRETARÍA ... \n LEY DE...
    re.compile(
        r"(?:H\.?\s*CONGRESO[^\n]*YUCAT[ÁA]N\s*[\n\s]+)?"
        r"(?:[A-ZÁÉÍÓÚÑ\s\.]*?SECRETAR[ÍI]A[^\n]*[\n\s]+)?"
        r"((?:LEY|CÓDIGO|REGLAMENTO|DECRETO|ESTATUTO|MANUAL|ACUERDO|REGLAS|LINEAMIENTOS|ARANCEL)\s+[A-ZÁÉÍÓÚÑÜ\d\s,\-\.\(\)/]{15,250})",
        re.IGNORECASE,
    ),
]


def extract_title(raw_text: str) -> str | None:
    if not raw_text: return None
    # Take first 3000 chars to find title
    sample = raw_text[:3000]
    for pat in TITLE_PATTERNS:
        m = pat.search(sample)
        if m:
            t = re.sub(r"\s+", " ", m.group(1)).strip()
            # Strip "Última Reforma ..." trailing
            t = re.sub(r"\s+[ÚU]ltima\s+[Rr]eforma.*$", "", t)
            # Truncate at first sentence break
            if "." in t and len(t) > 50:
                t = t.split(".")[0]
            return t[:300]
    return None


conn = sqlite3.connect(DB)
rows = conn.execute("SELECT law_id, raw_text FROM legis WHERE estado='YUC' AND raw_text IS NOT NULL AND length(raw_text) > 200").fetchall()
print(f"extracting titles from {len(rows)} YUC PDFs")
fixed = 0
for lid, text in rows:
    t = extract_title(text)
    if t:
        conn.execute("UPDATE legis SET nombre=? WHERE law_id=?", (t, lid))
        fixed += 1
conn.commit()
print(f"  fixed: {fixed}")

print("\n--- 15 samples after PDF-title extract ---")
for n, in conn.execute("SELECT substr(nombre,1,100) FROM legis WHERE estado='YUC' AND nombre IS NOT NULL ORDER BY RANDOM() LIMIT 15"):
    print(f"  {n}")

# Same for YUC_CODIGOS
rows = conn.execute("SELECT law_id, raw_text FROM legis WHERE estado='YUC_CODIGOS' AND raw_text IS NOT NULL").fetchall()
print(f"\nextracting titles from {len(rows)} YUC_CODIGOS PDFs")
for lid, text in rows:
    t = extract_title(text)
    if t:
        conn.execute("UPDATE legis SET nombre=? WHERE law_id=?", (t, lid))
conn.commit()
print("\n--- YUC_CODIGOS after fix ---")
for n, in conn.execute("SELECT substr(nombre,1,90) FROM legis WHERE estado='YUC_CODIGOS' ORDER BY nombre"):
    print(f"  {n}")
