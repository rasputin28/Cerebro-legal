"""Extract Yucatán law names from URL basenames (since the parser left them empty)."""
import re, sqlite3, urllib.parse
from pathlib import Path

DB = Path(__file__).parent / "legis_estatales.db"

# Patterns observed:
#   .../leyes/eyes/6def81_LEY-ASET-HUM (ultima ref 31-07-19).pdf
#   .../leyes/yucatan.gob.mx/storage/legislacion/leyes/ley_5.pdf
#   .../leyes/385142_ARANCEL-COB-HONO-ABOG.DOC
#   .../codigos/c42bae_Código de la Administración Pública...doc

def name_from_url(url: str) -> str:
    base = url.rsplit("/", 1)[-1]
    base = urllib.parse.unquote(base)
    base = re.sub(r"\.(pdf|docx?|html?)$", "", base, flags=re.I)
    # Drop hash prefix (e.g. "385142_" or "c42bae_")
    base = re.sub(r"^[0-9a-fA-F]{6,40}_+", "", base)
    base = re.sub(r"^[0-9]+_+", "", base)
    # Drop dates suffix like _2026-04-09
    base = re.sub(r"_\d{4}-\d{2}-\d{2}$", "", base)
    # Strip trailing "(última ref ...)" parenthetical
    base = re.sub(r"\s*\([^)]*ref[^)]*\)\s*$", "", base, flags=re.I)
    # Normalize separators
    base = base.replace("-", " ").replace("_", " ")
    base = re.sub(r"\s+", " ", base).strip()
    return base


conn = sqlite3.connect(DB)
rows = conn.execute("SELECT law_id, url_pdf FROM legis WHERE estado='YUC' AND (nombre IS NULL OR nombre='')").fetchall()
print(f"fixing {len(rows)} YUC names")
fixed = 0
for lid, url in rows:
    n = name_from_url(url)
    if n and len(n) > 2:
        conn.execute("UPDATE legis SET nombre=? WHERE law_id=?", (n[:300], lid))
        fixed += 1
conn.commit()
print(f"  fixed: {fixed}")

# Show samples
print("\n--- 10 Yucatán names after fix ---")
for nombre, in conn.execute("SELECT substr(nombre,1,80) FROM legis WHERE estado='YUC' AND nombre IS NOT NULL ORDER BY RANDOM() LIMIT 10"):
    print(f"  {nombre}")
