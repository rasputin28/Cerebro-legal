"""
Re-parse DOF dof_pubs.raw_html with proper content extraction.

The original parser stripped tags but left <script> content, producing
JS code as "text". Fix: use selectolax to remove <script>/<style>/<nav>
nodes BEFORE extracting text, plus target the actual content container.

DOF nota_detalle.php structure (confirmed via probe):
  - Body wrapped in <div id="DivDetalleNota"> or similar
  - Decree text inside <article> or main content div
  - Lots of nav/menu chrome to strip first
"""
import re, sqlite3
from pathlib import Path
from selectolax.parser import HTMLParser

DB = Path(__file__).parent / "dof.db"

# Block-level tags to remove entirely
DROP_TAGS = ("script", "style", "noscript", "iframe", "form")

# Containers where the actual decreto text lives (in priority order)
CONTENT_SELECTORS = [
    "#DivDetalleNota",
    "#detalleNota",
    "div.detalleNota",
    "article",
    "main",
    "div.content",
    "div#content",
]

# Boilerplate text fragments to strip (DOF page chrome)
BOILERPLATE = re.compile(
    r"(DOF\s*-\s*Diario Oficial de la Federación|jkmegamenu\.definemenu|"
    r"\$\(document\)\.ready|var nombre_cookie|function getCookie|ARRcookies|"
    r"setCookie|document\.cookie|window\.|<\?-?-|var\s+\w+\s*=)",
    re.I,
)


def clean(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def extract(html: str) -> str:
    if not html: return ""
    tree = HTMLParser(html)
    # Strip noise tags
    for tag in DROP_TAGS:
        for n in tree.css(tag):
            n.decompose()
    # Try content selectors
    for sel in CONTENT_SELECTORS:
        n = tree.css_first(sel)
        if n:
            text = n.text(separator=" ", strip=True)
            if text and len(text) > 200 and not BOILERPLATE.search(text[:200]):
                return clean(text)
    # Fallback: body minus chrome
    if tree.body:
        text = tree.body.text(separator=" ", strip=True)
        # Find first real content marker — DOF decrees usually start with org name
        m = re.search(
            r"(SECRETARÍA DE|PODER (?:EJECUTIVO|JUDICIAL)|"
            r"DECRETO|ACUERDO|RESOLUCIÓN|AVISO|CONVENIO|REGLAMENTO|MANUAL|"
            r"PROGRAMA|ANTEPROYECTO|FE\s+DE\s+ERRATAS)\b",
            text
        )
        if m:
            return clean(text[m.start():])
        return clean(text)
    return ""


def main():
    conn = sqlite3.connect(DB)
    n = conn.execute("SELECT COUNT(*) FROM dof_pubs WHERE raw_html IS NOT NULL").fetchone()[0]
    print(f"re-parsing {n} dof rows...")
    cur = conn.cursor()
    cur.execute("SELECT codigo, raw_html FROM dof_pubs WHERE raw_html IS NOT NULL")
    done = 0
    while True:
        rows = cur.fetchmany(200)
        if not rows: break
        for codigo, html in rows:
            text = extract(html)
            conn.execute("UPDATE dof_pubs SET texto=? WHERE codigo=?", (text[:300000], codigo))
            done += 1
        conn.commit()
        if done % 2000 == 0:
            print(f"  {done}/{n}", flush=True)
    print(f"\nDone. {done}/{n} re-parsed.")
    # Sample verification
    print("\n--- 3 samples after re-parse ---")
    for r in conn.execute("SELECT codigo, substr(texto,1,300) FROM dof_pubs WHERE status='ok' ORDER BY RANDOM() LIMIT 3"):
        print(f"\n[{r[0]}]")
        print(f"  {r[1]}")


if __name__ == "__main__":
    main()
