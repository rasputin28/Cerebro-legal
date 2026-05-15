"""
SRE reservas v2 — better extraction of Mexico's reservation text from UN HTML.

Previous version captured only "Mexico {date} {date}" (just dates, not the
actual reservation text). The UN page structure puts the reservation text in
a separate footnote/declaration block, often linked via superscript ²,³ etc.

Strategy: scrape the dedicated declarations/reservations table at end of page.
"""
import re, sqlite3, time
from pathlib import Path
import httpx
from selectolax.parser import HTMLParser

DB = Path(__file__).parent / "sre.db"

UN_TREATY_MAP = {
    "pacto internacional de derechos civiles y politicos":   ("IV-4", "4"),
    "pacto internacional de derechos economicos sociales y culturales": ("IV-3", "4"),
    "convencion contra la tortura":                          ("IV-9", "4"),
    "convencion sobre los derechos del nino":                ("IV-11", "4"),
    "convencion sobre la eliminacion de todas las formas de discriminacion contra la mujer": ("IV-8", "4"),
    "convencion internacional sobre la eliminacion de todas las formas de discriminacion racial": ("IV-2", "4"),
    "convencion sobre los derechos de las personas con discapacidad": ("IV-15", "4"),
    "convencion para la prevencion y la sancion del delito de genocidio": ("IV-1", "4"),
    "convencion internacional para la proteccion de todas las personas contra las desapariciones forzadas": ("IV-16", "4"),
    "convencion internacional sobre la proteccion de los derechos de todos los trabajadores migratorios": ("IV-13", "4"),
    "protocolo facultativo del pacto internacional de derechos civiles": ("IV-5", "4"),
    "segundo protocolo facultativo del pacto internacional": ("IV-12", "4"),
    "convencion sobre el estatuto de los refugiados":        ("V-2", "5"),
    "convencion unica de 1961 sobre estupefacientes":         ("VI-15", "6"),
    "convencion de las naciones unidas contra la delincuencia organizada": ("XVIII-12", "18"),
    "convencion de las naciones unidas contra la corrupcion": ("XVIII-14", "18"),
    "convencion de viena sobre relaciones diplomaticas":      ("III-3", "3"),
    "convencion de viena sobre relaciones consulares":        ("III-6", "3"),
    "convencion de viena sobre el derecho de los tratados":   ("XXIII-1", "23"),
    "convencion de las naciones unidas sobre el derecho del mar": ("XXI-6", "21"),
    "convencion marco de las naciones unidas sobre el cambio climatico": ("XXVII-7", "27"),
    "acuerdo de paris":                                       ("XXVII-7-d", "27"),
    "convenio sobre la diversidad biologica":                 ("XXVII-8", "27"),
    "convencion sobre los derechos politicos de la mujer":   ("XVI-1", "16"),
    "convencion sobre la nacionalidad de la mujer casada":   ("XVI-2", "16"),
    "convencion sobre el consentimiento para el matrimonio": ("XVI-3", "16"),
    "convencion sobre la imprescriptibilidad de los crimenes de guerra": ("IV-6", "4"),
    "convencion sobre el estatuto de los apatridas":         ("V-3", "5"),
    "convencion para reducir los casos de apatridia":        ("V-4", "5"),
    "protocolo facultativo de la convencion contra la tortura": ("IV-9-b", "4"),
    "protocolo de kyoto":                                     ("XXVII-7-a", "27"),
    "protocolo de nagoya":                                    ("XXVII-8-b", "27"),
    "convencion de basilea":                                  ("XXVII-3", "27"),
    "protocolo de montreal":                                  ("XXVII-2-a", "27"),
    "convencion contra el trafico ilicito de estupefacientes": ("VI-19", "6"),
    "convencion sobre el delito de genocidio":                ("IV-1", "4"),
    "protocolo para prevenir, reprimir y sancionar la trata": ("XVIII-12-a", "18"),
    "protocolo facultativo de la convencion sobre los derechos del nino relativo a la venta": ("IV-11-c", "4"),
    "protocolo facultativo de la convencion sobre los derechos del nino relativo a la participacion": ("IV-11-b", "4"),
}


def normalize(s):
    import unicodedata
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9 ]", " ", s.lower())).strip()


def match(name):
    nn = normalize(name)
    for k, v in UN_TREATY_MAP.items():
        if k in nn:
            return v
    return None


def fetch_un(mtdsg, chapter):
    url = f"https://treaties.un.org/Pages/ViewDetails.aspx?src=TREATY&mtdsg_no={mtdsg}&chapter={chapter}&clang=_en"
    try:
        r = httpx.get(url, follow_redirects=True, timeout=30,
                      headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code == 200 and len(r.text) > 5000:
            return r.text
    except Exception:
        return None
    return None


def extract_mexico_full(html):
    """Extract Mexico's full row + any associated declaration/reservation text.

    UN pages have:
      - Participant table with rows: Country | Sign date | Ratify date | superscripts
      - Below: 'Declarations and Reservations' section with full text per country
    """
    tree = HTMLParser(html)
    # Remove scripts
    for tag in ("script", "style"):
        for n in tree.css(tag): n.decompose()

    body_text = tree.body.text(separator="\n", strip=True) if tree.body else ""

    # Find "Declarations and Reservations" section
    # The section header typically: "Declarations and Reservations" then bullet list per country
    m = re.search(r"(Declarations?\s+(?:and|&)\s+Reservations?|End-note)(.{200,80000}?)(?:Footnotes|See chapter|©|\Z)",
                  body_text, re.S | re.I)
    if not m:
        # Fallback: just grab Mexico row from participant list
        rm = re.search(r"^\s*Mexico\b([^\n]{0,300})", body_text, re.M)
        return rm.group(0).strip()[:5000] if rm else None

    decl_section = m.group(2)
    # Within declarations section, find Mexico-specific entry
    # Pattern: "Mexico" as line start, then everything until next country name
    mx = re.search(
        r"(?:^|\n)\s*Mexico\s*\n?(.{20,8000}?)(?=\n\s*[A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,3}\s*\n|\Z)",
        decl_section, re.S | re.M
    )
    if mx:
        text = mx.group(0).strip()
        # Clean excess whitespace
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text[:8000]
    return None


def main():
    conn = sqlite3.connect(DB)
    # Re-process all DDHH tratados that have only short reservas
    rows = conn.execute("""
        SELECT token_sre, nombre FROM sre_tratados
        WHERE tipo='ddhh' AND (reservas IS NULL OR length(reservas) < 200)
    """).fetchall()
    print(f"Re-processing {len(rows)} DDHH tratados")
    matched = 0
    for tok, nombre in rows:
        m = match(nombre)
        if not m: continue
        mtdsg, chapter = m
        html = fetch_un(mtdsg, chapter)
        if not html: continue
        res = extract_mexico_full(html)
        if res and len(res) > 100:
            conn.execute("UPDATE sre_tratados SET reservas=? WHERE token_sre=?", (res, tok))
            conn.commit()
            matched += 1
            print(f"  ✓ {nombre[:50]}: {len(res)} chars")
        time.sleep(0.8)
    print(f"\nTotal matched: {matched}")

    print("\n--- 3 samples after fix ---")
    for nombre, res in conn.execute("SELECT nombre, substr(reservas, 1, 500) FROM sre_tratados WHERE reservas IS NOT NULL AND length(reservas) > 200 ORDER BY RANDOM() LIMIT 3"):
        print(f"\n→ {nombre[:60]}")
        print(f"  {res}")


if __name__ == "__main__":
    main()
