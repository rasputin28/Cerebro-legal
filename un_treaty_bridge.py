"""
UN Treaty Collection bridge — adds Mexico's reservas y declaraciones to the
sre_tratados table.

Approach (pragmatic v1):
  - For each sre_tratado classified as 'ddhh' or in a UN core treaty list,
    look up its UN Treaty Collection record by mtdsg_no.
  - Pull the participants page; isolate Mexico's row and any associated
    reservation / declaration footnotes.
  - Store the raw extracted text in sre_tratados.reservas.

UNTC does not expose a clean JSON API — we scrape the published HTML. The
treaty IDs (mtdsg_no) are not in our SRE data, so this script also includes a
small starter dictionary mapping common UN human-rights treaties to their
mtdsg_no. Extend it as needed.

This script must be run AFTER sre_crawler.py phase=detail has finished.
"""
from __future__ import annotations

import re
import sqlite3
import time
from pathlib import Path

import httpx
from selectolax.parser import HTMLParser

DB_PATH = Path(__file__).parent / "sre.db"

# Curated starter map of core UN human-rights treaties (chapter IV).
# Add more by inspecting https://treaties.un.org/pages/ParticipationStatus.aspx
UN_TREATY_MAP: dict[str, dict] = {
    "pacto internacional de derechos civiles y politicos": {
        "mtdsg_no": "IV-4", "chapter": "4", "title_en": "ICCPR",
    },
    "pacto internacional de derechos economicos sociales y culturales": {
        "mtdsg_no": "IV-3", "chapter": "4", "title_en": "ICESCR",
    },
    "convencion contra la tortura": {
        "mtdsg_no": "IV-9", "chapter": "4", "title_en": "CAT",
    },
    "convencion sobre los derechos del niño": {
        "mtdsg_no": "IV-11", "chapter": "4", "title_en": "CRC",
    },
    "convencion sobre la eliminacion de todas las formas de discriminacion contra la mujer": {
        "mtdsg_no": "IV-8", "chapter": "4", "title_en": "CEDAW",
    },
    "convencion internacional sobre la eliminacion de todas las formas de discriminacion racial": {
        "mtdsg_no": "IV-2", "chapter": "4", "title_en": "ICERD",
    },
    "convencion sobre los derechos de las personas con discapacidad": {
        "mtdsg_no": "IV-15", "chapter": "4", "title_en": "CRPD",
    },
    "convencion para la prevencion y la sancion del delito de genocidio": {
        "mtdsg_no": "IV-1", "chapter": "4", "title_en": "Genocide Convention",
    },
    "convencion internacional para la proteccion de todas las personas contra las desapariciones forzadas": {
        "mtdsg_no": "IV-16", "chapter": "4", "title_en": "ICPPED",
    },
    "convencion internacional sobre la proteccion de los derechos de todos los trabajadores migratorios": {
        "mtdsg_no": "IV-13", "chapter": "4", "title_en": "ICMW",
    },
}


def normalize(s: str) -> str:
    """Lowercase, strip accents, collapse whitespace, drop punctuation."""
    import unicodedata
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    s = s.lower()
    s = re.sub(r"[^a-z0-9 ]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def match_un_treaty(nombre: str) -> dict | None:
    nn = normalize(nombre)
    for key, info in UN_TREATY_MAP.items():
        if key in nn:
            return info
    return None


def fetch_participation_page(mtdsg_no: str, chapter: str) -> str | None:
    url = (
        "https://treaties.un.org/Pages/ViewDetails.aspx"
        f"?src=TREATY&mtdsg_no={mtdsg_no}&chapter={chapter}&clang=_en"
    )
    with httpx.Client(follow_redirects=True, timeout=30,
                      headers={"User-Agent": "Mozilla/5.0"}) as c:
        try:
            r = c.get(url)
            if r.status_code == 200 and len(r.text) > 5000:
                return r.text
        except Exception as e:
            print(f"  fetch err: {e}")
    return None


def extract_mexico_reservas(html: str) -> str | None:
    """Look for the participant table row for Mexico and any associated
    footnotes / declarations. UNTC pages put declarations in <span> or <div>
    after the participants table; we just isolate the "Mexico" section."""
    tree = HTMLParser(html)
    # Try to find a row containing "Mexico"
    text = tree.body.text(deep=True, separator="\n") if tree.body else ""
    # Heuristic: capture from "Mexico" up to next country name or 2 paragraphs
    m = re.search(r"\bMexico\b(.{50,3000}?)(?=\n[A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,3}\b|\Z)",
                  text, re.S)
    if m:
        snippet = m.group(0).strip()
        # Filter out menu items / boilerplate
        snippet = re.sub(r"\n{3,}", "\n\n", snippet)
        return snippet[:5000]
    return None


def main():
    if not DB_PATH.exists():
        print(f"DB not found: {DB_PATH}")
        return
    conn = sqlite3.connect(DB_PATH)

    rows = conn.execute(
        "SELECT token_sre, nombre, tipo FROM sre_tratados "
        "WHERE tipo='ddhh' AND reservas IS NULL AND nombre IS NOT NULL"
    ).fetchall()
    print(f"DDHH tratados pending UN bridge: {len(rows)}")

    matched = 0
    for tok, nombre, tipo in rows:
        info = match_un_treaty(nombre)
        if not info:
            continue
        print(f"\n→ {nombre[:60]!r} → {info['title_en']} ({info['mtdsg_no']})")
        html = fetch_participation_page(info["mtdsg_no"], info["chapter"])
        if not html:
            print("  no UN page")
            continue
        res = extract_mexico_reservas(html)
        if res:
            conn.execute(
                "UPDATE sre_tratados SET reservas=? WHERE token_sre=?",
                (res, tok),
            )
            conn.commit()
            matched += 1
            print(f"  reservas extracted ({len(res)} chars)")
        else:
            print("  no Mexico section found")
        time.sleep(1.0)

    print(f"\nDone. {matched}/{len(rows)} bridged.")


if __name__ == "__main__":
    main()
