"""
UN Treaty Collection bridge — adds Mexico's reservas/declaraciones to sre_tratados.

Cross-references SRE treaty names against UNTC's ParticipationStatus page
for each treaty's mtdsg_no. Scrapes the published HTML for Mexico's row + footnotes.

This script is run AFTER sre_crawler.py phase=detail has filled sre_tratados.
Extended dictionary covers core HR + refugees + drugs + treaties + climate.
"""
from __future__ import annotations
import re, sqlite3, time, unicodedata
from pathlib import Path
import httpx
from selectolax.parser import HTMLParser

DB_PATH = Path(__file__).parent / "sre.db"

UN_TREATY_MAP: dict[str, dict] = {
    # Chapter IV — Human Rights core
    "pacto internacional de derechos civiles y politicos":   {"mtdsg_no": "IV-4",  "chapter": "4", "title_en": "ICCPR"},
    "pacto internacional de derechos economicos sociales y culturales": {"mtdsg_no": "IV-3", "chapter": "4", "title_en": "ICESCR"},
    "convencion contra la tortura":                          {"mtdsg_no": "IV-9",  "chapter": "4", "title_en": "CAT"},
    "convencion sobre los derechos del nino":                {"mtdsg_no": "IV-11", "chapter": "4", "title_en": "CRC"},
    "convencion sobre la eliminacion de todas las formas de discriminacion contra la mujer": {"mtdsg_no": "IV-8", "chapter": "4", "title_en": "CEDAW"},
    "convencion internacional sobre la eliminacion de todas las formas de discriminacion racial": {"mtdsg_no": "IV-2", "chapter": "4", "title_en": "ICERD"},
    "convencion sobre los derechos de las personas con discapacidad": {"mtdsg_no": "IV-15", "chapter": "4", "title_en": "CRPD"},
    "convencion para la prevencion y la sancion del delito de genocidio": {"mtdsg_no": "IV-1", "chapter": "4", "title_en": "Genocide Convention"},
    "convencion internacional para la proteccion de todas las personas contra las desapariciones forzadas": {"mtdsg_no": "IV-16", "chapter": "4", "title_en": "ICPPED"},
    "convencion internacional sobre la proteccion de los derechos de todos los trabajadores migratorios": {"mtdsg_no": "IV-13", "chapter": "4", "title_en": "ICMW"},
    "protocolo facultativo del pacto internacional de derechos civiles":   {"mtdsg_no": "IV-5",  "chapter": "4", "title_en": "ICCPR-OP1"},
    "segundo protocolo facultativo del pacto internacional":               {"mtdsg_no": "IV-12", "chapter": "4", "title_en": "ICCPR-OP2"},
    "protocolo facultativo de la convencion sobre los derechos del nino relativo a la venta": {"mtdsg_no": "IV-11-c", "chapter": "4", "title_en": "OP-CRC-SC"},
    "protocolo facultativo de la convencion sobre los derechos del nino relativo a la participacion": {"mtdsg_no": "IV-11-b", "chapter": "4", "title_en": "OP-CRC-AC"},
    "protocolo facultativo de la convencion contra la tortura": {"mtdsg_no": "IV-9-b", "chapter": "4", "title_en": "OPCAT"},
    "convencion sobre la imprescriptibilidad de los crimenes de guerra": {"mtdsg_no": "IV-6", "chapter": "4", "title_en": "Non-Applicability of Statutory Limitations"},
    # Chapter V — Refugees + stateless
    "convencion sobre el estatuto de los refugiados":        {"mtdsg_no": "V-2", "chapter": "5", "title_en": "Refugees Convention 1951"},
    "convencion sobre el estatuto de los apatridas":         {"mtdsg_no": "V-3", "chapter": "5", "title_en": "Stateless Persons"},
    "convencion para reducir los casos de apatridia":        {"mtdsg_no": "V-4", "chapter": "5", "title_en": "Reduction of Statelessness"},
    # Women specific
    "convencion sobre los derechos politicos de la mujer":   {"mtdsg_no": "XVI-1", "chapter": "16", "title_en": "Political Rights of Women"},
    "convencion sobre la nacionalidad de la mujer casada":   {"mtdsg_no": "XVI-2", "chapter": "16", "title_en": "Nationality of Married Women"},
    "convencion sobre el consentimiento para el matrimonio": {"mtdsg_no": "XVI-3", "chapter": "16", "title_en": "Marriage Consent"},
    # Drugs
    "convencion unica de 1961 sobre estupefacientes":         {"mtdsg_no": "VI-15", "chapter": "6", "title_en": "Single Convention on Narcotic Drugs"},
    "convencion de las naciones unidas contra la delincuencia organizada": {"mtdsg_no": "XVIII-12", "chapter": "18", "title_en": "UNTOC"},
    "protocolo para prevenir, reprimir y sancionar la trata": {"mtdsg_no": "XVIII-12-a", "chapter": "18", "title_en": "Palermo Trafficking Protocol"},
    "convencion de las naciones unidas contra la corrupcion": {"mtdsg_no": "XVIII-14", "chapter": "18", "title_en": "UNCAC"},
    "convencion contra el trafico ilicito de estupefacientes": {"mtdsg_no": "VI-19", "chapter": "6", "title_en": "1988 Vienna Drug Convention"},
    # Diplomatic / treaties law
    "convencion de viena sobre relaciones diplomaticas":      {"mtdsg_no": "III-3", "chapter": "3", "title_en": "VCDR"},
    "convencion de viena sobre relaciones consulares":        {"mtdsg_no": "III-6", "chapter": "3", "title_en": "VCCR"},
    "convencion de viena sobre el derecho de los tratados":   {"mtdsg_no": "XXIII-1", "chapter": "23", "title_en": "VCLT"},
    # Law of the sea
    "convencion de las naciones unidas sobre el derecho del mar": {"mtdsg_no": "XXI-6", "chapter": "21", "title_en": "UNCLOS"},
    # Environment + climate
    "convencion marco de las naciones unidas sobre el cambio climatico": {"mtdsg_no": "XXVII-7", "chapter": "27", "title_en": "UNFCCC"},
    "protocolo de kyoto":                                     {"mtdsg_no": "XXVII-7-a", "chapter": "27", "title_en": "Kyoto Protocol"},
    "acuerdo de paris":                                       {"mtdsg_no": "XXVII-7-d", "chapter": "27", "title_en": "Paris Agreement"},
    "convenio sobre la diversidad biologica":                 {"mtdsg_no": "XXVII-8", "chapter": "27", "title_en": "CBD"},
    "protocolo de nagoya":                                    {"mtdsg_no": "XXVII-8-b", "chapter": "27", "title_en": "Nagoya Protocol"},
    "convencion de basilea":                                  {"mtdsg_no": "XXVII-3", "chapter": "27", "title_en": "Basel Convention"},
    "protocolo de montreal":                                  {"mtdsg_no": "XXVII-2-a", "chapter": "27", "title_en": "Montreal Protocol"},
}


def normalize(s: str) -> str:
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    s = re.sub(r"[^a-z0-9 ]", " ", s.lower())
    return re.sub(r"\s+", " ", s).strip()


def match(name: str) -> dict | None:
    nn = normalize(name)
    for k, info in UN_TREATY_MAP.items():
        if k in nn:
            return info
    return None


def fetch_un(mtdsg: str, chapter: str) -> str | None:
    url = f"https://treaties.un.org/Pages/ViewDetails.aspx?src=TREATY&mtdsg_no={mtdsg}&chapter={chapter}&clang=_en"
    try:
        r = httpx.get(url, follow_redirects=True, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code == 200 and len(r.text) > 5000:
            return r.text
    except Exception:
        pass
    return None


def extract_mexico(html: str) -> str | None:
    tree = HTMLParser(html)
    body = tree.body.text(separator="\n", strip=True) if tree.body else ""
    m = re.search(r"\bMexico\b(.{40,4000}?)(?=\n[A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,3}\b|\Z)", body, re.S)
    return re.sub(r"\n{3,}", "\n\n", m.group(0).strip())[:6000] if m else None


def main():
    if not DB_PATH.exists():
        print(f"DB not found: {DB_PATH}"); return
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute("SELECT token_sre, nombre FROM sre_tratados WHERE reservas IS NULL AND nombre IS NOT NULL").fetchall()
    print(f"Pending UN bridge: {len(rows)} (all non-reserved tratados)")
    matched = 0
    for tok, nombre in rows:
        info = match(nombre)
        if not info: continue
        print(f"\n→ {nombre[:60]!r} → {info['title_en']} ({info['mtdsg_no']})")
        html = fetch_un(info["mtdsg_no"], info["chapter"])
        if not html: continue
        res = extract_mexico(html)
        if res:
            conn.execute("UPDATE sre_tratados SET reservas=? WHERE token_sre=?", (res, tok))
            conn.commit()
            matched += 1
            print(f"  ✓ {len(res)} chars")
        time.sleep(0.8)
    print(f"\nMatched: {matched}")


if __name__ == "__main__":
    main()
