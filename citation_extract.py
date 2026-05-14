"""
Citation extraction over tesis.db.

Pure CPU pass over the 310k modern tesis. Extracts normative references from
texto_justificacion + texto_contenido + nota_precedente. Outputs three new
tables for the citation graph and high-precision keyword grouping.

Output tables (added to tesis.db):
  tesis_cita_articulo(registro_digital, ley_code, articulo, fraccion, parrafo, raw_match)
  tesis_cita_tratado(registro_digital, tratado_code, articulo, raw_match)
  tesis_cita_tesis(citing_rd, cited_clave)  -- citation to another tesis by clave
"""
from __future__ import annotations

import re
import sqlite3
import sys
from pathlib import Path

DB = Path(__file__).parent / "tesis.db"

# Knowledge bases
LEYES = {
    "CPEUM":     r"Constituci[oó]n\s+Pol[ií]tica(?:\s+de\s+los\s+Estados\s+Unidos\s+Mexicanos)?",
    "LA":        r"Ley\s+de\s+Amparo",
    "LFT":       r"Ley\s+Federal\s+del\s+Trabajo",
    "LFPCA":     r"Ley\s+Federal\s+de\s+Procedimiento\s+Contencioso\s+Administrativo",
    "CFF":       r"C[oó]digo\s+Fiscal\s+de\s+la\s+Federaci[oó]n",
    "CFPP":     r"C[oó]digo\s+Federal\s+de\s+Procedimientos\s+Penales",
    "CNPP":     r"C[oó]digo\s+Nacional\s+de\s+Procedimientos\s+Penales",
    "CFPC":      r"C[oó]digo\s+Federal\s+de\s+Procedimientos\s+Civiles",
    "CCF":       r"C[oó]digo\s+Civil\s+Federal",
    "CPF":       r"C[oó]digo\s+Penal\s+Federal",
    "CComer":    r"C[oó]digo\s+de\s+Comercio",
    "LISR":      r"Ley\s+del\s+Impuesto\s+sobre\s+la\s+Renta",
    "LIVA":      r"Ley\s+del\s+Impuesto\s+al\s+Valor\s+Agregado",
    "LGS":       r"Ley\s+General\s+de\s+Salud",
    "LGEEPA":    r"Ley\s+General\s+del\s+Equilibrio\s+Ecol[oó]gico",
    "LGRA":      r"Ley\s+General\s+de\s+Responsabilidades\s+Administrativas",
    "LSS":       r"Ley\s+del\s+Seguro\s+Social",
    "LGV":       r"Ley\s+General\s+de\s+V[ií]ctimas",
    "LFTAIP":    r"Ley\s+Federal\s+de\s+Transparencia",
    "LRPF":      r"Ley\s+de\s+Responsabilidad\s+Patrimonial",
    "LCAMM":     r"Ley\s+General\s+de\s+Acceso\s+de\s+las\s+Mujeres",
}

TRATADOS = {
    "CADH":      r"Convenci[oó]n\s+Americana(?:\s+sobre\s+Derechos\s+Humanos)?|Pacto\s+de\s+San\s+Jos[eé]",
    "PIDCP":     r"Pacto\s+Internacional\s+de\s+Derechos\s+Civiles\s+y\s+Pol[ií]ticos",
    "PIDESC":    r"Pacto\s+Internacional\s+de\s+Derechos\s+Econ[oó]micos,?\s+Sociales\s+y\s+Culturales",
    "CDN":       r"Convenci[oó]n\s+sobre\s+los\s+Derechos\s+del\s+Ni[ñn]o",
    "CAT":       r"Convenci[oó]n\s+contra\s+la\s+Tortura",
    "CEDAW":     r"Convenci[oó]n\s+sobre\s+la\s+Eliminaci[oó]n\s+de\s+Todas\s+las\s+Formas\s+de\s+Discriminaci[oó]n\s+contra\s+la\s+Mujer",
    "CDPD":      r"Convenci[oó]n\s+sobre\s+los\s+Derechos\s+de\s+las\s+Personas\s+con\s+Discapacidad",
    "CIDFP":     r"Convenci[oó]n\s+Interamericana\s+sobre\s+Desaparici[oó]n\s+Forzada",
    "Belem":     r"Convenci[oó]n\s+de\s+Bel[ée]m\s+do\s+Par[aá]",
    "DUDH":      r"Declaraci[oó]n\s+Universal\s+de\s+(?:los\s+)?Derechos\s+Humanos",
}

# Pattern: capture "artículo N (fracción X)? (párrafo Y)?" near a reference
ART_NEAR_RE = re.compile(
    r"(?:art[íi]culo|art\.?)\s+(?P<num>\d+(?:[\s,]\d+)*(?:\s+Bis(?:\s+\d+)?)?)"
    r"(?:[,\s]+fracci[oó]n(?:es)?\s+(?P<frac>[IVXLCDM]+(?:\s*y\s*[IVXLCDM]+)?))?"
    r"(?:[,\s]+(?:p[áa]rrafo|inciso)\s+(?P<parr>\w+))?",
    re.I,
)

# Tesis clave patterns:  '2a./J. 19/2021 (11a.)', 'P./J. 57/2014 (10a.)', 'I.6o.P.88 P', 'PR.A.CN. J/81 A (11a.)'
TESIS_CLAVE_RE = re.compile(
    r"\b("
    r"(?:[12]a\.?/J\.?|P\.?/J\.?|PC\.?\.?\w+\.?\s*J\.?|PR\.?\w+\.?\s*J\.?|[12]a\.?\s+CXVII)\s*\d+/\d{4}\s*\(\d{1,2}a?\.\)"
    r"|"
    r"\b[IVX]+\.\w{1,3}\.\w{1,4}\.\s*\d{1,4}\s+[A-Z]\s*\(\d{1,2}a?\.\)"
    r")",
    re.I,
)


def init_tables(conn: sqlite3.Connection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tesis_cita_articulo (
            citing_rd INTEGER NOT NULL,
            ley_code TEXT NOT NULL,
            articulo TEXT,
            fraccion TEXT,
            parrafo TEXT,
            raw_match TEXT,
            PRIMARY KEY(citing_rd, ley_code, articulo, fraccion, parrafo)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tesis_cita_tratado (
            citing_rd INTEGER NOT NULL,
            tratado_code TEXT NOT NULL,
            articulo TEXT,
            raw_match TEXT,
            PRIMARY KEY(citing_rd, tratado_code, articulo)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tesis_cita_tesis (
            citing_rd INTEGER NOT NULL,
            cited_clave TEXT NOT NULL,
            PRIMARY KEY(citing_rd, cited_clave)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cita_art_ley ON tesis_cita_articulo(ley_code)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cita_trat ON tesis_cita_tratado(tratado_code)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cita_clave ON tesis_cita_tesis(cited_clave)")
    conn.commit()


def find_law_refs(text: str, code: str, regex: re.Pattern):
    """Find article references near each match of the law pattern."""
    out = []
    for m in regex.finditer(text):
        # capture window around the law match
        i, j = m.start(), m.end()
        window = text[max(0, i - 200):min(len(text), j + 200)]
        for am in ART_NEAR_RE.finditer(window):
            out.append({
                "code": code,
                "articulo": am.group("num"),
                "fraccion": am.group("frac"),
                "parrafo": am.group("parr"),
                "raw_match": window[max(0, am.start() - 30):am.end() + 30],
            })
    return out


def process(conn: sqlite3.Connection, batch_size: int = 1000):
    init_tables(conn)
    LEY_PATTERNS = {k: re.compile(v, re.I) for k, v in LEYES.items()}
    TRAT_PATTERNS = {k: re.compile(v, re.I) for k, v in TRATADOS.items()}

    # Stream rows
    total = conn.execute("SELECT COUNT(*) FROM tesis WHERE texto_contenido IS NOT NULL").fetchone()[0]
    print(f"Processing {total} tesis...")
    processed = 0
    for rd, texto_full, texto_just in conn.execute("""
        SELECT registro_digital,
               COALESCE(texto_contenido, '') || ' ' || COALESCE(texto_hechos, '') || ' ' || COALESCE(texto_criterios_juridicos, ''),
               COALESCE(texto_justificacion, '')
        FROM tesis WHERE detail_fetched_at IS NOT NULL
    """):
        blob = (texto_full or "") + " " + (texto_just or "")
        if not blob.strip():
            processed += 1
            continue
        # Leyes
        for code, pat in LEY_PATTERNS.items():
            for cite in find_law_refs(blob, code, pat):
                conn.execute("""
                    INSERT OR IGNORE INTO tesis_cita_articulo(citing_rd, ley_code, articulo, fraccion, parrafo, raw_match)
                    VALUES(?,?,?,?,?,?)
                """, (rd, cite["code"], cite["articulo"], cite["fraccion"], cite["parrafo"], cite["raw_match"][:300]))
        # Tratados
        for code, pat in TRAT_PATTERNS.items():
            for cite in find_law_refs(blob, code, pat):
                conn.execute("""
                    INSERT OR IGNORE INTO tesis_cita_tratado(citing_rd, tratado_code, articulo, raw_match)
                    VALUES(?,?,?,?)
                """, (rd, cite["code"], cite["articulo"], cite["raw_match"][:300]))
        # Citas a otras tesis (claves)
        for tm in TESIS_CLAVE_RE.finditer(blob):
            clave = re.sub(r"\s+", " ", tm.group(1).strip())
            conn.execute(
                "INSERT OR IGNORE INTO tesis_cita_tesis(citing_rd, cited_clave) VALUES(?,?)",
                (rd, clave)
            )
        processed += 1
        if processed % batch_size == 0:
            conn.commit()
            print(f"  {processed}/{total}", end="\r", flush=True)
    conn.commit()
    print(f"\nDone. {processed} processed.")

    # Stats
    n_art = conn.execute("SELECT COUNT(*) FROM tesis_cita_articulo").fetchone()[0]
    n_trat = conn.execute("SELECT COUNT(*) FROM tesis_cita_tratado").fetchone()[0]
    n_tesis = conn.execute("SELECT COUNT(*) FROM tesis_cita_tesis").fetchone()[0]
    print(f"\ntesis_cita_articulo: {n_art:,}")
    print(f"tesis_cita_tratado: {n_trat:,}")
    print(f"tesis_cita_tesis:   {n_tesis:,}")

    print("\nTop leyes citadas:")
    for ley, n in conn.execute("SELECT ley_code, COUNT(*) FROM tesis_cita_articulo GROUP BY ley_code ORDER BY 2 DESC LIMIT 10"):
        print(f"  {ley}: {n:,}")
    print("\nTop tratados citados:")
    for trat, n in conn.execute("SELECT tratado_code, COUNT(*) FROM tesis_cita_tratado GROUP BY tratado_code ORDER BY 2 DESC LIMIT 10"):
        print(f"  {trat}: {n:,}")


def main():
    conn = sqlite3.connect(DB)
    process(conn)


if __name__ == "__main__":
    sys.exit(main())
