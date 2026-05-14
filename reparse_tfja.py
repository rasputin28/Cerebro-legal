"""Re-parse all tfja_tesis rows from their stored raw_text — no re-download."""
import sqlite3, sys
sys.path.insert(0, ".")
from tfja_crawler import parse_pdf, CLAVE_RE, MATERIA_RE, RUBRO_RE, JUICIO_RE, PONENTE_RE, SECRETARIO_RE, VOTACION_RE, FECHA_SESION_RE, RTFJA_RE, ANIO_REVISTA_RE, PRECEDENTE_SECTION_RE, PROC_START_RE, parse_clave_to_sala_epoca
import re

def reparse_from_raw(raw: str) -> dict:
    """Same logic as parse_pdf but takes raw_text directly (no pdfplumber)."""
    out: dict = {"raw_text": raw}

    def first(rx, src):
        m = rx.search(src)
        return m.group(1).strip() if m else None

    out["clave"] = first(CLAVE_RE, raw)
    mm = MATERIA_RE.search(raw)
    out["materia"] = " ".join(mm.group(1).split()).strip() if mm else None
    epoca, sala = parse_clave_to_sala_epoca(out["clave"])
    out["epoca"] = epoca
    out["sala"] = sala
    rm = RUBRO_RE.search(raw)
    out["rubro"] = " ".join(rm.group(1).split()).strip() if rm else None

    body_end = None
    prec_start = None
    pm = PRECEDENTE_SECTION_RE.search(raw)
    if pm:
        body_end = pm.start()
        prec_start = pm.end()
    else:
        rstart = rm.end() if rm else 0
        proc = PROC_START_RE.search(raw, rstart)
        if proc:
            body_end = proc.start()
            prec_start = proc.start()

    if rm and body_end is not None:
        out["texto"] = raw[rm.end():body_end].strip() or None
    else:
        out["texto"] = None

    if prec_start is not None:
        rtfja_m = RTFJA_RE.search(raw, prec_start)
        prec_end = rtfja_m.start() if rtfja_m else len(raw)
        out["precedente_raw"] = raw[prec_start:prec_end].strip() or None
    else:
        out["precedente_raw"] = None

    src = out["precedente_raw"] or raw
    out["juicio_num"] = first(JUICIO_RE, src)
    out["ponente"] = first(PONENTE_RE, src)
    out["secretario"] = first(SECRETARIO_RE, src)
    out["votacion"] = first(VOTACION_RE, src)
    out["fecha_sesion"] = first(FECHA_SESION_RE, src)

    m = RTFJA_RE.search(raw)
    out["revista_ref"] = m.group(0).strip() if m else None
    am = ANIO_REVISTA_RE.search(out["revista_ref"] or "")
    out["anio_revista"] = int(am.group(1)) if am else None

    return out


conn = sqlite3.connect("tfja.db")
rows = conn.execute("SELECT id_tfja, raw_text FROM tfja_tesis WHERE raw_text IS NOT NULL").fetchall()
print(f"re-parsing {len(rows)} rows...")
for tid, raw in rows:
    p = reparse_from_raw(raw)
    conn.execute("""
        UPDATE tfja_tesis SET
            epoca=?, sala=?, clave=?, materia=?, rubro=?, texto=?, precedente_raw=?,
            juicio_num=?, ponente=?, secretario=?, votacion=?, fecha_sesion=?,
            revista_ref=?, anio_revista=?
        WHERE id_tfja=?
    """, (p.get("epoca"), p.get("sala"), p.get("clave"), p.get("materia"), p.get("rubro"),
          p.get("texto"), p.get("precedente_raw"), p.get("juicio_num"), p.get("ponente"),
          p.get("secretario"), p.get("votacion"), p.get("fecha_sesion"),
          p.get("revista_ref"), p.get("anio_revista"), tid))
conn.commit()

n = conn.execute("SELECT COUNT(*) as n, SUM(clave IS NULL) as null_clave, SUM(materia IS NULL) as null_mat, SUM(rubro IS NULL) as null_rub, SUM(texto IS NULL) as null_tex, SUM(juicio_num IS NULL) as null_j, SUM(ponente IS NULL) as null_p, SUM(precedente_raw IS NULL) as null_pre, SUM(revista_ref IS NULL) as null_rev FROM tfja_tesis").fetchone()
print(f"\nN={n[0]}  null clave={n[1]}  materia={n[2]}  rubro={n[3]}  texto={n[4]}  juicio={n[5]}  ponente={n[6]}  prec={n[7]}  rev={n[8]}")
