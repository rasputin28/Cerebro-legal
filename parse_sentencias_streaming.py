"""Streaming parse of sentencias_raw → sentencias_section. Memory-bounded."""
import sqlite3, sys, re, gc, time
from pathlib import Path
from selectolax.parser import HTMLParser

DB = Path(__file__).parent / "sentencias.db"
BATCH = 100


def classify(cls: str) -> str:
    cls_l = (cls or "").lower()
    if "datos" in cls_l: return "encabezado"
    if "ponente" in cls_l: return "ponente"
    if "resolutivo" in cls_l: return "resolutivo"
    if "considerando" in cls_l: return "considerando"
    if "antecedente" in cls_l: return "antecedente"
    if "precedente" in cls_l: return "precedente"
    if "voto" in cls_l: return "voto"
    if "corte" in cls_l: return "cuerpo"
    return "otro"


def parse(html: str) -> list[dict]:
    if not html: return []
    try:
        tree = HTMLParser(html)
    except Exception:
        return []
    sections = []
    paragraphs = tree.css("p, h1, h2, h3")
    if not paragraphs:
        body_text = tree.body.text(separator=" ", strip=True) if tree.body else ""
        if body_text:
            return [{"i": 0, "type": "cuerpo", "cls": None, "text": body_text[:8000]}]
        return []
    idx = 0
    for p in paragraphs:
        cls = p.attributes.get("class", "") or ""
        sec_type = classify(cls) if cls else ("titulo" if p.tag == "h1" else "subtitulo" if p.tag in ("h2","h3") else "parrafo")
        text = p.text(separator=" ", strip=True)
        if not text or len(text) < 3:
            continue
        sections.append({"i": idx, "type": sec_type, "cls": cls or None, "text": text[:8000]})
        idx += 1
    return sections


def main():
    conn = sqlite3.connect(DB)
    n_pending = conn.execute("""
        SELECT COUNT(*) FROM sentencias_meta m
        JOIN sentencias_raw r ON r.id_engrose = m.id_engrose
        WHERE m.parsed_at IS NULL
    """).fetchone()[0]
    print(f"pending parse: {n_pending}")
    if not n_pending: return

    done = 0
    t0 = time.time()
    cur = conn.cursor()
    while True:
        # Stream batch
        rows = cur.execute("""
            SELECT m.id_engrose, r.body_html
            FROM sentencias_meta m
            JOIN sentencias_raw r ON r.id_engrose = m.id_engrose
            WHERE m.parsed_at IS NULL
            LIMIT ?
        """, (BATCH,)).fetchall()
        if not rows: break

        for ie, html in rows:
            secs = parse(html)
            conn.execute("DELETE FROM sentencias_section WHERE id_engrose=?", (ie,))
            for s in secs:
                conn.execute(
                    "INSERT INTO sentencias_section(id_engrose, section_index, section_type, section_class, text, char_count) VALUES(?,?,?,?,?,?)",
                    (ie, s["i"], s["type"], s["cls"], s["text"], len(s["text"]))
                )
            conn.execute("UPDATE sentencias_meta SET parsed_at=datetime('now') WHERE id_engrose=?", (ie,))
            done += 1
        conn.commit()
        del rows
        gc.collect()
        rate = done / (time.time() - t0)
        eta_min = (n_pending - done) / rate / 60 if rate > 0 else 0
        print(f"  {done}/{n_pending}  ({rate:.0f}/s, ETA {eta_min:.0f}m)", flush=True)


if __name__ == "__main__":
    main()
