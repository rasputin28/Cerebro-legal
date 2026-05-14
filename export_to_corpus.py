"""
Export per-source SQLite DBs → unified corpus.db (Postgres-ready).

Builds the schema described in CORPUS_SCHEMA.md as SQLite (for staging) plus
emits JSONL files that can be bulk-loaded into Postgres with \\COPY:
  corpus_document.jsonl
  corpus_chunk.jsonl
  corpus_citation.jsonl

Each source has its own mapping function that produces (document_row, chunks)
tuples. Chunks are produced at the semantic boundaries documented in the
schema doc so that downstream pgvector indexing produces good retrievals.

Run:
  .venv/bin/python export_to_corpus.py --source all
  .venv/bin/python export_to_corpus.py --source scjn_tesis
"""
from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
from pathlib import Path
from typing import Iterator

HERE = Path(__file__).parent
CORPUS_DB = HERE / "corpus.db"
JSONL_DIR = HERE / "corpus_jsonl"
JSONL_DIR.mkdir(exist_ok=True)


# ---------- target schema (mirror of CORPUS_SCHEMA.md, SQLite-flavor) -----------

def init_corpus(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS corpus_document (
            doc_id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            source_native_id TEXT NOT NULL,
            tipo TEXT,
            jerarquia TEXT,
            titulo TEXT,
            rubro TEXT,
            clave TEXT,
            epoca TEXT,
            instancia TEXT,
            organo TEXT,
            materias_json TEXT,
            fecha_emision TEXT,
            fecha_publicacion TEXT,
            vigente INTEGER DEFAULT 1,
            status_detalle TEXT,
            autor TEXT,
            metadata_json TEXT,
            raw_text TEXT,
            source_url TEXT,
            fetched_at TEXT DEFAULT (datetime('now')),
            UNIQUE(source, source_native_id)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS corpus_chunk (
            chunk_id INTEGER PRIMARY KEY AUTOINCREMENT,
            doc_id INTEGER NOT NULL REFERENCES corpus_document(doc_id),
            chunk_index INTEGER NOT NULL,
            chunk_type TEXT NOT NULL,
            text TEXT NOT NULL,
            char_count INTEGER,
            embedding_model TEXT,
            UNIQUE(doc_id, chunk_index)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS corpus_citation (
            cite_id INTEGER PRIMARY KEY AUTOINCREMENT,
            citing_doc_id INTEGER REFERENCES corpus_document(doc_id),
            norm_type TEXT NOT NULL,
            norm_canon TEXT NOT NULL,
            norm_raw TEXT,
            cited_doc_id INTEGER REFERENCES corpus_document(doc_id),
            context_snippet TEXT
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_doc_source ON corpus_document(source)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_doc_fecha ON corpus_document(fecha_emision)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_doc_jer ON corpus_document(jerarquia)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_chunk_doc ON corpus_chunk(doc_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_chunk_type ON corpus_chunk(chunk_type)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cite_canon ON corpus_citation(norm_canon)")
    conn.commit()
    return conn


def insert_doc(conn: sqlite3.Connection, doc: dict) -> int | None:
    """Insert a document; returns the doc_id. Returns existing id on conflict."""
    cur = conn.execute("""
        INSERT INTO corpus_document(source, source_native_id, tipo, jerarquia, titulo,
                                    rubro, clave, epoca, instancia, organo, materias_json,
                                    fecha_emision, fecha_publicacion, vigente, status_detalle,
                                    autor, metadata_json, raw_text, source_url)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(source, source_native_id) DO UPDATE SET
            tipo=excluded.tipo, jerarquia=excluded.jerarquia, titulo=excluded.titulo,
            rubro=excluded.rubro, clave=excluded.clave, epoca=excluded.epoca,
            instancia=excluded.instancia, organo=excluded.organo,
            materias_json=excluded.materias_json,
            fecha_emision=excluded.fecha_emision, fecha_publicacion=excluded.fecha_publicacion,
            vigente=excluded.vigente, status_detalle=excluded.status_detalle,
            autor=excluded.autor, metadata_json=excluded.metadata_json,
            raw_text=excluded.raw_text, source_url=excluded.source_url
        RETURNING doc_id
    """, (
        doc.get("source"), doc.get("source_native_id"), doc.get("tipo"), doc.get("jerarquia"),
        doc.get("titulo"), doc.get("rubro"), doc.get("clave"), doc.get("epoca"),
        doc.get("instancia"), doc.get("organo"),
        json.dumps(doc.get("materias", []), ensure_ascii=False) if doc.get("materias") else None,
        doc.get("fecha_emision"), doc.get("fecha_publicacion"),
        1 if doc.get("vigente", True) else 0, doc.get("status_detalle"),
        doc.get("autor"),
        json.dumps(doc.get("metadata", {}), ensure_ascii=False) if doc.get("metadata") else None,
        doc.get("raw_text"), doc.get("source_url"),
    ))
    row = cur.fetchone()
    return row[0] if row else None


def insert_chunks(conn: sqlite3.Connection, doc_id: int, chunks: list[dict]):
    # Delete existing chunks (idempotent re-run)
    conn.execute("DELETE FROM corpus_chunk WHERE doc_id=?", (doc_id,))
    for i, ch in enumerate(chunks):
        text = (ch.get("text") or "").strip()
        if not text:
            continue
        conn.execute(
            "INSERT INTO corpus_chunk(doc_id, chunk_index, chunk_type, text, char_count) "
            "VALUES(?,?,?,?,?)",
            (doc_id, i, ch.get("chunk_type", "parrafo"), text, len(text))
        )


# ---------- source: scjn_tesis (modern, tesis.db) -------------------------------

def export_scjn_tesis(conn: sqlite3.Connection, src_db: str = "tesis.db"):
    src = sqlite3.connect(src_db)
    src.row_factory = sqlite3.Row
    n = src.execute("SELECT COUNT(*) FROM tesis WHERE detail_fetched_at IS NOT NULL").fetchone()[0]
    print(f"scjn_tesis: {n} rows")
    for i, r in enumerate(src.execute("""
        SELECT registro_digital, rubro, titulo, subtitulo, tipo, epoca_numero, epoca_nombre,
               clave, fuente, instancia, organo_jurisdiccional, fecha_publ_semanario,
               texto_contenido, texto_hechos, texto_justificacion, texto_criterios_juridicos,
               detail_json
        FROM tesis WHERE detail_fetched_at IS NOT NULL
    """)):
        doc = {
            "source": "scjn_tesis",
            "source_native_id": str(r["registro_digital"]),
            "tipo": "jurisprudencia" if (r["tipo"] or "").lower().startswith("tesis de juris") else "tesis_aislada",
            "jerarquia": "criterio_pjf",
            "titulo": r["titulo"] or r["rubro"],
            "rubro": r["rubro"],
            "clave": r["clave"],
            "epoca": r["epoca_nombre"] or r["epoca_numero"],
            "instancia": r["instancia"],
            "organo": r["organo_jurisdiccional"],
            "fecha_publicacion": r["fecha_publ_semanario"],
            "status_detalle": "vigente",
            "metadata": {"epoca_numero": r["epoca_numero"], "fuente": r["fuente"]},
            "raw_text": r["texto_contenido"],
            "source_url": f"https://bj.scjn.gob.mx/documento/sentencias_pub/{r['registro_digital']}",
        }
        doc_id = insert_doc(conn, doc)
        chunks = []
        if r["rubro"]:                    chunks.append({"chunk_type": "rubro", "text": r["rubro"]})
        if r["texto_hechos"]:             chunks.append({"chunk_type": "hechos", "text": r["texto_hechos"]})
        if r["texto_criterios_juridicos"]:chunks.append({"chunk_type": "criterio", "text": r["texto_criterios_juridicos"]})
        if r["texto_justificacion"]:      chunks.append({"chunk_type": "justificacion", "text": r["texto_justificacion"]})
        if not chunks and r["texto_contenido"]:
            chunks.append({"chunk_type": "cuerpo", "text": r["texto_contenido"]})
        insert_chunks(conn, doc_id, chunks)
        if (i + 1) % 5000 == 0:
            conn.commit()
            print(f"  {i+1}/{n}")
    conn.commit()


# ---------- source: scjn_historica (sjf.db) -------------------------------------

def export_scjn_historica(conn: sqlite3.Connection, src_db: str = "sjf.db"):
    src = sqlite3.connect(src_db)
    src.row_factory = sqlite3.Row
    n = src.execute("SELECT COUNT(*) FROM tesis").fetchone()[0]
    print(f"scjn_historica: {n} rows")
    for i, r in enumerate(src.execute("SELECT ius, rubro, localizacion, epoca, instancia, tipo_tesis, texto FROM tesis")):
        doc = {
            "source": "scjn_historica",
            "source_native_id": str(r["ius"]),
            "tipo": "tesis_historica",
            "jerarquia": "criterio_pjf",
            "titulo": r["rubro"],
            "rubro": r["rubro"],
            "epoca": r["epoca"],
            "instancia": r["instancia"],
            "status_detalle": "historica",
            "metadata": {"localizacion": r["localizacion"], "tipo": r["tipo_tesis"]},
            "raw_text": r["texto"],
        }
        doc_id = insert_doc(conn, doc)
        chunks = []
        if r["rubro"]:    chunks.append({"chunk_type": "rubro", "text": r["rubro"]})
        if r["texto"]:    chunks.append({"chunk_type": "cuerpo", "text": r["texto"]})
        insert_chunks(conn, doc_id, chunks)
        if (i + 1) % 2000 == 0:
            conn.commit()
            print(f"  {i+1}/{n}")
    conn.commit()


# ---------- source: tfja (tfja.db) ----------------------------------------------

def export_tfja(conn: sqlite3.Connection, src_db: str = "tfja.db"):
    src = sqlite3.connect(src_db)
    src.row_factory = sqlite3.Row
    n = src.execute("SELECT COUNT(*) FROM tfja_tesis").fetchone()[0]
    print(f"tfja: {n} rows")
    for i, r in enumerate(src.execute("""
        SELECT id_tfja, epoca, sala, clave, materia, rubro, texto, precedente_raw,
               ponente, revista_ref, anio_revista, status
        FROM tfja_tesis
    """)):
        doc = {
            "source": "tfja",
            "source_native_id": str(r["id_tfja"]),
            "tipo": "criterio_tfja",
            "jerarquia": "administrativo",
            "titulo": r["rubro"],
            "rubro": r["rubro"],
            "clave": r["clave"],
            "epoca": r["epoca"],
            "instancia": "TFJA",
            "organo": r["sala"],
            "materias": [r["materia"]] if r["materia"] else [],
            "fecha_publicacion": str(r["anio_revista"]) if r["anio_revista"] else None,
            "vigente": (r["status"] or "vigente") == "vigente",
            "status_detalle": r["status"],
            "autor": r["ponente"],
            "metadata": {"revista_ref": r["revista_ref"], "precedente_raw": r["precedente_raw"]},
            "raw_text": r["texto"],
            "source_url": f"https://www.tfja.gob.mx/cesmdfa/sctj/tesis-pdf-detalle/{r['id_tfja']}/",
        }
        doc_id = insert_doc(conn, doc)
        chunks = []
        if r["rubro"]:           chunks.append({"chunk_type": "rubro", "text": r["rubro"]})
        if r["texto"]:           chunks.append({"chunk_type": "cuerpo", "text": r["texto"]})
        if r["precedente_raw"]:  chunks.append({"chunk_type": "precedente", "text": r["precedente_raw"]})
        insert_chunks(conn, doc_id, chunks)
        if (i + 1) % 500 == 0:
            conn.commit()
    conn.commit()


# ---------- source: sre (sre.db) ------------------------------------------------

def export_sre(conn: sqlite3.Connection, src_db: str = "sre.db"):
    src = sqlite3.connect(src_db)
    src.row_factory = sqlite3.Row
    n = src.execute("SELECT COUNT(*) FROM sre_tratados").fetchone()[0]
    print(f"sre: {n} rows")
    for i, r in enumerate(src.execute("""
        SELECT token_sre, nombre, categoria, tema, estatus, lugar_adopcion,
               fecha_vigor_mexico, publ_dof_promulgacion, aprobacion_senado,
               tramite_constitucional, notas, tipo, reservas
        FROM sre_tratados WHERE detail_fetched_at IS NOT NULL
    """)):
        jerarquia = "convencional" if r["tipo"] == "ddhh" else "internacional"
        doc = {
            "source": "sre_tratado",
            "source_native_id": r["token_sre"],
            "tipo": "tratado",
            "jerarquia": jerarquia,
            "titulo": r["nombre"],
            "fecha_emision": r["lugar_adopcion"],
            "fecha_publicacion": r["publ_dof_promulgacion"],
            "vigente": (r["estatus"] or "").lower() != "abrogado",
            "status_detalle": r["estatus"],
            "materias": [r["tipo"] or "otro"],
            "metadata": {
                "categoria": r["categoria"], "tema": r["tema"],
                "fecha_vigor_mexico": r["fecha_vigor_mexico"],
                "aprobacion_senado": r["aprobacion_senado"],
                "tramite_constitucional": r["tramite_constitucional"],
                "reservas": r["reservas"],
            },
            "raw_text": r["notas"] or "",
            "source_url": f"https://cja.sre.gob.mx/tratadosmexico/tratados/{r['token_sre']}",
        }
        doc_id = insert_doc(conn, doc)
        chunks = []
        if r["nombre"]:   chunks.append({"chunk_type": "titulo", "text": r["nombre"]})
        if r["notas"]:    chunks.append({"chunk_type": "notas", "text": r["notas"]})
        if r["reservas"]: chunks.append({"chunk_type": "reserva", "text": r["reservas"]})
        insert_chunks(conn, doc_id, chunks)
    conn.commit()


# ---------- source: corteidh (corteidh.db) --------------------------------------

def export_corteidh(conn: sqlite3.Connection, src_db: str = "corteidh.db"):
    src = sqlite3.connect(src_db)
    src.row_factory = sqlite3.Row
    n = src.execute("SELECT COUNT(*) FROM idh_docs WHERE status='ok'").fetchone()[0]
    print(f"corteidh: {n} rows")
    for r in src.execute("""
        SELECT serie, numero, url, nombre, pais, fecha_sentencia, anio, tipo, raw_text
        FROM idh_docs WHERE status='ok'
    """):
        doc = {
            "source": "corte_idh",
            "source_native_id": f"{r['serie']}-{r['numero']}",
            "tipo": r["tipo"] or "sentencia_idh",
            "jerarquia": "convencional",
            "titulo": r["nombre"],
            "fecha_emision": r["fecha_sentencia"],
            "metadata": {"pais": r["pais"], "anio": r["anio"], "serie": r["serie"]},
            "raw_text": r["raw_text"],
            "source_url": r["url"],
        }
        doc_id = insert_doc(conn, doc)
        chunks = []
        if r["nombre"]:    chunks.append({"chunk_type": "titulo", "text": r["nombre"]})
        # Naive paragraph chunk of raw text (we'll improve with section-aware
        # parsing later).
        if r["raw_text"]:
            paragraphs = re.split(r"\n\s*\n", r["raw_text"])
            paragraphs = [p.strip() for p in paragraphs if len(p.strip()) > 80]
            for p in paragraphs[:200]:  # cap for now
                chunks.append({"chunk_type": "parrafo", "text": p})
        insert_chunks(conn, doc_id, chunks)
    conn.commit()


# ---------- source: dip_leyes (dip_leyes.db) ------------------------------------

def export_dip_leyes(conn: sqlite3.Connection, src_db: str = "dip_leyes.db"):
    src = sqlite3.connect(src_db)
    src.row_factory = sqlite3.Row
    n = src.execute("SELECT COUNT(*) FROM dip_leyes").fetchone()[0]
    print(f"dip_leyes: {n} rows")
    for r in src.execute("SELECT law_id, nombre, abreviatura, ultima_reforma, categoria, primary_url FROM dip_leyes"):
        # Gather parsed text from .htm archivos
        text_parts = []
        for at in src.execute(
            "SELECT parsed_text FROM dip_archivos WHERE law_id=? AND parsed_text IS NOT NULL AND ext='htm'",
            (r["law_id"],)
        ):
            if at[0]:
                text_parts.append(at[0])
        raw = "\n\n".join(text_parts)
        doc = {
            "source": "dip_ley",
            "source_native_id": str(r["law_id"]),
            "tipo": "ley_federal",
            "jerarquia": "federal",
            "titulo": r["nombre"],
            "fecha_publicacion": r["ultima_reforma"],
            "metadata": {"abreviatura": r["abreviatura"], "categoria": r["categoria"]},
            "raw_text": raw,
            "source_url": r["primary_url"],
        }
        doc_id = insert_doc(conn, doc)
        chunks = []
        if r["nombre"]: chunks.append({"chunk_type": "titulo", "text": r["nombre"]})
        # Split by Capítulo / Artículo markers when available
        if raw:
            # naive article split
            arts = re.split(r"\bArt[íi]culo\s+(\d+(?:\s*Bis(?:\s*\d+)?)?)\.", raw)
            # arts = [pre, num1, body1, num2, body2, ...]
            for i in range(1, len(arts), 2):
                num = arts[i].strip()
                body = arts[i + 1].strip() if i + 1 < len(arts) else ""
                if body and len(body) > 30:
                    chunks.append({"chunk_type": "articulo", "text": f"Artículo {num}. {body[:4000]}"})
            if not arts[1:]:
                # No article markers — store body as a single cuerpo chunk (or N split)
                chunks.append({"chunk_type": "cuerpo", "text": raw[:5000]})
        insert_chunks(conn, doc_id, chunks)
    conn.commit()


# ---------- main ---------------------------------------------------------------

EXPORTERS = {
    "scjn_tesis": export_scjn_tesis,
    "scjn_historica": export_scjn_historica,
    "tfja": export_tfja,
    "sre": export_sre,
    "corteidh": export_corteidh,
    "dip_leyes": export_dip_leyes,
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", choices=list(EXPORTERS) + ["all"], default="all")
    args = ap.parse_args()

    conn = init_corpus(CORPUS_DB)
    print(f"corpus DB: {CORPUS_DB}")
    sources = list(EXPORTERS) if args.source == "all" else [args.source]
    for s in sources:
        path = HERE / f"{s.replace('_', '_')}.db" if s != "scjn_tesis" else HERE / "tesis.db"
        if s == "scjn_historica":
            path = HERE / "sjf.db"
        elif s == "tfja":
            path = HERE / "tfja.db"
        elif s == "sre":
            path = HERE / "sre.db"
        elif s == "corteidh":
            path = HERE / "corteidh.db"
        elif s == "dip_leyes":
            path = HERE / "dip_leyes.db"
        if not path.exists():
            print(f"  skip {s} (no DB at {path})")
            continue
        EXPORTERS[s](conn, str(path))

    n_doc = conn.execute("SELECT COUNT(*) FROM corpus_document").fetchone()[0]
    n_ch = conn.execute("SELECT COUNT(*) FROM corpus_chunk").fetchone()[0]
    print(f"\nFinal corpus: {n_doc} documents, {n_ch} chunks")
    by_src = conn.execute("SELECT source, COUNT(*) FROM corpus_document GROUP BY source ORDER BY 2 DESC").fetchall()
    for s, k in by_src:
        print(f"  {s}: {k}")


if __name__ == "__main__":
    sys.exit(main())
