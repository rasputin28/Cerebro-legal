"""
TFJA crawler — Tribunal Federal de Justicia Administrativa.

Inputs validated:
  Base URL:  https://www.tfja.gob.mx/cesmdfa/sctj/tesis-pdf-detalle/{ID}/
  ID range:  1 to ~48,500 (sparse; many placeholders)
  Mime:      application/pdf direct
  Headers:   none required
  Placeholder PDF: 5,088 bytes, 1 page, contains only contact-info string
              → treated as "no tesis at this ID", skipped

Async download + parse with pdfplumber. Resume-safe (skips IDs already in DB).
Schema:
  tfja_tesis(id_tfja PK, epoca, sala, clave, materia, rubro, texto,
             precedente_raw, juicio_num, ponente, secretario, votacion,
             fecha_sesion, revista_ref, anio_revista, status DEFAULT 'vigente',
             pdf_bytes BLOB, raw_text TEXT, fetched_at)

Run:
  .venv/bin/python tfja_crawler.py --smoke   # ~200 IDs, sanity check
  .venv/bin/python tfja_crawler.py           # full crawl 1..48500
  .venv/bin/python tfja_crawler.py --start 30000 --end 35000
"""
from __future__ import annotations

import argparse
import asyncio
import io
import random
import re
import sqlite3
import sys
import time
from pathlib import Path

import httpx
import pdfplumber
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from rich.console import Console
from rich.progress import Progress, BarColumn, TextColumn, MofNCompleteColumn, TimeRemainingColumn

console = Console()

DB_PATH = Path(__file__).parent / "tfja.db"
BASE_URL = "https://www.tfja.gob.mx/cesmdfa/sctj/tesis-pdf-detalle/{}/"
DEFAULT_START = 1
DEFAULT_END = 48500
PLACEHOLDER_SIZE = 5088
PLACEHOLDER_MAX = 10000  # any PDF under this is suspicious
CONCURRENCY = 6
SLEEP_BASE = 0.15
SLEEP_JITTER = 0.10

HEADERS = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) Chrome/131.0 Safari/537.36"}


# ---------- DB ----------

def init_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tfja_tesis (
            id_tfja INTEGER PRIMARY KEY,
            epoca TEXT,
            sala TEXT,
            clave TEXT,
            materia TEXT,
            rubro TEXT,
            texto TEXT,
            precedente_raw TEXT,
            juicio_num TEXT,
            ponente TEXT,
            secretario TEXT,
            votacion TEXT,
            fecha_sesion TEXT,
            revista_ref TEXT,
            anio_revista INTEGER,
            status TEXT DEFAULT 'vigente',
            pdf_bytes BLOB,
            raw_text TEXT,
            byte_size INTEGER,
            fetched_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tfja_missing (
            id_tfja INTEGER PRIMARY KEY,
            reason TEXT,
            fetched_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tfja_clave ON tfja_tesis(clave)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tfja_materia ON tfja_tesis(materia)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tfja_status ON tfja_tesis(status)")
    conn.commit()
    return conn


# ---------- PDF parsing ----------

CLAVE_RE = re.compile(r"^Clave:\s*([^\n]+?)\s*$", re.M)
MATERIA_RE = re.compile(r"^Materia:\s*(.+?)(?=^Clave:|\Z)", re.M | re.S)
RUBRO_RE = re.compile(r"^Rubro:\s*(.+?)(?=^[a-záéíóúñ]|^\(|^R\.T\.F|\Z)", re.M | re.S)
# Broader procedural-identifier capture: Juicio Contencioso Administrativo, Revisión, Expediente, Recurso de Apelación, Queja, etc.
JUICIO_RE = re.compile(
    r"(?:Juicio\s+Contencioso\s+Administrativo|Juicio(?:\s+Atrayente)?|Revisi[oó]n|Expediente|Recurso\s+de\s+Apelaci[oó]n|Reclamaci[oó]n|Queja|Excitativa\s+de\s+Justicia|Incidente\s+de\s+\w+|Contradicci[oó]n\s+de\s+Sentencias?)\s+(?:Atrayente\s+)?(?:N[uú]m\.?|N[uú]mero|No\.)\s*([\w\-\/\.]+)",
    re.I,
)
PONENTE_RE = re.compile(r"Magistrad[oa]\s+Ponente:\s*([^\.]+?)(?=\.\-\s*Secretari|\.\s*Secretari|\-\s*Secretari|\.\s*\(|\Z)", re.I | re.S)
SECRETARIO_RE = re.compile(r"Secretari[oa](?:\s+de\s+Acuerdos)?:\s*(?:Lic\.\s*)?([^\.]+?)(?=\.\s*\(|\.\s*$|\.\n|\(Tesis|\Z)", re.I | re.S)
VOTACION_RE = re.compile(r"((?:por\s+)?(?:unanimidad|mayor[íi]a)\s+de\s+\d+\s+votos[^\.]*?)(?=\.|\-|\Z)", re.I)
FECHA_SESION_RE = re.compile(r"sesi[oó]n\s+de\s+(\d{1,2}\s+de\s+\w+\s+de\s+\d{4})", re.I)
RTFJA_RE = re.compile(r"R\.?T\.?F\.?(?:J\.?A\.?|F\.?)[^\n]*", re.I)
EPOCA_RE = re.compile(r"R\.?T\.?F\.?[JF]\.?A?\.?\s*([\wáéíóúÁÉÍÓÚÑñ]+)\s+[ÉE]poca", re.I)
ANIO_REVISTA_RE = re.compile(r"R\.?T\.?F\.?[JF]\.?A?\.?[^\n]*?(?:A[ñn]o\s+\w+\.?\s*)?(\d{4})", re.I)
PRECEDENTE_SECTION_RE = re.compile(r"(?:PRECEDENTE\(?S\)?|REITERACI[OÓ]N(?:\s+QUE\s+SE\s+PUBLICA)?)\s*:?\s*\n", re.I)
# Fallback: the body ends and procedural metadata begins at a line starting with the procedural identifier
PROC_START_RE = re.compile(
    r"^(?:Juicio\s+Contencioso|Revisi[oó]n\s+(?:No|N[uú]m)|Expediente\s+(?:N[uú]m|No)|Recurso\s+de\s+Apelaci[oó]n|Reclamaci[oó]n\s+(?:No|N[uú]m)|Queja\s+(?:No|N[uú]m)|Incidente\s+de\s+\w+)",
    re.M | re.I,
)


def parse_clave_to_sala_epoca(clave: str | None) -> tuple[str | None, str | None]:
    """E.g. VIII-P-1aS-660 -> (epoca='VIII', sala='1aS')."""
    if not clave:
        return None, None
    parts = clave.split("-")
    epoca = parts[0] if parts else None
    sala = parts[2] if len(parts) >= 3 else None
    return epoca, sala


def parse_pdf(pdf_bytes: bytes) -> dict:
    """Return parsed fields plus raw_text. Returns {} on failure."""
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            raw = "\n".join((p.extract_text() or "") for p in pdf.pages)
    except Exception as e:
        return {"_parse_error": str(e)}

    out: dict = {"raw_text": raw}

    def first(rx, src):
        m = rx.search(src)
        return m.group(1).strip() if m else None

    out["clave"] = first(CLAVE_RE, raw)
    # Materia: multi-line until "Clave:"
    mm = MATERIA_RE.search(raw)
    out["materia"] = " ".join(mm.group(1).split()).strip() if mm else None
    epoca, sala = parse_clave_to_sala_epoca(out["clave"])
    out["epoca"] = epoca
    out["sala"] = sala

    # Rubro: all consecutive all-caps lines after "Rubro:" until a normal-case line / "(" / R.T.F...
    rm = RUBRO_RE.search(raw)
    out["rubro"] = " ".join(rm.group(1).split()).strip() if rm else None

    # Body + precedente split: try PRECEDENTE marker first; if absent, split at the
    # procedural identifier line (Revisión No., Juicio Contencioso..., Expediente Núm.).
    body_end = None
    prec_start = None
    pm = PRECEDENTE_SECTION_RE.search(raw)
    if pm:
        body_end = pm.start()
        prec_start = pm.end()
    else:
        # No PRECEDENTES marker → split at first procedural line after rubro
        rstart = rm.end() if rm else 0
        proc = PROC_START_RE.search(raw, rstart)
        if proc:
            body_end = proc.start()
            prec_start = proc.start()  # procedural block IS the precedente raw

    if rm and body_end is not None:
        body_text = raw[rm.end():body_end].strip()
        out["texto"] = body_text or None
    else:
        out["texto"] = None

    if prec_start is not None:
        # Stop precedente at the R.T.F.J.A./R.T.F.F. line which marks revista metadata
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
    m = ANIO_REVISTA_RE.search(out["revista_ref"] or "")
    out["anio_revista"] = int(m.group(1)) if m else None

    return out


# ---------- HTTP ----------

async def jittered_sleep():
    await asyncio.sleep(SLEEP_BASE + random.uniform(0, SLEEP_JITTER))


@retry(
    reraise=True,
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    retry=retry_if_exception_type((httpx.HTTPError, httpx.RemoteProtocolError, httpx.ReadTimeout)),
)
async def fetch_pdf(client: httpx.AsyncClient, tid: int) -> bytes | None:
    r = await client.get(BASE_URL.format(tid))
    if r.status_code == 404:
        return None
    if r.status_code >= 500 or r.status_code == 429:
        raise httpx.HTTPError(f"server {r.status_code}")
    r.raise_for_status()
    return r.content


def upsert_tesis(conn: sqlite3.Connection, tid: int, parsed: dict, pdf_bytes: bytes) -> None:
    conn.execute("""
        INSERT INTO tfja_tesis(id_tfja, epoca, sala, clave, materia, rubro, texto,
                               precedente_raw, juicio_num, ponente, secretario, votacion,
                               fecha_sesion, revista_ref, anio_revista, status,
                               pdf_bytes, raw_text, byte_size)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(id_tfja) DO UPDATE SET
            epoca=excluded.epoca, sala=excluded.sala, clave=excluded.clave,
            materia=excluded.materia, rubro=excluded.rubro, texto=excluded.texto,
            precedente_raw=excluded.precedente_raw, juicio_num=excluded.juicio_num,
            ponente=excluded.ponente, secretario=excluded.secretario,
            votacion=excluded.votacion, fecha_sesion=excluded.fecha_sesion,
            revista_ref=excluded.revista_ref, anio_revista=excluded.anio_revista,
            pdf_bytes=excluded.pdf_bytes, raw_text=excluded.raw_text,
            byte_size=excluded.byte_size, fetched_at=datetime('now')
    """, (
        tid, parsed.get("epoca"), parsed.get("sala"), parsed.get("clave"),
        parsed.get("materia"), parsed.get("rubro"), parsed.get("texto"),
        parsed.get("precedente_raw"), parsed.get("juicio_num"),
        parsed.get("ponente"), parsed.get("secretario"), parsed.get("votacion"),
        parsed.get("fecha_sesion"), parsed.get("revista_ref"), parsed.get("anio_revista"),
        "vigente", pdf_bytes, parsed.get("raw_text"), len(pdf_bytes),
    ))


def mark_missing(conn: sqlite3.Connection, tid: int, reason: str) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO tfja_missing(id_tfja, reason, fetched_at) VALUES(?,?,datetime('now'))",
        (tid, reason),
    )


def already_seen(conn: sqlite3.Connection, tid: int) -> bool:
    a = conn.execute("SELECT 1 FROM tfja_tesis WHERE id_tfja=?", (tid,)).fetchone()
    b = conn.execute("SELECT 1 FROM tfja_missing WHERE id_tfja=?", (tid,)).fetchone()
    return bool(a or b)


# ---------- crawl ----------

async def crawl(start: int, end: int) -> None:
    conn = init_db(DB_PATH)
    sem = asyncio.Semaphore(CONCURRENCY)

    async with httpx.AsyncClient(http2=True, timeout=45, headers=HEADERS, follow_redirects=True) as client:
        with Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TextColumn("•"),
            TimeRemainingColumn(),
            console=console,
        ) as prog:
            task = prog.add_task("tfja IDs", total=end - start + 1)

            async def one(tid: int):
                async with sem:
                    if already_seen(conn, tid):
                        prog.advance(task)
                        return
                    try:
                        pdf = await fetch_pdf(client, tid)
                    except Exception as e:
                        mark_missing(conn, tid, f"http: {e}")
                        conn.commit()
                        prog.advance(task)
                        return
                    if pdf is None:
                        mark_missing(conn, tid, "404")
                        conn.commit()
                        prog.advance(task)
                        return
                    if len(pdf) < PLACEHOLDER_MAX and pdf[:5] == b"%PDF-":
                        mark_missing(conn, tid, f"placeholder ({len(pdf)}b)")
                        conn.commit()
                        prog.advance(task)
                        return
                    parsed = parse_pdf(pdf)
                    if "_parse_error" in parsed:
                        mark_missing(conn, tid, f"parse: {parsed['_parse_error']}")
                    else:
                        upsert_tesis(conn, tid, parsed, pdf)
                    conn.commit()
                    await jittered_sleep()
                    prog.advance(task)

            BATCH = 200
            for i in range(start, end + 1, BATCH):
                await asyncio.gather(*[one(t) for t in range(i, min(i + BATCH, end + 1))])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", type=int, default=DEFAULT_START)
    ap.add_argument("--end", type=int, default=DEFAULT_END)
    ap.add_argument("--smoke", action="store_true",
                    help="200 IDs across known-good range (1, 1000, 5000, 10000, 20000, 30000, 40000, 44948..45148)")
    args = ap.parse_args()

    t0 = time.time()

    if args.smoke:
        # Run a manual smoke set across the ID range.
        smoke_ids = list(range(1, 11)) + list(range(1000, 1010)) + list(range(5000, 5010)) + \
                    list(range(10000, 10010)) + list(range(20000, 20010)) + \
                    list(range(30000, 30010)) + list(range(40000, 40010)) + \
                    list(range(44948, 45148))
        conn = init_db(DB_PATH)
        loop = asyncio.new_event_loop()

        async def smoke():
            async with httpx.AsyncClient(http2=True, timeout=45, headers=HEADERS, follow_redirects=True) as client:
                sem = asyncio.Semaphore(CONCURRENCY)
                with Progress(
                    TextColumn("smoke"),
                    BarColumn(),
                    MofNCompleteColumn(),
                    console=console,
                ) as prog:
                    task = prog.add_task("smoke", total=len(smoke_ids))

                    async def one(tid: int):
                        async with sem:
                            if already_seen(conn, tid):
                                prog.advance(task); return
                            pdf = await fetch_pdf(client, tid)
                            if pdf is None:
                                mark_missing(conn, tid, "404")
                            elif len(pdf) < PLACEHOLDER_MAX:
                                mark_missing(conn, tid, f"placeholder ({len(pdf)}b)")
                            else:
                                parsed = parse_pdf(pdf)
                                if "_parse_error" in parsed:
                                    mark_missing(conn, tid, f"parse: {parsed['_parse_error']}")
                                else:
                                    upsert_tesis(conn, tid, parsed, pdf)
                            conn.commit()
                            await jittered_sleep()
                            prog.advance(task)

                    await asyncio.gather(*[one(t) for t in smoke_ids])

        loop.run_until_complete(smoke())
    else:
        loop = asyncio.new_event_loop()
        loop.run_until_complete(crawl(args.start, args.end))

    conn = sqlite3.connect(DB_PATH)
    n = conn.execute("SELECT COUNT(*) FROM tfja_tesis").fetchone()[0]
    m = conn.execute("SELECT COUNT(*) FROM tfja_missing").fetchone()[0]
    parse_fails = conn.execute("SELECT COUNT(*) FROM tfja_missing WHERE reason LIKE 'parse:%'").fetchone()[0]
    console.print(f"[green]tfja_tesis rows: {n}[/]")
    console.print(f"[yellow]missing/placeholder: {m} (parse failures: {parse_fails})[/]")
    console.print(f"[bold]Elapsed:[/] {time.time()-t0:.1f}s")


if __name__ == "__main__":
    sys.exit(main())
