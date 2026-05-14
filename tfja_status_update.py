"""
Download JUR_SUSP_MOD.pdf from TFJA, parse to extract (clave, status) pairs,
and update tfja_tesis.status accordingly.

JUR_SUSP_MOD.pdf lists tesis that have been:
  - SUSPENDIDAS
  - MODIFICADAS
  - DEROGADAS / DEJADAS SIN EFECTOS

Format inferred from inspection — robust regex catches common rows.
"""
from __future__ import annotations

import io
import re
import sqlite3
from pathlib import Path

import httpx
import pdfplumber

DB_PATH = Path(__file__).parent / "tfja.db"
SUSP_URL = "https://www.tfja.gob.mx/media/media/pdf/cesmdfa/scjl/JUR_SUSP_MOD.pdf"

CLAVE_LINE_RE = re.compile(
    r"\b([IVX]+\-(?:P|J|TS|TASS|TASR|TA|CASR|CASS|CASE|TJ|JL|JM|RM)\-[\w\-]+?)\b",
    re.I,
)
STATUS_HEADERS = [
    ("DEROGAD", "derogada"),
    ("SUSPEND", "suspendida"),
    ("DEJAR\\s+SIN\\s+EFECTO", "sin_efectos"),
    ("MODIFICAD", "modificada"),
    ("INTERRUMP", "interrumpida"),
]


def fetch_pdf() -> bytes:
    with httpx.Client(http2=True, follow_redirects=True, timeout=120) as c:
        r = c.get(SUSP_URL, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        return r.content


def parse_status_map(pdf_bytes: bytes) -> dict[str, str]:
    """Returns {clave: status}. Walks the doc, tracking the most recent section
    heading (DEROGADAS / SUSPENDIDAS / MODIFICADAS) and tagging each clave to
    the current section."""
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        full_text = "\n".join((p.extract_text() or "") for p in pdf.pages)

    out: dict[str, str] = {}
    current_status: str | None = None
    for line in full_text.split("\n"):
        line_upper = line.upper()
        # Detect section headers
        for pat, status in STATUS_HEADERS:
            if re.search(pat, line_upper):
                current_status = status
                break
        # Catch claves on the line
        for m in CLAVE_LINE_RE.finditer(line):
            clave = m.group(1).upper()
            if current_status:
                # Prefer the most severe status if same clave appears under multiple sections
                # (priority: derogada > sin_efectos > suspendida > modificada > interrumpida)
                priority = {"derogada": 5, "sin_efectos": 4, "suspendida": 3,
                            "modificada": 2, "interrumpida": 1}
                if clave not in out or priority.get(current_status, 0) > priority.get(out[clave], 0):
                    out[clave] = current_status
    return out


def apply_statuses(conn: sqlite3.Connection, status_map: dict[str, str]) -> tuple[int, int]:
    """Update tfja_tesis.status by clave match. Returns (matched, total_in_map)."""
    matched = 0
    for clave, status in status_map.items():
        cur = conn.execute(
            "UPDATE tfja_tesis SET status=? WHERE upper(clave)=?",
            (status, clave),
        )
        if cur.rowcount:
            matched += cur.rowcount
    conn.commit()
    return matched, len(status_map)


def main():
    print(f"Downloading {SUSP_URL}")
    pdf = fetch_pdf()
    print(f"  PDF size: {len(pdf):,}")
    status_map = parse_status_map(pdf)
    print(f"  Parsed {len(status_map)} clave→status pairs")

    if not status_map:
        print("  (parser returned nothing — inspect PDF structure)")
        return

    # Distribution
    from collections import Counter
    c = Counter(status_map.values())
    for s, n in c.most_common():
        print(f"    {s}: {n}")

    conn = sqlite3.connect(DB_PATH)
    matched, total = apply_statuses(conn, status_map)
    print(f"\n  Matched against DB: {matched} / {total}")

    # Stats per status in DB
    rows = conn.execute("SELECT status, COUNT(*) FROM tfja_tesis GROUP BY status ORDER BY 2 DESC").fetchall()
    print("\n  status distribution in tfja_tesis:")
    for s, n in rows:
        print(f"    {s}: {n}")


if __name__ == "__main__":
    main()
