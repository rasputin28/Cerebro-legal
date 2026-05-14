"""
SCJN Jurisprudencia Histórica scraper.

Endpoint: POST /services/sjftesismicroservice/api/public/historicalfile
No auth, no browser required.

Server enforces Elasticsearch's max_result_window=10000 (page*size capped at 10k).
Workaround: query each idEpoca slice separately — each slice is <10k records.

Behavior:
- Iterates over epoch slices (1, 2, 3, 4) and paginates each.
- Resume-safe: skips (slice, page) already written.
- Survives transient errors with tenacity; logs and continues on hard failures.
- SQLite-backed; primary key is `ius` so re-runs deduplicate.

Run:  .venv/bin/python scraper.py            # full crawl
      .venv/bin/python scraper.py --epoch 1  # one slice only
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from rich.console import Console
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn, MofNCompleteColumn

console = Console()

URL = "https://sjf2.scjn.gob.mx/services/sjftesismicroservice/api/public/historicalfile"
DB_PATH = Path(__file__).parent / "sjf.db"
PAGE_SIZE = 50
RATE_SLEEP = 0.4
EPOCHS = ["1", "2", "3", "4"]
INSTANCIAS = ["0", "1", "2", "3", "7"]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "es-MX,es;q=0.9,en;q=0.8",
    "Origin": "https://sjf2.scjn.gob.mx",
    "Referer": "https://sjf2.scjn.gob.mx/listado-tesis-historicas",
    "Content-Type": "application/json",
}


def body_for(epoch: str | None, instancia: str | None = None) -> dict[str, Any]:
    return {
        "classifiers": [
            {"name": "idEpoca", "value": [epoch] if epoch else EPOCHS,
             "allSelected": epoch is None, "visible": False, "isMatrix": True},
            {"name": "idInstancia", "value": [instancia] if instancia else INSTANCIAS,
             "allSelected": instancia is None, "visible": False, "isMatrix": True},
        ],
        "searchTerms": [],
        "bFacet": True,
        "ius": [],
        "idApp": "SJFAPP2020",
        "lbSearch": ["Todo"],
        "filterExpression": "",
    }


def init_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tesis (
            ius INTEGER PRIMARY KEY,
            rubro TEXT,
            localizacion TEXT,
            epoca TEXT,
            instancia TEXT,
            tipo_tesis TEXT,
            clave_tesis TEXT,
            fecha_publicacion TEXT,
            materias TEXT,
            texto TEXT,
            precedentes TEXT,
            raw_json TEXT NOT NULL,
            fetched_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pages (
            slice TEXT NOT NULL,
            page INTEGER NOT NULL,
            size INTEGER NOT NULL,
            count INTEGER NOT NULL,
            total_at_fetch INTEGER NOT NULL,
            fetched_at TEXT DEFAULT (datetime('now')),
            PRIMARY KEY(slice, page)
        )
    """)
    conn.commit()
    return conn


@retry(
    reraise=True,
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    retry=retry_if_exception_type((httpx.HTTPError, httpx.RemoteProtocolError)),
)
def fetch(client: httpx.Client, body: dict, page: int, size: int) -> dict:
    r = client.post(URL, params={"page": page, "size": size}, json=body)
    if r.status_code >= 500 or r.status_code == 429:
        raise httpx.HTTPError(f"server {r.status_code}")
    r.raise_for_status()
    return r.json()


def materias_to_str(m: Any) -> str:
    if not m:
        return ""
    if isinstance(m, list):
        return ", ".join(x.get("nombre", "") if isinstance(x, dict) else str(x) for x in m)
    return str(m)


def upsert(conn: sqlite3.Connection, doc: dict) -> None:
    conn.execute(
        """INSERT INTO tesis(ius,rubro,localizacion,epoca,instancia,tipo_tesis,clave_tesis,
                             fecha_publicacion,materias,texto,precedentes,raw_json)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
           ON CONFLICT(ius) DO UPDATE SET
             rubro=excluded.rubro, localizacion=excluded.localizacion,
             epoca=excluded.epoca, instancia=excluded.instancia,
             tipo_tesis=excluded.tipo_tesis, clave_tesis=excluded.clave_tesis,
             fecha_publicacion=excluded.fecha_publicacion, materias=excluded.materias,
             texto=excluded.texto, precedentes=excluded.precedentes,
             raw_json=excluded.raw_json, fetched_at=datetime('now')""",
        (doc.get("ius"), doc.get("rubro"), doc.get("localizacion"), doc.get("epoca"),
         doc.get("instancia"), doc.get("tipoTesis"), doc.get("claveTesis"),
         doc.get("fechaPublicacion"), materias_to_str(doc.get("materias")),
         doc.get("texto"), doc.get("precedentes"),
         json.dumps(doc, ensure_ascii=False)),
    )


def already_complete(conn: sqlite3.Connection, slice_key: str, page: int, size: int) -> bool:
    row = conn.execute("SELECT count, size FROM pages WHERE slice=? AND page=?",
                       (slice_key, page)).fetchone()
    return bool(row and row[1] == size and row[0] > 0)


def crawl_slice(client: httpx.Client, conn: sqlite3.Connection,
                slice_key: str, body: dict, size: int, sleep: float,
                progress: Progress) -> tuple[int, int]:
    """Returns (records_seen, hard_failures)."""
    first = fetch(client, body, 0, size)
    total = first.get("total", 0)
    if total == 0:
        return 0, 0

    capped = min(total, 9999)  # Hard ES limit; never request offset >= 10000.
    pages = (capped + size - 1) // size

    task = progress.add_task(f"{slice_key} ({total} docs)", total=pages)
    seen = 0
    fails = 0

    for page in range(pages):
        if already_complete(conn, slice_key, page, size):
            progress.advance(task)
            continue
        try:
            j = first if page == 0 else fetch(client, body, page, size)
        except Exception as e:
            console.print(f"[red]  {slice_key} page {page} FAILED after retries: {e}[/red]")
            fails += 1
            progress.advance(task)
            continue

        docs = j.get("documents") or []
        for d in docs:
            if d.get("ius") is None:
                continue
            upsert(conn, d)
            seen += 1
        conn.execute(
            "INSERT OR REPLACE INTO pages(slice,page,size,count,total_at_fetch) VALUES(?,?,?,?,?)",
            (slice_key, page, size, len(docs), j.get("total", total)),
        )
        conn.commit()
        progress.advance(task)
        time.sleep(sleep)

    return seen, fails


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--size", type=int, default=PAGE_SIZE)
    ap.add_argument("--sleep", type=float, default=RATE_SLEEP)
    ap.add_argument("--epoch", choices=EPOCHS, help="only one epoch slice")
    args = ap.parse_args()

    conn = init_db(DB_PATH)
    epochs = [args.epoch] if args.epoch else EPOCHS

    with httpx.Client(http2=True, timeout=60, headers=HEADERS) as client:
        # Show grand total first.
        probe = fetch(client, body_for(None), 0, 1)
        grand_total = probe.get("total", 0)
        console.print(f"[bold]Grand total in corpus:[/] {grand_total}")
        console.print(f"[bold]Slicing by epoch:[/] {', '.join(epochs)}")

        total_seen = 0
        total_fails = 0
        with Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TextColumn("•"),
            TimeRemainingColumn(),
            console=console,
        ) as prog:
            for ep in epochs:
                seen, fails = crawl_slice(
                    client, conn, f"epoch={ep}", body_for(ep), args.size, args.sleep, prog
                )
                total_seen += seen
                total_fails += fails

    db_rows = conn.execute("SELECT COUNT(*) FROM tesis").fetchone()[0]
    console.print()
    console.print(f"[green]Records upserted this run:[/] {total_seen}")
    console.print(f"[green]Total unique tesis in DB:[/]  {db_rows}")
    if total_fails:
        console.print(f"[yellow]Pages that hard-failed: {total_fails}[/]")
    return 0


if __name__ == "__main__":
    sys.exit(main())
