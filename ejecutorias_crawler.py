"""
SCJN ejecutorias crawler (bj.scjn.gob.mx `ejecutorias` index).

Each tesis points to rdEjecutoria — the formal sentence that originated it.
We have 17,463 unique ejecutoria IDs from the tesis crawl (tesis_ejecutoria_ref).
Pull each via /documento/ejecutorias/{rd}.

Schema (ejecutorias.db):
  ejecutorias(registro_digital PK, num_expediente, asunto, ponente,
              fecha_resolucion, organo, tipo, texto, raw_json, status, fetched_at)
"""
from __future__ import annotations

import argparse, asyncio, json, random, sqlite3, sys, time
from pathlib import Path

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from rich.console import Console
from rich.progress import Progress, BarColumn, TextColumn, MofNCompleteColumn, TimeRemainingColumn

console = Console()
DB_PATH = Path(__file__).parent / "ejecutorias.db"
TESIS_DB = Path(__file__).parent / "tesis.db"
BJ = "https://bj.scjn.gob.mx/api/v1/bj"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) Chrome/131.0",
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://bj.scjn.gob.mx",
    "Referer": "https://bj.scjn.gob.mx/",
    "Content-Type": "application/json",
}
CONCURRENCY = 6
SLEEP_BASE = 0.15
SLEEP_JITTER = 0.10


def init_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ejecutorias (
            registro_digital INTEGER PRIMARY KEY,
            num_expediente TEXT,
            asunto TEXT,
            ponente TEXT,
            secretario TEXT,
            fecha_resolucion TEXT,
            organo_jurisdiccional TEXT,
            tipo_asunto TEXT,
            votacion TEXT,
            texto TEXT,
            raw_json TEXT,
            status TEXT,
            fetched_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ejec_exp ON ejecutorias(num_expediente)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ejec_organo ON ejecutorias(organo_jurisdiccional)")
    conn.commit()
    return conn


def load_ejecutoria_ids() -> list[int]:
    """Read all unique rd_ejecutoria from tesis.db."""
    conn = sqlite3.connect(TESIS_DB)
    rows = conn.execute("SELECT DISTINCT rd_ejecutoria FROM tesis_ejecutoria_ref ORDER BY rd_ejecutoria").fetchall()
    return [r[0] for r in rows]


async def jittered_sleep():
    await asyncio.sleep(SLEEP_BASE + random.uniform(0, SLEEP_JITTER))


@retry(reraise=True, stop=stop_after_attempt(5),
       wait=wait_exponential(multiplier=1, min=2, max=30),
       retry=retry_if_exception_type((httpx.HTTPError, httpx.RemoteProtocolError, httpx.ReadTimeout)))
async def fetch_doc(client: httpx.AsyncClient, rd: int) -> dict | None:
    """Get the full ejecutoria document by registroDigital."""
    r = await client.get(f"{BJ}/documento/ejecutorias/{rd}")
    if r.status_code == 404:
        return None
    if r.status_code >= 500 or r.status_code == 429:
        raise httpx.HTTPError(f"server {r.status_code}")
    r.raise_for_status()
    return r.json()


async def crawl():
    ids = load_ejecutoria_ids()
    console.print(f"[bold]ejecutoria IDs from tesis_ejecutoria_ref:[/] {len(ids)}")

    conn = init_db(DB_PATH)
    # Filter already-fetched
    existing = {r[0] for r in conn.execute("SELECT registro_digital FROM ejecutorias")}
    todo = [i for i in ids if i not in existing]
    console.print(f"[bold]pending:[/] {len(todo)}")

    sem = asyncio.Semaphore(CONCURRENCY)
    ok = 0
    async with httpx.AsyncClient(http2=True, headers=HEADERS, timeout=60) as client:
        with Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TextColumn("•"),
            TimeRemainingColumn(),
            console=console,
        ) as prog:
            task = prog.add_task("ejecutorias", total=len(todo))

            async def one(rd: int):
                nonlocal ok
                async with sem:
                    try:
                        j = await fetch_doc(client, rd)
                    except Exception as e:
                        conn.execute("INSERT OR IGNORE INTO ejecutorias(registro_digital, status) VALUES(?,?)",
                                     (rd, f"err: {e}"))
                        conn.commit()
                        prog.advance(task); return
                    if not j:
                        conn.execute("INSERT OR IGNORE INTO ejecutorias(registro_digital, status) VALUES(?,?)",
                                     (rd, "404"))
                        conn.commit()
                        prog.advance(task); return
                    texto = j.get("texto")
                    if isinstance(texto, dict):
                        texto_str = "\n\n".join(f"[{k}]\n{v}" for k, v in texto.items() if v)
                    else:
                        texto_str = texto or ""
                    conn.execute("""
                        INSERT OR REPLACE INTO ejecutorias(registro_digital, num_expediente, asunto,
                                                          ponente, secretario, fecha_resolucion,
                                                          organo_jurisdiccional, tipo_asunto, votacion,
                                                          texto, raw_json, status, fetched_at)
                        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,datetime('now'))
                    """, (
                        rd, j.get("numExpediente"), j.get("asunto"),
                        j.get("ponente"), j.get("secretario"), j.get("fechaResolucion"),
                        j.get("organoJurisdiccional") or j.get("organo"),
                        j.get("tipoAsunto"), j.get("votacion"),
                        texto_str, json.dumps(j, ensure_ascii=False), "ok"
                    ))
                    conn.commit()
                    ok += 1
                    await jittered_sleep()
                    prog.advance(task)

            BATCH = 200
            for i in range(0, len(todo), BATCH):
                await asyncio.gather(*[one(r) for r in todo[i:i+BATCH]])

    n_ok = conn.execute("SELECT COUNT(*) FROM ejecutorias WHERE status='ok'").fetchone()[0]
    console.print(f"[green]ejecutorias ok: {n_ok}[/]")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--smoke", action="store_true")
    args = ap.parse_args()
    asyncio.run(crawl())


if __name__ == "__main__":
    sys.exit(main())
