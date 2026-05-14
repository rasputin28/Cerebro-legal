"""
SCJN votos crawler — opiniones disidentes/particulares de los Ministros.

Pulls /api/v1/bj/documento/votos/{rd} for each unique rd_voto from
tesis_voto_ref. Votos are formal individual opinions attached to a sentencia.
"""
from __future__ import annotations
import argparse, asyncio, json, random, sqlite3, sys
from pathlib import Path
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from rich.console import Console
from rich.progress import Progress, BarColumn, TextColumn, MofNCompleteColumn, TimeRemainingColumn

console = Console()
DB_PATH = Path(__file__).parent / "votos.db"
TESIS_DB = Path(__file__).parent / "tesis.db"
BJ = "https://bj.scjn.gob.mx/api/v1/bj"
HEADERS = {"User-Agent": "Mozilla/5.0 Chrome/131.0", "Accept": "application/json", "Referer": "https://bj.scjn.gob.mx/"}
SEM = 6
SLEEP = 0.15


def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS votos (
            registro_digital INTEGER PRIMARY KEY,
            tipo_voto TEXT,
            ministro TEXT,
            num_expediente TEXT,
            organo TEXT,
            fecha TEXT,
            texto TEXT,
            raw_json TEXT,
            status TEXT,
            fetched_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.commit()
    return conn


def load_ids():
    c = sqlite3.connect(TESIS_DB)
    return [r[0] for r in c.execute("SELECT DISTINCT rd_voto FROM tesis_voto_ref ORDER BY rd_voto")]


@retry(reraise=True, stop=stop_after_attempt(4),
       wait=wait_exponential(multiplier=1, min=2, max=20),
       retry=retry_if_exception_type((httpx.HTTPError, httpx.RemoteProtocolError, httpx.ReadTimeout)))
async def fetch(client, rd):
    r = await client.get(f"{BJ}/documento/votos/{rd}")
    if r.status_code == 404: return None
    if r.status_code >= 500 or r.status_code == 429:
        raise httpx.HTTPError(f"{r.status_code}")
    r.raise_for_status()
    return r.json()


async def main():
    ids = load_ids()
    conn = init_db()
    existing = {r[0] for r in conn.execute("SELECT registro_digital FROM votos")}
    todo = [i for i in ids if i not in existing]
    console.print(f"[bold]votos to fetch:[/] {len(todo)} (of {len(ids)} unique IDs)")
    sem = asyncio.Semaphore(SEM)
    ok = 0
    async with httpx.AsyncClient(http2=True, headers=HEADERS, timeout=45) as client:
        with Progress(TextColumn("votos"), BarColumn(), MofNCompleteColumn(), TextColumn("•"), TimeRemainingColumn(), console=console) as prog:
            task = prog.add_task("votos", total=len(todo))

            async def one(rd):
                nonlocal ok
                async with sem:
                    try:
                        j = await fetch(client, rd)
                    except Exception as e:
                        conn.execute("INSERT OR IGNORE INTO votos(registro_digital, status) VALUES(?,?)", (rd, f"err: {e}"))
                        conn.commit(); prog.advance(task); return
                    if not j:
                        conn.execute("INSERT OR IGNORE INTO votos(registro_digital, status) VALUES(?,?)", (rd, "404"))
                        conn.commit(); prog.advance(task); return
                    texto = j.get("texto")
                    if isinstance(texto, dict):
                        texto_str = "\n\n".join(f"[{k}]\n{v}" for k, v in texto.items() if v)
                    else:
                        texto_str = texto or ""
                    conn.execute("""
                        INSERT OR REPLACE INTO votos(registro_digital, tipo_voto, ministro, num_expediente,
                                                    organo, fecha, texto, raw_json, status, fetched_at)
                        VALUES(?,?,?,?,?,?,?,?,?,datetime('now'))
                    """, (rd, j.get("tipoVoto") or j.get("tipo"),
                          j.get("ministro") or j.get("emisor") or j.get("ponente"),
                          j.get("numExpediente") or j.get("numeroExpediente"),
                          j.get("organoJurisdiccional") or j.get("organo"),
                          j.get("fechaResolucion") or j.get("fechaPublicacionSemanario"),
                          texto_str, json.dumps(j, ensure_ascii=False), "ok"))
                    conn.commit()
                    ok += 1
                    await asyncio.sleep(SLEEP + random.uniform(0, 0.1))
                    prog.advance(task)

            BATCH = 100
            for i in range(0, len(todo), BATCH):
                await asyncio.gather(*[one(r) for r in todo[i:i+BATCH]])

    n = conn.execute("SELECT COUNT(*) FROM votos WHERE status='ok'").fetchone()[0]
    console.print(f"[green]votos ok: {n}[/]")


if __name__ == "__main__":
    asyncio.run(main())
