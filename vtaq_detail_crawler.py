"""Pull full vtaquigraficas (court transcripts) detail JSON."""
import asyncio, json, random, sqlite3, sys
from pathlib import Path
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from rich.console import Console
from rich.progress import Progress, BarColumn, TextColumn, MofNCompleteColumn, TimeRemainingColumn

console = Console()
DB = Path(__file__).parent / "vtaquigraficas.db"
H = {"User-Agent": "Mozilla/5.0", "Accept": "application/json", "Referer": "https://bj.scjn.gob.mx/"}


def init():
    conn = sqlite3.connect(DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS vtaq_detail (
            idVT TEXT PRIMARY KEY,
            fechaSesion TEXT,
            organoJurisdiccional TEXT,
            asunto TEXT,
            texto TEXT,
            raw_json TEXT,
            byte_size INTEGER,
            status TEXT,
            fetched_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.commit()
    return conn


@retry(reraise=True, stop=stop_after_attempt(4),
       wait=wait_exponential(multiplier=1, min=2, max=20),
       retry=retry_if_exception_type((httpx.HTTPError, httpx.RemoteProtocolError)))
async def fetch(client, idvt):
    r = await client.get(f"https://bj.scjn.gob.mx/api/v1/bj/documento/vtaquigraficas/{idvt}")
    if r.status_code == 404: return None
    if r.status_code >= 500 or r.status_code == 429: raise httpx.HTTPError(f"{r.status_code}")
    r.raise_for_status()
    return r.json()


async def main():
    conn = init()
    ids = [r[0] for r in sqlite3.connect("vtaquigraficas.db").execute("SELECT id_native FROM vtaquigraficas")]
    existing = {r[0] for r in conn.execute("SELECT idVT FROM vtaq_detail")}
    todo = [i for i in ids if i not in existing]
    console.print(f"[bold]vtaq detail to fetch:[/] {len(todo)}")
    sem = asyncio.Semaphore(6)
    ok = 0
    async with httpx.AsyncClient(http2=True, headers=H, timeout=45) as client:
        with Progress(TextColumn("vtaq"), BarColumn(), MofNCompleteColumn(), TextColumn("•"), TimeRemainingColumn(), console=console) as prog:
            task = prog.add_task("vtaq", total=len(todo))
            async def one(idvt):
                nonlocal ok
                async with sem:
                    try:
                        j = await fetch(client, idvt)
                    except Exception as e:
                        conn.execute("INSERT OR IGNORE INTO vtaq_detail(idVT, status) VALUES(?,?)", (idvt, f"err: {e}"))
                        conn.commit(); prog.advance(task); return
                    if not j:
                        conn.execute("INSERT OR IGNORE INTO vtaq_detail(idVT, status) VALUES(?,?)", (idvt, "404"))
                        conn.commit(); prog.advance(task); return
                    texto = j.get("texto") or j.get("contenido") or ""
                    if isinstance(texto, dict):
                        texto = "\n\n".join(f"[{k}]\n{v}" for k, v in texto.items() if v)
                    raw = json.dumps(j, ensure_ascii=False)
                    conn.execute("""INSERT OR REPLACE INTO vtaq_detail(idVT, fechaSesion, organoJurisdiccional, asunto, texto, raw_json, byte_size, status, fetched_at)
                                    VALUES(?,?,?,?,?,?,?,?,datetime('now'))""",
                                 (idvt, j.get("fechaSesion"), j.get("organoJurisdiccional"),
                                  j.get("asunto") or j.get("tema"), texto, raw, len(raw), "ok"))
                    conn.commit()
                    ok += 1
                    await asyncio.sleep(0.15 + random.uniform(0, 0.1))
                    prog.advance(task)
            BATCH = 200
            for i in range(0, len(todo), BATCH):
                await asyncio.gather(*[one(r) for r in todo[i:i+BATCH]])
    n = conn.execute("SELECT COUNT(*) FROM vtaq_detail WHERE status='ok'").fetchone()[0]
    console.print(f"[green]vtaq_detail ok: {n}[/]")


if __name__ == "__main__":
    asyncio.run(main())
