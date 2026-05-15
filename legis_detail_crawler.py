"""
Pull full text of priority legislation from bj.scjn /documento/legislacion/{id}.

Targets (configurable):
  - All FEDERAL vigentes (~7k) — incluye reglamentos federales
  - All TRATADOS vigentes (~1.3k)
  - All Códigos + Constituciones + Leyes Orgánicas vigentes (fed+estatal) (~4k)

Schema: stores articulos JSON + raw + url.
"""
from __future__ import annotations
import argparse, asyncio, json, random, sqlite3, sys
from pathlib import Path
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from rich.console import Console
from rich.progress import Progress, BarColumn, TextColumn, MofNCompleteColumn, TimeRemainingColumn

console = Console()
DB_PATH = Path(__file__).parent / "legislacion_detail.db"
SRC_DB = Path(__file__).parent / "legislacion.db"
BJ = "https://bj.scjn.gob.mx/api/v1/bj"
HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json", "Referer": "https://bj.scjn.gob.mx/"}
CONCURRENCY = 6
SLEEP = 0.18


def init_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS legis_detail (
            id_native TEXT PRIMARY KEY,
            ordenamiento TEXT,
            ambito TEXT,
            estado TEXT,
            municipio TEXT,
            poder TEXT,
            organo TEXT,
            categoria TEXT,
            materia TEXT,
            vigencia TEXT,
            fecha_publicado TEXT,
            fecha_ultima_actualizacion TEXT,
            articulos_json TEXT,
            articulos_count INTEGER,
            char_count INTEGER,
            raw_json TEXT,
            status TEXT,
            fetched_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ld_ambito ON legis_detail(ambito)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ld_estado ON legis_detail(estado)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ld_cat ON legis_detail(categoria)")
    conn.commit()
    return conn


def get_target_ids(scope: str) -> list[str]:
    src = sqlite3.connect(SRC_DB)
    if scope == "priority":
        # Códigos, Constituciones, Leyes Orgánicas (fed+estatal vigentes)
        rows = src.execute("""
            SELECT id_native FROM legislacion
            WHERE vigencia='VIGENTE'
              AND ambito IN ('FEDERAL', 'ESTATAL')
              AND (ordenamiento LIKE 'CODIGO%' OR ordenamiento LIKE 'CONSTITUCION%'
                   OR ordenamiento LIKE 'LEY ORG%')
            ORDER BY id_native
        """).fetchall()
    elif scope == "federal":
        rows = src.execute("SELECT id_native FROM legislacion WHERE ambito='FEDERAL' AND vigencia='VIGENTE'").fetchall()
    elif scope == "tratados":
        rows = src.execute("SELECT id_native FROM legislacion WHERE ambito='TRATADOS INTERNACIONALES' AND vigencia='VIGENTE'").fetchall()
    elif scope == "estatales_codigos":
        rows = src.execute("""
            SELECT id_native FROM legislacion
            WHERE ambito='ESTATAL' AND vigencia='VIGENTE'
              AND (ordenamiento LIKE 'CODIGO%' OR ordenamiento LIKE 'CONSTITUCION%')
        """).fetchall()
    else:
        raise ValueError(f"unknown scope {scope}")
    return [r[0] for r in rows]


async def jittered_sleep():
    await asyncio.sleep(SLEEP + random.uniform(0, 0.1))


@retry(reraise=True, stop=stop_after_attempt(5),
       wait=wait_exponential(multiplier=1, min=2, max=30),
       retry=retry_if_exception_type((httpx.HTTPError, httpx.RemoteProtocolError, httpx.ReadTimeout)))
async def fetch(client, lid):
    r = await client.get(f"{BJ}/documento/legislacion/{lid}")
    if r.status_code == 404: return None
    if r.status_code >= 500 or r.status_code == 429:
        raise httpx.HTTPError(f"{r.status_code}")
    r.raise_for_status()
    return r.json()


async def crawl(target_ids: list[str]):
    conn = init_db(DB_PATH)
    existing = {r[0] for r in conn.execute("SELECT id_native FROM legis_detail")}
    todo = [i for i in target_ids if i not in existing]
    console.print(f"[bold]Target:[/] {len(target_ids)} ({len(todo)} pending, {len(target_ids)-len(todo)} already done)")
    if not todo: return
    sem = asyncio.Semaphore(CONCURRENCY)
    async with httpx.AsyncClient(http2=True, headers=HEADERS, timeout=60) as client:
        with Progress(TextColumn("legis detail"), BarColumn(), MofNCompleteColumn(),
                      TextColumn("•"), TimeRemainingColumn(), console=console) as prog:
            task = prog.add_task("detail", total=len(todo))

            async def one(lid):
                async with sem:
                    try:
                        j = await fetch(client, lid)
                    except Exception as e:
                        conn.execute("INSERT OR IGNORE INTO legis_detail(id_native, status) VALUES(?,?)", (lid, f"err: {e}"))
                        conn.commit(); prog.advance(task); return
                    if not j:
                        conn.execute("INSERT OR IGNORE INTO legis_detail(id_native, status) VALUES(?,?)", (lid, "404"))
                        conn.commit(); prog.advance(task); return
                    articulos = j.get("articulos") or []
                    articulos_json = json.dumps(articulos, ensure_ascii=False)
                    char_count = sum(len(a.get("texto","") if isinstance(a, dict) else str(a)) for a in articulos)
                    raw_json = json.dumps(j, ensure_ascii=False)
                    materia = j.get("materia") or []
                    if isinstance(materia, list): materia_str = ", ".join(materia)
                    else: materia_str = str(materia)
                    conn.execute("""
                        INSERT OR REPLACE INTO legis_detail(id_native, ordenamiento, ambito, estado, municipio,
                                                            poder, organo, categoria, materia, vigencia,
                                                            fecha_publicado, fecha_ultima_actualizacion,
                                                            articulos_json, articulos_count, char_count,
                                                            raw_json, status, fetched_at)
                        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,datetime('now'))
                    """, (
                        lid, j.get("ordenamiento"), j.get("ambito"), j.get("estado"),
                        j.get("municipio"), j.get("poder"), j.get("organo"),
                        j.get("categoriaOrdenamiento"), materia_str, j.get("vigencia"),
                        j.get("fechaPublicado"), j.get("fechaUltimaActualizacion"),
                        articulos_json, len(articulos), char_count,
                        raw_json, "ok",
                    ))
                    conn.commit()
                    await jittered_sleep()
                    prog.advance(task)

            BATCH = 200
            for i in range(0, len(todo), BATCH):
                await asyncio.gather(*[one(r) for r in todo[i:i+BATCH]])

    n = conn.execute("SELECT COUNT(*) FROM legis_detail WHERE status='ok'").fetchone()[0]
    console.print(f"[green]Total ok: {n}[/]")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--scope", choices=["priority","federal","tratados","estatales_codigos","all"], default="priority")
    args = ap.parse_args()
    if args.scope == "all":
        ids = list(set(get_target_ids("priority") + get_target_ids("federal") + get_target_ids("tratados")))
    else:
        ids = get_target_ids(args.scope)
    asyncio.run(crawl(ids))


if __name__ == "__main__":
    sys.exit(main())
