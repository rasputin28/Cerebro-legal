"""TEPJF v5 — wait for __doPostBack hidration then submit with broad query."""
import asyncio, re, sqlite3
from pathlib import Path
from playwright.async_api import async_playwright

DB_PATH = Path(__file__).parent / "tepjf.db"


def init_db(path: Path):
    conn = sqlite3.connect(path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tepjf_tesis (
            id_iuse TEXT PRIMARY KEY,
            tipo TEXT, clave TEXT, rubro TEXT, texto TEXT, partes TEXT,
            instancia TEXT, fecha TEXT, anio INTEGER, url TEXT,
            raw_html TEXT, status TEXT,
            fetched_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.commit()
    return conn


TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")
def text_only(s): return WS_RE.sub(" ", TAG_RE.sub(" ", s)).strip()


async def main():
    conn = init_db(DB_PATH)
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-blink-features=AutomationControlled"])
        ctx = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Chrome/131.0 Safari/537.36",
            locale="es-MX", viewport={"width": 1440, "height": 900},
        )
        await ctx.add_init_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined});")
        page = await ctx.new_page()

        # Go DIRECTLY to BusquedaAvanzada and wait for hidration
        url = "https://www.te.gob.mx/IUSEapp/BusquedaAvanzada.aspx"
        print(f"loading {url}")
        await page.goto(url, wait_until="networkidle", timeout=60_000)
        # Wait for __doPostBack to actually become available
        try:
            await page.wait_for_function("typeof __doPostBack !== 'undefined'", timeout=20_000)
            print("  __doPostBack ready")
        except Exception as e:
            print(f"  __doPostBack timeout: {e}")
            # Continue anyway

        # Set form criteria
        async def safe_check(name):
            try:
                await page.locator(f'input[name="{name}"]').first.check(timeout=3000)
                return True
            except Exception:
                return False
        async def safe_fill(name, value):
            try:
                await page.fill(f'input[name="{name}"]', value, timeout=3000)
                return True
            except Exception:
                return False

        for cb in ["chkSelAll", "chkJuris", "chkTesis"]:
            ok = await safe_check(cb)
            print(f"  check {cb}: {ok}")
        ok = await safe_fill("txtBuscar", "amparo")
        print(f"  fill txtBuscar='amparo': {ok}")

        # Find search button — print all visible submit-ish controls first
        all_btns = await page.locator("input[type='submit'], input[type='button'], button").all()
        print(f"  {len(all_btns)} button-ish elements")
        candidates = []
        for el in all_btns:
            try:
                if await el.is_visible():
                    name = await el.get_attribute("name") or await el.get_attribute("id") or ""
                    val = await el.get_attribute("value") or (await el.inner_text())
                    candidates.append((name, val))
            except Exception:
                pass
        for n, v in candidates[:15]:
            print(f"    btn: name={n!r}  text/value={v[:40]!r}")

        # Click first button whose value/text contains Buscar
        clicked = False
        for el in all_btns:
            try:
                if await el.is_visible():
                    val = (await el.get_attribute("value")) or (await el.inner_text())
                    if val and re.search(r"buscar|consult", val, re.I):
                        name = await el.get_attribute("name") or ""
                        print(f"  clicking '{val}' (name={name})")
                        await el.click(timeout=5000)
                        clicked = True
                        break
            except Exception as e:
                continue
        if not clicked:
            print("  no Buscar button found, abort")
            await browser.close()
            return

        await page.wait_for_load_state("networkidle", timeout=30_000)
        await page.wait_for_timeout(3000)
        print(f"  after submit url: {page.url}")

        html = await page.content()
        Path("iuse_after_amparo.html").write_text(html[:300000])
        print(f"  size: {len(html):,}")

        # Look for result rows / links / postbacks
        result_links = re.findall(r'href="([^"]*(?:Detalle|TesisDetalle|MostrarDoc)[^"]*)"', html, re.I)
        postbacks = re.findall(r"__doPostBack\(['\"]([^'\"]+)['\"],['\"]([^'\"]*)['\"]\)", html)
        result_postbacks = [(e, a) for e, a in postbacks if any(x in e.lower() for x in ("tesis", "juris", "doc", "row", "grid"))]
        print(f"  result-style links: {len(set(result_links))}")
        print(f"  result-style postbacks: {len(result_postbacks)}")
        if result_postbacks:
            for e, a in result_postbacks[:10]:
                print(f"    pb: {e} | {a}")

        # Also look for tables of rows
        tables = re.findall(r"<table[^>]*id='([^']+)'", html)
        print(f"  tables: {tables[:5]}")
        # GridView results
        for tid in tables:
            if "Grid" in tid or "Resul" in tid or "Tesis" in tid:
                # extract rows
                m = re.search(rf"<table[^>]*id='{re.escape(tid)}'.*?</table>", html, re.S | re.I)
                if m:
                    tab_html = m.group(0)
                    rows = re.findall(r"<tr[^>]*>(.*?)</tr>", tab_html, re.S | re.I)
                    print(f"  table {tid}: {len(rows)} rows")
                    saved = 0
                    for i, row in enumerate(rows):
                        cells = [text_only(c) for c in re.findall(r"<td[^>]*>(.*?)</td>", row, re.S | re.I)]
                        cells = [c for c in cells if c and len(c) > 2]
                        if len(cells) < 2:
                            continue
                        clave = next((c for c in cells if re.match(r"^(Jurisprudencia|Tesis)\s+\d", c, re.I)), None)
                        rubro_cand = [c for c in cells if len(c) > 40]
                        rubro = max(rubro_cand, key=len) if rubro_cand else " | ".join(cells)
                        if rubro:
                            rid = clave or f"{tid}_row{i}"
                            conn.execute("""
                                INSERT OR REPLACE INTO tepjf_tesis(id_iuse, clave, rubro, raw_html, status, fetched_at)
                                VALUES(?,?,?,?,?,datetime('now'))
                            """, (rid, clave, rubro[:1500], row[:5000], "listed"))
                            saved += 1
                    conn.commit()
                    print(f"  saved from {tid}: {saved}")

        # Also try to find pagination
        next_links = re.findall(r"__doPostBack\(['\"]([^'\"]*[Pp]age[^'\"]*)['\"],['\"]([^'\"]*)['\"]\)", html)
        print(f"  pagination postbacks: {len(set(next_links))}")
        if next_links:
            for e, a in next_links[:5]:
                print(f"    {e} | {a}")

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
