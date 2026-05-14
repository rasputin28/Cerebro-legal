"""TEPJF v4 — submit BusquedaAvanzada.aspx with chkSelAll, walk results."""
import asyncio, re, sqlite3, time
from pathlib import Path
from playwright.async_api import async_playwright

DB_PATH = Path(__file__).parent / "tepjf.db"
START_URL = "https://www.te.gob.mx/IUSEapp/"


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


def text_only(s):
    return WS_RE.sub(" ", TAG_RE.sub(" ", s)).strip()


async def main():
    conn = init_db(DB_PATH)
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-blink-features=AutomationControlled"])
        ctx = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Chrome/131.0 Safari/537.36",
            locale="es-MX",
            viewport={"width": 1440, "height": 900},
        )
        await ctx.add_init_script("Object.defineProperty(navigator, 'webdriver', { get: () => undefined });")
        page = await ctx.new_page()

        print("→ IUSEapp home")
        await page.goto(START_URL, wait_until="domcontentloaded", timeout=60_000)
        await page.wait_for_timeout(4000)

        print("→ Búsqueda avanzada")
        try:
            await page.get_by_text("Búsqueda avanzada", exact=False).first.click(timeout=5000)
        except Exception:
            await page.evaluate("__doPostBack('lnkBusquedaAvanzada','')")
        await page.wait_for_timeout(5000)
        print(f"  url: {page.url}")

        # Check the chkSelAll (select all materias / types)
        try:
            sel_all = page.locator('input[name="chkSelAll"]').first
            if await sel_all.count():
                if not await sel_all.is_checked():
                    await sel_all.check(timeout=3000)
                print("  chkSelAll: checked")
        except Exception as e:
            print(f"  chkSelAll err: {e}")
        try:
            await page.locator('input[name="chkJuris"]').first.check(timeout=3000)
            await page.locator('input[name="chkTesis"]').first.check(timeout=3000)
            print("  chkJuris + chkTesis checked")
        except Exception as e:
            print(f"  juris/tesis check err: {e}")

        # Fill a broad query term — IUSE often refuses empty searches
        try:
            await page.fill('input[name="txtBuscar"]', "amparo", timeout=3000)
            print("  txtBuscar: 'amparo'")
        except Exception as e:
            print(f"  txtBuscar err: {e}")

        # Click the search button (try several common names)
        clicked = False
        for sel in [
            "input[type='submit'][value*='uscar' i]",
            "input[type='submit'][value*='onsult' i]",
            "input[name*='btnBuscar' i]",
            "#btnBuscar",
            "input[name*='Buscar' i]",
        ]:
            try:
                b = page.locator(sel).first
                if await b.count() and await b.is_visible():
                    print(f"  submit via {sel}: {(await b.get_attribute('value')) or ''}")
                    await b.click(timeout=5000)
                    clicked = True
                    break
            except Exception:
                pass
        if not clicked:
            # Postback fallback
            for fn in ["btnBuscar", "btnConsultar", "btnConsulta", "btnBuscarTesis"]:
                try:
                    await page.evaluate(f"__doPostBack({fn!r},'')")
                    clicked = True
                    print(f"  postback({fn!r})")
                    break
                except Exception:
                    pass
        await page.wait_for_timeout(8000)
        Path("iuse_after_search.html").write_text((await page.content())[:200000])
        print(f"  after search url: {page.url}")

        # Find result links
        html = await page.content()
        # Likely patterns: TesisDetalle.aspx?Id=N or postback link buttons
        result_anchors = re.findall(r'href="((?:TesisDetalle|Detalle|Mostrar)[^"]*)"', html, re.I)
        result_postbacks = re.findall(r"__doPostBack\(['\"]([^'\"]+)['\"],['\"]([^'\"]*)['\"]\)", html)
        print(f"\nresults page: {len(set(result_anchors))} hrefs, {len(set(result_postbacks))} postbacks")

        # Save the page HTML for inspection
        Path("iuse_results.html").write_text(html[:300000])

        # Show a sample of postback patterns to understand the navigation
        seen = {}
        for evt, arg in result_postbacks:
            key = evt.rsplit("$", 1)[0]
            seen.setdefault(key, []).append((evt, arg))
        for k, items in list(seen.items())[:10]:
            print(f"  postback group {k}: {len(items)} items, sample: {items[0]}")

        # If we got result rows directly, extract them
        # Pattern: each row has tesis metadata in adjacent <td> cells
        rows = re.findall(r"<tr[^>]*class=['\"]?(?:dgRow|GridRowStyle|GridAlternatingRowStyle)['\"][^>]*>(.*?)</tr>", html, re.S | re.I)
        print(f"  data rows by class: {len(rows)}")
        if not rows:
            # try generic tr inside any <table>
            rows = re.findall(r"<tr[^>]*>\s*<td[^>]*>.*?</td>\s*<td[^>]*>.*?</td>.*?</tr>", html, re.S | re.I)
            print(f"  generic data rows: {len(rows)}")

        # Parse rows
        saved = 0
        for i, row in enumerate(rows[:100]):  # cap for testing
            cells = [text_only(c) for c in re.findall(r"<td[^>]*>(.*?)</td>", row, re.S | re.I)]
            cells = [c for c in cells if c]
            if len(cells) < 2:
                continue
            # Heuristic field assignment
            clave = next((c for c in cells if re.match(r"^(Jurisprudencia|Tesis)\s+\d", c, re.I)), None)
            rubro = max(cells, key=len)
            if rubro and len(rubro) > 20:
                rid = clave or f"row_{i}"
                conn.execute("""
                    INSERT OR REPLACE INTO tepjf_tesis(id_iuse, clave, rubro, raw_html, status, fetched_at)
                    VALUES(?,?,?,?,?,datetime('now'))
                """, (rid, clave, rubro[:1000], row[:5000], "listed"))
                saved += 1
        conn.commit()
        print(f"\nsaved (listing): {saved}")

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
