"""TEPJF v3 — Playwright stealth approach to bypass Radware Bot Manager."""
import asyncio, re, sqlite3, time
from pathlib import Path
from playwright.async_api import async_playwright

DB_PATH = Path(__file__).parent / "tepjf.db"


async def main():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tepjf_pdf_files (
            url TEXT PRIMARY KEY, nombre TEXT, byte_size INTEGER,
            pdf_bytes BLOB, fetched_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.commit()

    async with async_playwright() as p:
        # Stealth-like config
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--disable-features=IsolateOrigins,site-per-process",
                "--disable-web-security",
            ],
        )
        ctx = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            locale="es-MX",
            viewport={"width": 1440, "height": 900},
            timezone_id="America/Mexico_City",
            geolocation={"latitude": 19.4326, "longitude": -99.1332},
            permissions=["geolocation"],
        )
        # Anti-detection script
        await ctx.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            Object.defineProperty(navigator, 'languages', { get: () => ['es-MX', 'es', 'en-US', 'en'] });
            Object.defineProperty(navigator, 'plugins', { get: () => [1,2,3,4,5] });
        """)
        page = await ctx.new_page()

        # Try the IUSE app which worked previously (not behind Radware)
        print("Loading IUSE app (not Radware-protected)...")
        await page.goto("https://www.te.gob.mx/IUSEapp/", wait_until="domcontentloaded", timeout=60_000)
        await page.wait_for_timeout(10_000)  # wait long for any client-side hydration

        # Discover navigation: TEPJF IUSE has buttons that navigate to result pages
        html = await page.content()
        Path("iuse_home.html").write_text(html)
        print(f"IUSE home: {len(html):,} chars")

        # Look for ANY links in the page
        anchors = re.findall(r'<a[^>]+href="([^"]+)"[^>]*>(.{1,200}?)</a>', html, re.S | re.I)
        print(f"Total anchors: {len(anchors)}")
        # Find anything tesis/jurisprudencia/repositorio-related
        relevant = []
        for h, t in anchors:
            text = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", t)).strip()
            if any(kw in h.lower() for kw in ("tesis", "jurisp", "repos", "compi", "doc")) or any(kw in text.lower() for kw in ("tesis", "jurisp", "compi")):
                relevant.append((h, text[:80]))
        print(f"Relevant: {len(relevant)}")
        for h, t in relevant[:20]:
            print(f"  {h:60s} :: {t}")

        # Try to click 'Búsqueda avanzada' link
        for txt in ["Búsqueda avanzada", "Búsqueda", "Buscar", "Tesis y Jurisprudencia"]:
            try:
                link = page.get_by_text(txt, exact=False).first
                if await link.count():
                    print(f"\nclicking {txt}...")
                    await link.click(timeout=5000)
                    await page.wait_for_timeout(8000)
                    new_html = await page.content()
                    print(f"  after click: {len(new_html):,} chars, url: {page.url}")
                    Path(f"iuse_after_{txt.replace(' ','_')}.html").write_text(new_html[:80000])
                    # Find new links and inputs
                    inps = await page.locator("input, select, button").all()
                    print(f"  inputs visible: {len(inps)}")
                    for el in inps[:10]:
                        try:
                            if await el.is_visible():
                                tag = await el.evaluate("e => e.tagName")
                                name = await el.get_attribute("name") or await el.get_attribute("id")
                                val = await el.get_attribute("value") or ""
                                print(f"    {tag} name={name!r} value={val[:30]!r}")
                        except Exception:
                            pass
                    break
            except Exception as e:
                print(f"  {txt} fail: {e}")

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
