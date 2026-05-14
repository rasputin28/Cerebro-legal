"""
Two-pronged recon for SCJN sentencias:

A) Drive Playwright on https://bj.scjn.gob.mx/  (the modern "Buscador Jurídico"
   that replaced the old SJF search) and https://buscador.scjn.gob.mx/sentencias
   and https://sjf2.scjn.gob.mx/listado-de-sentencias, capturing every XHR.

B) Probe the same historicalfile microservice with idApp variants and modern
   epoca IDs to see if tesis modernas (with urlSemanario+precedentes) live
   there too.

Output: api-map-sentencias.json
"""
import asyncio
import json
import re
from pathlib import Path
from playwright.async_api import async_playwright

OUT = Path(__file__).parent / "api-map-sentencias.json"
INTERESTING = re.compile(r"/(api-resource|services|api|rest|sentencias?|expediente)/", re.I)

ROUTES = [
    "https://bj.scjn.gob.mx/",
    "https://bj.scjn.gob.mx/buscador",
    "https://buscador.scjn.gob.mx/",
    "https://sjf2.scjn.gob.mx/listado-de-sentencias",
    "https://sjf2.scjn.gob.mx/listado-tesis",
]


async def main():
    captured = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-blink-features=AutomationControlled"],
        )
        ctx = await browser.new_context(
            user_agent=("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"),
            locale="es-MX",
            viewport={"width": 1366, "height": 900},
        )
        page = await ctx.new_page()

        async def on_response(resp):
            url = resp.url
            if not INTERESTING.search(url):
                return
            ct = resp.headers.get("content-type", "")
            preview = ""
            try:
                if "json" in ct or "text" in ct or "html" in ct:
                    preview = (await resp.text())[:1200]
                else:
                    preview = f"<{ct} len=?>"
            except Exception as e:
                preview = f"<err {e}>"
            captured.append({
                "method": resp.request.method,
                "status": resp.status,
                "url": url,
                "ctype": ct,
                "req_body": resp.request.post_data,
                "resp_preview": preview,
            })

        page.on("response", on_response)

        for route in ROUTES:
            print(f"\n--- {route} ---")
            try:
                await page.goto(route, wait_until="networkidle", timeout=45_000)
            except Exception as e:
                print(f"  goto err: {e}")
                continue
            await page.wait_for_timeout(4_000)

            # Try a search interaction.
            for sel in [
                'input[type="search"]',
                'input[placeholder*="busc" i]',
                'input[placeholder*="palabra" i]',
                'input.form-control',
            ]:
                loc = page.locator(sel).first
                try:
                    if await loc.count() and await loc.is_visible():
                        await loc.fill("amparo directo")
                        await page.keyboard.press("Enter")
                        await page.wait_for_timeout(5000)
                        # Try to click a result link to surface detail endpoint.
                        link = page.locator("a").filter(has_text=re.compile(r"^\s*\d", re.I)).first
                        if await link.count():
                            try:
                                await link.click(timeout=2500)
                                await page.wait_for_timeout(4000)
                            except Exception:
                                pass
                        break
                except Exception:
                    continue

        OUT.write_text(json.dumps({"captured": captured}, indent=2, ensure_ascii=False))

        seen = {}
        for c in captured:
            key = f"{c['method']} {c['url'].split('?')[0]}"
            seen[key] = seen.get(key, 0) + 1
        print(f"\n=== {len(captured)} requests, {len(seen)} unique endpoints ===")
        for k, n in sorted(seen.items(), key=lambda x: -x[1])[:30]:
            print(f"  {n:>3}  {k}")
        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
