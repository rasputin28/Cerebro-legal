"""
DOF (Diario Oficial de la Federación) recon — dof.gob.mx.

Goals:
- Find URL pattern for editions (by date) and individual publications.
- Identify how to filter or search for "Decreto Promulgatorio" type treaty publications.
- Capture the JSON or HTML search responses.
"""
import asyncio, json, re
from pathlib import Path
from playwright.async_api import async_playwright

OUT = Path(__file__).parent / "api-map-dof.json"

ROUTES = [
    "https://www.dof.gob.mx/",
    "https://www.dof.gob.mx/index_111.php?year=2024&month=01&day=10",
    "https://dof.gob.mx/busqueda_avanzada.php",
    "https://www.dof.gob.mx/index.php",
]


async def main():
    captured = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        ctx = await browser.new_context(user_agent="Mozilla/5.0 Chrome/131.0", locale="es-MX")
        page = await ctx.new_page()

        async def on_response(resp):
            url = resp.url
            if "dof" not in url:
                return
            ct = resp.headers.get("content-type", "")
            if any(x in ct for x in ("image", "font", "css")): return
            try:
                body = (await resp.text())[:1500]
            except Exception:
                body = "<err>"
            captured.append({"method": resp.request.method, "status": resp.status, "url": url, "ctype": ct, "resp_preview": body})

        page.on("response", on_response)

        for r in ROUTES:
            print(f"--- {r} ---")
            try:
                await page.goto(r, wait_until="networkidle", timeout=30_000)
            except Exception as e:
                print(f"  goto: {e}")
                continue
            await page.wait_for_timeout(3000)
            html = await page.content()
            # forms
            for m in re.finditer(r'<form[^>]*action="([^"]+)"', html, re.I):
                print(f"  form action: {m.group(1)}")
            # interesting links
            for m in re.finditer(r'href="([^"]*nota[^"]*|[^"]*ejemplar[^"]*|[^"]*tratado[^"]*)"', html, re.I):
                u = m.group(1)
                if u not in (None, "#"):
                    print(f"  link: {u[:120]}")

        OUT.write_text(json.dumps({"captured": captured}, indent=2, ensure_ascii=False))
        unique = {}
        for c in captured:
            k = f"{c['method']} {c['url'].split('?')[0]}"
            unique[k] = unique.get(k, 0) + 1
        print(f"\n=== {len(captured)} reqs / {len(unique)} unique ===")
        for k, n in sorted(unique.items(), key=lambda x: -x[1])[:20]:
            print(f"  {n:>3}  {k}")
        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
