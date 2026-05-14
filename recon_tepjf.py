"""
TEPJF recon — tesis y jurisprudencia electoral.

Sites of interest:
  - https://www.te.gob.mx/jurisprudencia/
  - https://www.te.gob.mx/IUSEapp/  (sistema histórico)
  - https://www.te.gob.mx/IUSEapp/tesisjur.aspx
  - portal.te.gob.mx/colecciones
"""
import asyncio, json, re
from pathlib import Path
from playwright.async_api import async_playwright

OUT = Path(__file__).parent / "api-map-tepjf.json"

ROUTES = [
    "https://www.te.gob.mx/jurisprudencia/",
    "https://www.te.gob.mx/IUSEapp/tesisjur.aspx",
    "https://www.te.gob.mx/IUSEapp/",
    "https://portal.te.gob.mx/colecciones",
]


async def main():
    captured = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        ctx = await browser.new_context(user_agent="Mozilla/5.0 Chrome/131.0", locale="es-MX")
        page = await ctx.new_page()

        async def on_response(resp):
            if not any(d in resp.url for d in ("te.gob.mx",)): return
            ct = resp.headers.get("content-type", "")
            if any(x in ct for x in ("image","font","css")): return
            try:
                body = (await resp.text())[:1200]
            except Exception:
                body = "<err>"
            captured.append({"method": resp.request.method, "status": resp.status, "url": resp.url, "ctype": ct, "resp_preview": body, "req_body": resp.request.post_data})

        page.on("response", on_response)

        for r in ROUTES:
            print(f"\n--- {r} ---")
            try:
                await page.goto(r, wait_until="networkidle", timeout=45_000)
            except Exception as e:
                print(f"  goto: {e}")
                continue
            await page.wait_for_timeout(4000)
            # find input fields
            inps = await page.locator("input, select, button").all()
            for el in inps[:25]:
                try:
                    tag = await el.evaluate("e => e.tagName")
                    name = await el.get_attribute("name") or await el.get_attribute("id")
                    typ = await el.get_attribute("type")
                    vis = await el.is_visible()
                    if vis and name:
                        print(f"  {tag} name={name} type={typ}")
                except Exception:
                    pass
            # try to find a result list pattern
            html = await page.content()
            links = re.findall(r'href="([^"]*tesis[^"]*|[^"]*jurispr[^"]*|[^"]*colec[^"]*)"', html, re.I)
            uniq = sorted(set(links))[:10]
            for l in uniq:
                print(f"  link: {l[:120]}")

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
