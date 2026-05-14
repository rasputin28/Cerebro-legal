"""TEPJF proper recon — drive the IUSE search form with Playwright."""
import asyncio, json, re
from pathlib import Path
from playwright.async_api import async_playwright

OUT = Path(__file__).parent / "api-map-tepjf2.json"

URLS_TO_TRY = [
    "https://www.te.gob.mx/IUSEapp/",
    "https://www.te.gob.mx/IUSEapp/tesis_jurisp.aspx",
    "https://www.te.gob.mx/IUSEapp/buscador.aspx",
    "https://portal.te.gob.mx/",
]


async def main():
    captured = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        ctx = await browser.new_context(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) Chrome/131.0",
            locale="es-MX",
            ignore_https_errors=True,
        )
        page = await ctx.new_page()

        async def on_response(resp):
            if "te.gob.mx" not in resp.url: return
            ct = resp.headers.get("content-type", "")
            if any(x in ct for x in ("image","font","css")): return
            try:
                body = (await resp.text())[:2000]
            except Exception:
                body = "<err>"
            captured.append({"method": resp.request.method, "status": resp.status, "url": resp.url, "ctype": ct, "req_body": resp.request.post_data, "resp_preview": body})

        page.on("response", on_response)

        for u in URLS_TO_TRY:
            print(f"\n--- {u} ---")
            try:
                await page.goto(u, wait_until="networkidle", timeout=45_000)
            except Exception as e:
                print(f"  goto: {e}")
                continue
            await page.wait_for_timeout(5000)
            html = await page.content()
            print(f"  page title: {await page.title()}")
            print(f"  size: {len(html):,}")
            # find form action
            for m in re.finditer(r'<form[^>]+action="([^"]+)"', html, re.I)[:5] if False else re.finditer(r'<form[^>]+action="([^"]+)"', html, re.I):
                print(f"  form action: {m.group(1)}")
            # find iframe URLs (IUSE often nested)
            for m in re.finditer(r'<iframe[^>]+src="([^"]+)"', html, re.I):
                print(f"  iframe src: {m.group(1)}")
            # find anchor patterns
            for m in re.finditer(r'href="([^"]*tesis[^"]*|[^"]*jurispr[^"]*)"', html, re.I):
                u2 = m.group(1)
                if "javascript" not in u2 and u2 != "#":
                    print(f"  link: {u2[:120]}")

            # try clicking buttons
            for txt in ["Búsqueda", "Buscar", "Consulta", "Iniciar"]:
                btn = page.get_by_text(txt, exact=False).first
                try:
                    if await btn.count() and await btn.is_visible():
                        href = await btn.evaluate("e => e.href || e.getAttribute('onclick')")
                        if href:
                            print(f"  button '{txt}' href/onclick: {href[:200]}")
                except Exception:
                    pass

        OUT.write_text(json.dumps({"captured": captured}, indent=2, ensure_ascii=False))
        unique = {}
        for c in captured:
            k = f"{c['method']} {c['url'].split('?')[0]}"
            unique[k] = unique.get(k, 0) + 1
        print(f"\n=== {len(captured)} reqs / {len(unique)} unique ===")
        for k, n in sorted(unique.items(), key=lambda x: -x[1])[:25]:
            print(f"  {n:>3}  {k}")

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
