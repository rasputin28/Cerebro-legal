"""CNDH Angular SPA recon — capture API calls."""
import asyncio, json, re
from pathlib import Path
from playwright.async_api import async_playwright

OUT = Path(__file__).parent / "api-map-cndh.json"

ROUTES = [
    "https://www.cndh.org.mx/documento/recomendaciones",
    "https://www.cndh.org.mx/tipo/1/recomendacion",
    "https://www.cndh.org.mx/inicio",
]


async def main():
    captured = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        ctx = await browser.new_context(user_agent="Mozilla/5.0 Chrome/131.0", locale="es-MX")
        page = await ctx.new_page()

        async def on_response(resp):
            if "cndh" not in resp.url:
                return
            ct = resp.headers.get("content-type", "")
            if any(x in ct for x in ("image","font","css")): return
            try:
                body = (await resp.text())[:1500]
            except Exception:
                body = "<err>"
            captured.append({"method": resp.request.method, "status": resp.status, "url": resp.url, "ctype": ct, "resp_preview": body})

        page.on("response", on_response)

        for r in ROUTES:
            print(f"--- {r} ---")
            try:
                await page.goto(r, wait_until="networkidle", timeout=45_000)
            except Exception as e:
                print(f"  goto: {e}")
                continue
            await page.wait_for_timeout(6000)

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
