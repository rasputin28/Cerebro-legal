"""
Reverse-engineers the SCJN jurisprudencia-historica API by driving the SPA
in a headless browser, logging every XHR/fetch, and dumping the captured
URLs + request bodies + response samples to api-map.json.

Run: .venv/bin/python recon.py
"""
import asyncio
import json
import re
from pathlib import Path
from playwright.async_api import async_playwright

START_URL = "https://sjf2.scjn.gob.mx/jurisprudencia-historica"
OUT = Path(__file__).parent / "api-map.json"

INTERESTING = re.compile(r"/(api-resource|api|services|rest)/", re.I)


async def main():
    captured = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-blink-features=AutomationControlled"],
        )
        ctx = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
            ),
            locale="es-MX",
            viewport={"width": 1366, "height": 900},
        )
        page = await ctx.new_page()

        async def on_response(resp):
            url = resp.url
            if not INTERESTING.search(url):
                return
            try:
                body_preview = (await resp.text())[:800]
            except Exception:
                body_preview = "<binary>"
            captured.append({
                "method": resp.request.method,
                "status": resp.status,
                "url": url,
                "req_headers": {k: v for k, v in resp.request.headers.items()
                                if k.lower() in ("authorization", "content-type", "accept", "x-requested-with")},
                "req_body": resp.request.post_data,
                "resp_preview": body_preview,
            })

        page.on("response", on_response)

        print(f"Loading {START_URL} ...")
        await page.goto(START_URL, wait_until="networkidle", timeout=60_000)
        await page.wait_for_timeout(4_000)

        # Try a couple of generic interactions to surface search endpoints.
        # The SPA labels its main search field generically — try several selectors.
        for sel in [
            'input[type="search"]',
            'input[placeholder*="usqueda" i]',
            'input[placeholder*="buscar" i]',
            'input[aria-label*="busca" i]',
        ]:
            loc = page.locator(sel).first
            try:
                if await loc.count() > 0:
                    await loc.fill("amparo")
                    await loc.press("Enter")
                    await page.wait_for_timeout(5_000)
                    break
            except Exception as e:
                print(f"  selector {sel} failed: {e}")

        # Final settle.
        await page.wait_for_timeout(3_000)

        # Also grab final cookies & a sample localStorage token if any.
        cookies = await ctx.cookies()
        token_probe = await page.evaluate("""() => {
            const out = {};
            for (let i = 0; i < localStorage.length; i++) {
                const k = localStorage.key(i);
                out[k] = (localStorage.getItem(k) || '').slice(0, 200);
            }
            return out;
        }""")

        OUT.write_text(json.dumps({
            "captured": captured,
            "cookies": cookies,
            "localStorage": token_probe,
        }, indent=2, ensure_ascii=False))

        # Print a quick summary.
        seen = {}
        for c in captured:
            key = f"{c['method']} {c['url'].split('?')[0]}"
            seen[key] = seen.get(key, 0) + 1
        print(f"\nCaptured {len(captured)} interesting requests:")
        for k, n in sorted(seen.items(), key=lambda x: -x[1]):
            print(f"  {n:>3}  {k}")
        print(f"\nFull dump: {OUT}")

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
