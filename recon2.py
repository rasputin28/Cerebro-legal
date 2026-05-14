"""
Deeper recon: visit listing + detail SPA routes and click around to
surface every API endpoint.
"""
import asyncio
import json
import re
from pathlib import Path
from playwright.async_api import async_playwright

ROUTES = [
    "https://sjf2.scjn.gob.mx/jurisprudencia-historica",
    "https://sjf2.scjn.gob.mx/listado-tesis-historicas",
]
OUT = Path(__file__).parent / "api-map2.json"
INTERESTING = re.compile(r"/(api-resource|services|api|rest)/", re.I)


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
            try:
                text = await resp.text()
            except Exception:
                text = "<binary>"
            captured.append({
                "method": resp.request.method,
                "status": resp.status,
                "url": url,
                "req_body": resp.request.post_data,
                "resp_len": len(text),
                "resp_preview": text[:1500],
            })

        page.on("response", on_response)

        for route in ROUTES:
            print(f"\n--- visiting {route} ---")
            try:
                await page.goto(route, wait_until="networkidle", timeout=45_000)
            except Exception as e:
                print(f"  goto error: {e}")
            await page.wait_for_timeout(5_000)

            # Try clicking the first visible result if any.
            try:
                buttons = page.locator("a, button").filter(has_text=re.compile(r"buscar|consultar|ver", re.I))
                if await buttons.count() > 0:
                    print(f"  clicking first matching button ({await buttons.count()} found)")
                    await buttons.first.click(timeout=3000)
                    await page.wait_for_timeout(3000)
            except Exception as e:
                print(f"  click err: {e}")

            # Try filling search field.
            try:
                inp = page.locator('input').first
                if await inp.count():
                    await inp.fill("amparo")
                    await page.keyboard.press("Enter")
                    await page.wait_for_timeout(5000)
            except Exception as e:
                print(f"  search err: {e}")

        OUT.write_text(json.dumps({"captured": captured}, indent=2, ensure_ascii=False))

        seen = {}
        for c in captured:
            key = f"{c['method']} {c['url'].split('?')[0]}"
            seen[key] = seen.get(key, 0) + 1
        print(f"\n=== {len(captured)} requests ===")
        for k, n in sorted(seen.items(), key=lambda x: -x[1]):
            print(f"  {n:>3}  {k}")

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
