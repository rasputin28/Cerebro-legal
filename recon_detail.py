"""
Open bj.scjn.gob.mx, perform a search, click into a result, click any
download/view button, and capture every URL hit — especially binary
content-types (docx/pdf).
"""
import asyncio
import json
import re
from pathlib import Path
from playwright.async_api import async_playwright

OUT = Path(__file__).parent / "api-map-detail.json"


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
            accept_downloads=True,
        )
        page = await ctx.new_page()

        async def on_response(resp):
            url = resp.url
            ct = resp.headers.get("content-type", "")
            # capture everything except images/css/fonts/sourcemap
            skip = ("image/", "font/", "text/css", ".map")
            if any(x in ct for x in skip) or url.endswith(".map"):
                return
            # focus on backend traffic
            if "scjn.gob.mx" not in url and "bj." not in url:
                return
            try:
                body = (await resp.text())[:600] if ("json" in ct or "html" in ct or "text" in ct) else f"<{ct}>"
            except Exception:
                body = f"<{ct} unreadable>"
            captured.append({
                "method": resp.request.method,
                "status": resp.status,
                "url": url,
                "ctype": ct,
                "headers": {k: v for k, v in resp.headers.items()
                            if k.lower() in ("content-disposition", "location", "content-length", "x-cdn")},
                "req_body": resp.request.post_data,
                "resp_preview": body,
            })

        page.on("response", on_response)

        # Listen for downloads (the .docx click triggers a download stream).
        downloads = []
        page.on("download", lambda d: downloads.append({"url": d.url, "suggested": d.suggested_filename}))

        print("--- loading bj.scjn ---")
        await page.goto("https://bj.scjn.gob.mx/", wait_until="networkidle", timeout=60_000)
        await page.wait_for_timeout(5_000)

        print("--- typing search ---")
        # The home page has a big search box.
        inputs = page.locator("input").all()
        for inp in await inputs:
            try:
                if await inp.is_visible(timeout=500):
                    await inp.fill("amparo directo 2024")
                    await page.keyboard.press("Enter")
                    print("  search submitted")
                    break
            except Exception:
                continue
        await page.wait_for_timeout(7000)

        # Try restricting to Sentencias tab.
        print("--- click Sentencias tab ---")
        for txt in ["Sentencias", "Sentencia"]:
            tab = page.get_by_text(txt, exact=False).first
            try:
                if await tab.count() and await tab.is_visible():
                    await tab.click(timeout=3000)
                    print(f"  clicked {txt}")
                    await page.wait_for_timeout(4000)
                    break
            except Exception:
                continue

        # Click the first result.
        print("--- click first result ---")
        result_links = [
            'a:has-text("AMPARO")',
            'a.titulo, a.title',
            'a[href*="/detalle"]',
            'a[href*="/sentencia"]',
            'a.btn',
            'a',
        ]
        clicked = False
        for sel in result_links:
            try:
                loc = page.locator(sel).first
                if await loc.count() and await loc.is_visible():
                    href = await loc.get_attribute("href")
                    print(f"  clicking {sel} href={href}")
                    await loc.click(timeout=3000)
                    await page.wait_for_timeout(6000)
                    clicked = True
                    break
            except Exception:
                continue
        print(f"  clicked={clicked}")
        print(f"  current url: {page.url}")

        # Now try to find a download/view button.
        print("--- look for download button ---")
        for txt in ["Descargar", "Engrose", "PDF", "Word", "Ver documento", "Documento"]:
            btn = page.get_by_text(txt, exact=False).first
            try:
                if await btn.count() and await btn.is_visible():
                    print(f"  clicking {txt}")
                    async with page.expect_download(timeout=15_000) as d_info:
                        try:
                            await btn.click(timeout=3000)
                        except Exception:
                            pass
                    d = await d_info.value
                    print(f"  DOWNLOAD: url={d.url} name={d.suggested_filename}")
                    downloads.append({"url": d.url, "suggested": d.suggested_filename})
                    break
            except Exception:
                # Not a download — maybe nav. Just click.
                try:
                    if await btn.count() and await btn.is_visible():
                        await btn.click(timeout=3000)
                        await page.wait_for_timeout(3000)
                except Exception:
                    pass

        await page.wait_for_timeout(3000)

        OUT.write_text(json.dumps({"captured": captured, "downloads": downloads, "final_url": page.url}, indent=2, ensure_ascii=False))

        # Summary.
        binary_hits = [c for c in captured if c["ctype"] and "html" not in c["ctype"] and "json" not in c["ctype"] and "text/plain" not in c["ctype"]]
        print(f"\nTotal captured: {len(captured)}")
        print(f"Binary-ish responses: {len(binary_hits)}")
        for c in binary_hits[:20]:
            print(f"  {c['status']} {c['method']} {c['ctype']} <- {c['url']}")
        print(f"\nDownloads observed: {len(downloads)}")
        for d in downloads:
            print(f"  {d}")

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
