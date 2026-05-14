"""
SRE Tratados — Playwright recon of cja.sre.gob.mx/tratadosmexico/

Goals:
  1. Map the buscador: search form fields, AJAX endpoints, pagination.
  2. Identify how to enumerate ALL tratados (likely: empty search → all pages).
  3. Capture detail-page DOM shape (which fields are present, CSS selectors).
  4. Confirm Laravel token format.

Output: api-map-sre.json
"""
import asyncio
import json
import re
from pathlib import Path
from playwright.async_api import async_playwright

OUT = Path(__file__).parent / "api-map-sre.json"


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
            if "sre.gob.mx" not in url:
                return
            ct = resp.headers.get("content-type", "")
            try:
                if "json" in ct or "html" in ct or "text" in ct:
                    body = (await resp.text())[:1500]
                else:
                    body = f"<{ct}>"
            except Exception:
                body = "<read err>"
            captured.append({
                "method": resp.request.method,
                "status": resp.status,
                "url": url,
                "ctype": ct,
                "req_body": resp.request.post_data,
                "resp_preview": body,
            })

        page.on("response", on_response)

        print("=== load home ===")
        await page.goto("https://cja.sre.gob.mx/tratadosmexico/", wait_until="networkidle", timeout=60_000)
        await page.wait_for_timeout(4000)

        # Snapshot the home page
        home_html = await page.content()
        Path("sre_home.html").write_text(home_html[:50000])

        # Look for the search form / buscador
        print("\n=== input fields on home ===")
        inputs = await page.locator("input, select, button").all()
        print(f"  total: {len(inputs)}")
        for i, el in enumerate(inputs[:30]):
            try:
                tag = await el.evaluate("el => el.tagName")
                name = await el.get_attribute("name")
                ph = await el.get_attribute("placeholder")
                typ = await el.get_attribute("type")
                vis = await el.is_visible()
                txt = (await el.inner_text())[:30] if tag.lower() == "button" else ""
                print(f"  [{i}] {tag} name={name!r} type={typ!r} placeholder={ph!r} text={txt!r} visible={vis}")
            except Exception:
                pass

        # Find a link to the buscador or click "buscar" / "consultar".
        print("\n=== look for links to buscador ===")
        anchors = await page.locator("a").all()
        candidate_routes = set()
        for a in anchors[:60]:
            try:
                href = await a.get_attribute("href")
                txt = (await a.inner_text())[:60]
                if href and ("busca" in href.lower() or "tratado" in href.lower() or "consulta" in href.lower()):
                    candidate_routes.add(href)
                    print(f"  href={href}  text={txt!r}")
            except Exception:
                pass

        # Try the most likely buscador routes
        for path in ["buscar", "consulta", "tratados", "lista", "buscador"]:
            url = f"https://cja.sre.gob.mx/tratadosmexico/{path}"
            try:
                r_old = len(captured)
                await page.goto(url, wait_until="domcontentloaded", timeout=15_000)
                await page.wait_for_timeout(2000)
                # Find form inputs
                cur = await page.url
                if "/tratadosmexico/" in cur and cur != "https://cja.sre.gob.mx/tratadosmexico/":
                    print(f"\n=== {url} → {cur} ===")
                    # quick scan for token urls in page
                    html = await page.content()
                    tokens = re.findall(r'/tratadosmexico/tratados/(ey[\w\-_=]+)', html)
                    if tokens:
                        print(f"  found {len(tokens)} token URLs on this page. Sample: {tokens[:3]}")
                    # show input fields
                    inps = await page.locator("input[type='text'], input[type='search'], select").all()
                    for inp in inps[:10]:
                        try:
                            n = await inp.get_attribute("name")
                            ph = await inp.get_attribute("placeholder")
                            print(f"    input name={n!r} placeholder={ph!r}")
                        except Exception:
                            pass
            except Exception as e:
                print(f"  goto {url} failed: {e}")

        # Final: dump captured + final URL info.
        OUT.write_text(json.dumps({
            "captured": captured,
            "final_url": page.url,
        }, indent=2, ensure_ascii=False))

        # Summary
        unique = {}
        for c in captured:
            k = f"{c['method']} {c['url'].split('?')[0]}"
            unique[k] = unique.get(k, 0) + 1
        print(f"\n=== {len(captured)} requests, {len(unique)} unique endpoints ===")
        for k, n in sorted(unique.items(), key=lambda x: -x[1])[:30]:
            print(f"  {n:>3}  {k}")
        print(f"\nDumped to: {OUT}")
        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
