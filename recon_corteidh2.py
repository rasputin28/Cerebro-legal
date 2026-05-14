"""
Corte IDH deeper recon — drive search forms + click results to map endpoints.
"""
import asyncio, json, re
from pathlib import Path
from playwright.async_api import async_playwright

OUT = Path(__file__).parent / "api-map-idh2.json"


async def main():
    captured = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        ctx = await browser.new_context(user_agent="Mozilla/5.0 Chrome/131.0", locale="es-MX")
        page = await ctx.new_page()

        async def on_response(resp):
            if "corteidh" not in resp.url: return
            ct = resp.headers.get("content-type", "")
            if any(x in ct for x in ("image","font","css")): return
            try:
                body = (await resp.text())[:2000]
            except Exception:
                body = "<err>"
            captured.append({"method": resp.request.method, "status": resp.status, "url": resp.url, "ctype": ct, "req_body": resp.request.post_data, "resp_preview": body})

        page.on("response", on_response)

        # The casos-sentencias.cfm page builds a table client-side. We need to wait.
        await page.goto("https://www.corteidh.or.cr/casos-sentencias.cfm", wait_until="networkidle", timeout=45_000)
        await page.wait_for_timeout(8_000)
        html = await page.content()
        Path("idh_casos.html").write_text(html[:200000])
        # Look for ficha_caso links (case detail)
        ficha = re.findall(r'href="([^"]*ficha_caso[^"]*)"', html, re.I)
        print(f"ficha_caso links: {len(set(ficha))}")
        for f in sorted(set(ficha))[:5]:
            print(f"  {f}")
        # try opinion consultivas
        await page.goto("https://www.corteidh.or.cr/opiniones_consultivas.cfm", wait_until="networkidle", timeout=45_000)
        await page.wait_for_timeout(6_000)
        html2 = await page.content()
        Path("idh_oc.html").write_text(html2[:200000])
        oc = re.findall(r'href="([^"]*opinion[^"]*|[^"]*OC[^"]*)"', html2, re.I)
        print(f"OC links: {len(set(oc))}")
        for o in sorted(set(oc))[:5]:
            print(f"  {o}")

        # Try jurisprudencia search
        await page.goto("https://www.corteidh.or.cr/cf/Jurisprudencia2/busqueda_casos_contenciosos.cfm",
                        wait_until="networkidle", timeout=45_000)
        await page.wait_for_timeout(5_000)
        # Submit empty
        try:
            btns = await page.get_by_role("button").all()
            for b in btns[:5]:
                try:
                    if await b.is_visible():
                        await b.click(timeout=2000)
                        await page.wait_for_timeout(4000)
                        break
                except Exception:
                    pass
        except Exception:
            pass

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
