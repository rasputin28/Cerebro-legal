"""
Corte IDH recon (corteidh.or.cr).

Goal: map endpoints for the case database + opinions consultivas.
The official sites are:
  - https://www.corteidh.or.cr/casos-sentencias.cfm  (cases)
  - https://www.corteidh.or.cr/opiniones_consultivas.cfm  (advisory opinions)
  - https://www.corteidh.or.cr/cf/Jurisprudencia2/  (CIJUR newer search)

Inspect: structure of case listings, detail link patterns, downloadable PDFs.
Outputs api-map-idh.json with seen requests + observations.
"""
import asyncio, json, re
from pathlib import Path
from playwright.async_api import async_playwright

OUT = Path(__file__).parent / "api-map-idh.json"

ROUTES = [
    "https://www.corteidh.or.cr/casos-sentencias.cfm",
    "https://www.corteidh.or.cr/opiniones_consultivas.cfm",
    "https://www.corteidh.or.cr/cf/Jurisprudencia2/",
    "https://www.corteidh.or.cr/cf/Jurisprudencia2/busqueda_casos_contenciosos.cfm",
]


async def main():
    captured = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        ctx = await browser.new_context(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) Chrome/131.0 Safari/537.36",
            locale="es-MX",
        )
        page = await ctx.new_page()

        async def on_response(resp):
            url = resp.url
            if "corteidh.or.cr" not in url:
                return
            ct = resp.headers.get("content-type", "")
            if any(x in ct for x in ("image", "font", "css")):
                return
            try:
                body = (await resp.text())[:1200] if ("json" in ct or "html" in ct or "text" in ct) else f"<{ct}>"
            except Exception:
                body = "<err>"
            captured.append({
                "method": resp.request.method,
                "status": resp.status,
                "url": url,
                "ctype": ct,
                "req_body": resp.request.post_data,
                "resp_preview": body,
            })

        page.on("response", on_response)

        for route in ROUTES:
            print(f"\n--- {route} ---")
            try:
                await page.goto(route, wait_until="networkidle", timeout=45_000)
            except Exception as e:
                print(f"  goto err: {e}")
                continue
            await page.wait_for_timeout(3000)
            html = await page.content()
            # find case detail links
            links = re.findall(r'href="([^"]*ficha_caso[^"]*|[^"]*caso[^"]*|[^"]*opinion[^"]*)"', html, re.I)
            uniq = sorted(set(links))[:15]
            print(f"  links sample ({len(set(links))} unique):")
            for l in uniq:
                print(f"    {l}")
            # Look for tables
            tables = re.findall(r'<table[^>]*>', html, re.I)
            print(f"  tables: {len(tables)}")
            # search for case-card patterns
            cases = re.findall(r'(?:Caso|Sentencia)\s+(?:de\s+)?([A-Z][\w\s\.\-,]{4,80}?)\s*Vs\.?\s+([A-Z][\w\s\.]{3,80})', html)
            print(f"  case-style matches: {len(cases)}")
            for c in cases[:3]:
                print(f"    {c}")

        OUT.write_text(json.dumps({"captured": captured}, indent=2, ensure_ascii=False))
        print(f"\nDumped to {OUT}")
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
