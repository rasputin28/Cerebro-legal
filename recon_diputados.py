"""
Diputados (Cámara) recon — diputados.gob.mx/LeyesBiblio/

LeyesBiblio is the canonical federal legislation repository. Vamos a:
1. Identificar listado de leyes vigentes
2. Patrón de URL para PDF/DOC/HTML por ley
3. Versionado por fecha de última reforma
"""
import asyncio, json, re
from pathlib import Path
from playwright.async_api import async_playwright

OUT = Path(__file__).parent / "api-map-dip.json"


ROUTES = [
    "https://www.diputados.gob.mx/LeyesBiblio/index.php",
    "https://www.diputados.gob.mx/LeyesBiblio/ref/cpeum.htm",
    "https://www.diputados.gob.mx/LeyesBiblio/index_codigos.php",
    "https://www.diputados.gob.mx/LeyesBiblio/index_reglamentos.php",
    "https://www.diputados.gob.mx/LeyesBiblio/index_estatuto.php",
]


async def main():
    captured = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        ctx = await browser.new_context(user_agent="Mozilla/5.0 Chrome/131.0", locale="es-MX")
        page = await ctx.new_page()

        async def on_response(resp):
            if "diputados.gob.mx" not in resp.url: return
            ct = resp.headers.get("content-type", "")
            if any(x in ct for x in ("image","font","css")): return
            try:
                body = (await resp.text())[:1200]
            except Exception:
                body = "<err>"
            captured.append({"method": resp.request.method, "status": resp.status, "url": resp.url, "ctype": ct, "resp_preview": body})

        page.on("response", on_response)

        all_pdf_links = set()
        all_doc_links = set()
        all_htm_links = set()
        for r in ROUTES:
            print(f"\n--- {r} ---")
            try:
                await page.goto(r, wait_until="networkidle", timeout=30_000)
            except Exception as e:
                print(f"  goto: {e}")
                continue
            await page.wait_for_timeout(2000)
            html = await page.content()
            for m in re.finditer(r'href="([^"]+\.pdf)"', html, re.I):
                all_pdf_links.add(m.group(1))
            for m in re.finditer(r'href="([^"]+\.docx?)"', html, re.I):
                all_doc_links.add(m.group(1))
            for m in re.finditer(r'href="([^"]+\.htm)"', html, re.I):
                all_htm_links.add(m.group(1))
            # find law names
            for m in re.finditer(r'>([^<]{20,150}(?:Ley|Código|Reglamento|Estatuto)[^<]{0,80})<', html, re.I):
                pass  # too noisy to print all

        print(f"\nunique .pdf links: {len(all_pdf_links)}")
        for l in sorted(all_pdf_links)[:10]:
            print(f"  {l}")
        print(f"\nunique .doc/.docx links: {len(all_doc_links)}")
        for l in sorted(all_doc_links)[:10]:
            print(f"  {l}")
        print(f"\nunique .htm links: {len(all_htm_links)}")
        for l in sorted(all_htm_links)[:10]:
            print(f"  {l}")

        OUT.write_text(json.dumps({
            "captured": captured,
            "pdf_links": sorted(all_pdf_links),
            "doc_links": sorted(all_doc_links),
            "htm_links": sorted(all_htm_links),
        }, indent=2, ensure_ascii=False))
        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
