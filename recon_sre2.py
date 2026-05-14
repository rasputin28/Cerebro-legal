"""SRE recon v2 — focus on /tratadosmexico/buscador, the actual search page."""
import asyncio, json, re
from pathlib import Path
from playwright.async_api import async_playwright

OUT = Path(__file__).parent / "api-map-sre2.json"


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
            if "sre.gob.mx" not in resp.url:
                return
            ct = resp.headers.get("content-type", "")
            if "image" in ct or "font" in ct or "css" in ct:
                return
            try:
                body = (await resp.text())[:2000] if ("json" in ct or "html" in ct or "text" in ct) else f"<{ct}>"
            except Exception:
                body = "<err>"
            captured.append({
                "method": resp.request.method,
                "status": resp.status,
                "url": resp.url,
                "ctype": ct,
                "req_body": resp.request.post_data,
                "resp_preview": body,
            })

        page.on("response", on_response)

        await page.goto("https://cja.sre.gob.mx/tratadosmexico/buscador", wait_until="networkidle", timeout=60_000)
        await page.wait_for_timeout(5_000)

        html = await page.content()
        Path("sre_buscador.html").write_text(html[:80000])
        print(f"buscador html size: {len(html)}")

        # All inputs / selects
        print("\n=== form fields ===")
        for el in await page.locator("input, select, textarea").all():
            try:
                tag = await el.evaluate("e => e.tagName")
                name = await el.get_attribute("name")
                typ = await el.get_attribute("type")
                ph = await el.get_attribute("placeholder")
                ngm = await el.get_attribute("ng-model")
                vis = await el.is_visible()
                if vis:
                    print(f"  {tag} name={name!r} type={typ!r} placeholder={ph!r} ng-model={ngm!r}")
            except Exception:
                pass

        # Submit empty search to get all results
        print("\n=== submit empty search ===")
        for btn_text in ["Buscar", "Consultar", "Mostrar todos", "Ver todos", "Aceptar"]:
            btn = page.get_by_role("button", name=re.compile(btn_text, re.I)).first
            try:
                if await btn.count() and await btn.is_visible():
                    print(f"  clicking '{btn_text}'")
                    await btn.click(timeout=3000)
                    await page.wait_for_timeout(6000)
                    break
            except Exception as e:
                print(f"  '{btn_text}' failed: {e}")

        # Grab tokens from results
        html_after = await page.content()
        tokens = re.findall(r'/tratadosmexico/tratados/([\w\-=]+)', html_after)
        print(f"\nFound {len(tokens)} token URLs after search. Unique: {len(set(tokens))}")
        if tokens:
            print(f"Sample tokens: {list(set(tokens))[:3]}")

        # Look for paginator
        print("\n=== paginator hint ===")
        pag = await page.locator("[class*='pag'], [ng-class*='pag']").all()
        for el in pag[:10]:
            try:
                t = (await el.inner_text())[:80]
                cls = await el.get_attribute("class")
                if t.strip():
                    print(f"  class={cls!r}: {t!r}")
            except Exception:
                pass

        # Visit first detail to map its DOM
        if tokens:
            tok = list(set(tokens))[0]
            detail_url = f"https://cja.sre.gob.mx/tratadosmexico/tratados/{tok}"
            print(f"\n=== visit detail: {detail_url} ===")
            await page.goto(detail_url, wait_until="networkidle", timeout=30_000)
            await page.wait_for_timeout(3000)
            detail_html = await page.content()
            Path("sre_detail.html").write_text(detail_html[:80000])
            # Extract field-looking labels
            print("\nLabels visible (h*, dt, label, strong):")
            for sel in ["h1", "h2", "h3", "dt", "label", "strong", "b"]:
                els = await page.locator(sel).all()
                for el in els[:25]:
                    try:
                        txt = (await el.inner_text()).strip()
                        if 2 < len(txt) < 100:
                            print(f"  {sel}: {txt}")
                    except Exception:
                        pass

        # Final dump
        OUT.write_text(json.dumps({"captured": captured}, indent=2, ensure_ascii=False))

        # Summary of interesting endpoints
        unique = {}
        for c in captured:
            k = f"{c['method']} {c['url'].split('?')[0]}"
            unique[k] = unique.get(k, 0) + 1
        print(f"\n=== {len(captured)} total / {len(unique)} unique endpoints ===")
        for k, n in sorted(unique.items(), key=lambda x: -x[1])[:25]:
            print(f"  {n:>3}  {k}")

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
