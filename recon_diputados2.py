"""
Diputados deeper recon — pull index pages of all kinds (leyes, codigos,
reglamentos, estatutos), enumerate every law-detail link, sample one to
confirm we can extract the actual law text.
"""
import asyncio, httpx, re, json
from pathlib import Path

OUT = Path(__file__).parent / "dip_index.json"

INDEX_PAGES = [
    ("leyes",       "https://www.diputados.gob.mx/LeyesBiblio/index.php"),
    ("codigos",     "https://www.diputados.gob.mx/LeyesBiblio/index_codigos.php"),
    ("reglamentos", "https://www.diputados.gob.mx/LeyesBiblio/index_reglamentos.php"),
    ("estatutos",   "https://www.diputados.gob.mx/LeyesBiblio/index_estatuto.php"),
]

LEY_LINK_RE = re.compile(r'href="(?P<href>[\w\-\.\/]*?(?:ref|reg)?/?[\w\-]*?\.(?:pdf|htm|docx?))"\s*(?:target="[^"]+"\s*)?(?:title="[^"]+"\s*)?>(?P<text>[^<]{0,200})<', re.I)
LAW_NAME_RE = re.compile(r"<a[^>]+href=\"(?P<href>[^\"]+\.(?:pdf|htm|doc|docx))\"[^>]*>(?P<name>[^<]{5,200})</a>", re.I)


def absolutize(href: str, base: str) -> str:
    if href.startswith("http"):
        return href
    if href.startswith("/"):
        return f"https://www.diputados.gob.mx{href}"
    # relative -> resolve against base directory
    base_dir = base.rsplit("/", 1)[0]
    while href.startswith("../"):
        href = href[3:]
        base_dir = base_dir.rsplit("/", 1)[0]
    return f"{base_dir}/{href}"


def main():
    h = {"User-Agent": "Mozilla/5.0 Chrome/131.0"}
    all_laws = []
    with httpx.Client(http2=True, headers=h, timeout=30, follow_redirects=True) as c:
        for label, url in INDEX_PAGES:
            print(f"\n--- {label}: {url} ---")
            try:
                r = c.get(url)
                if r.status_code != 200:
                    print(f"  status={r.status_code}")
                    continue
                html = r.text
                # Look for table rows with law name + multiple file format links (PDF/Word/HTML)
                # Pattern: <td>Ley X</td>...<a href=".../pdf/LX.pdf">...
                for m in LAW_NAME_RE.finditer(html):
                    href = m.group("href").strip()
                    name = re.sub(r"\s+", " ", m.group("name").strip())
                    if href.startswith("#") or href.endswith(".php"):
                        continue
                    abs_url = absolutize(href, url)
                    all_laws.append({
                        "category": label,
                        "name": name,
                        "url": abs_url,
                        "ext": href.rsplit(".", 1)[-1].lower(),
                    })
                print(f"  links collected so far: {len(all_laws)}")
            except Exception as e:
                print(f"  err: {e}")

    # Dedup by URL
    seen = set()
    uniq = []
    for l in all_laws:
        if l["url"] not in seen:
            seen.add(l["url"])
            uniq.append(l)
    by_ext = {}
    for l in uniq:
        by_ext.setdefault(l["ext"], 0)
        by_ext[l["ext"]] += 1
    print(f"\nUnique law-file links: {len(uniq)}")
    print(f"By ext: {by_ext}")
    print("\nFirst 8 PDFs:")
    for l in [x for x in uniq if x["ext"] == "pdf"][:8]:
        print(f"  {l['name'][:60]!r} → {l['url']}")
    print("\nFirst 8 HTMs:")
    for l in [x for x in uniq if x["ext"] == "htm"][:8]:
        print(f"  {l['name'][:60]!r} → {l['url']}")

    OUT.write_text(json.dumps({"laws": uniq, "by_ext": by_ext}, indent=2, ensure_ascii=False))
    print(f"\nDumped to {OUT}")


if __name__ == "__main__":
    main()
