"""Recon QRoo + Yucatán state legislation portals."""
import httpx, re
H = {"User-Agent": "Mozilla/5.0 Chrome/131.0"}
SITES = [
    ("QRoo Congreso", "https://www.congresoqroo.gob.mx/"),
    ("QRoo Congreso leyes", "https://documentos.congresoqroo.gob.mx/leyes/"),
    ("QRoo gobierno",  "https://qroo.gob.mx/"),
    ("Yuc gobierno",   "https://www.yucatan.gob.mx/transparencia/leyesyreglamentos.php"),
    ("Yuc Congreso",   "http://www.congresoyucatan.gob.mx/"),
]
for name, url in SITES:
    print(f"\n=== {name}: {url} ===")
    try:
        r = httpx.get(url, headers=H, follow_redirects=True, timeout=20, verify=False)
        print(f"  status: {r.status_code}, len: {len(r.text)}, url: {r.url}")
        if r.status_code == 200:
            # Extract PDF/DOC/HTM links
            pdfs = re.findall(r'href=[\'"]([^\'"]+\.pdf)[\'"]', r.text, re.I)
            docs = re.findall(r'href=[\'"]([^\'"]+\.docx?)[\'"]', r.text, re.I)
            htms = re.findall(r'href=[\'"]([^\'"]+(?:ley|reglamento|codigo)[\'"][^\'"]*\.htm?)', r.text, re.I)
            print(f"  PDFs: {len(set(pdfs))} unique")
            for p in sorted(set(pdfs))[:5]:
                print(f"    {p[:120]}")
            print(f"  DOCs: {len(set(docs))} unique")
            for d in sorted(set(docs))[:3]:
                print(f"    {d[:120]}")
            # Look for state law mentions
            ley_text = re.findall(r"(?:Ley|Código|Reglamento)\s+(?:Estatal\s+|de\s+|del\s+|para\s+|sobre\s+)?[A-ZÁÉÍÓÚÑ][^<,\n]{5,100}", r.text)
            print(f"  Ley/Código mentions: {len(ley_text)}")
            for l in ley_text[:3]:
                print(f"    {l[:100].strip()}")
    except Exception as e:
        print(f"  err: {e}")
